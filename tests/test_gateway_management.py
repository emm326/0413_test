"""Transactional device tests with a virtual filesystem; never access a gateway."""
import hashlib
import json
import shlex
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from test_desktop_control import factory
from host.management import GatewayManager, NETWORK_FILE, CONFIG_LINK, PROJECT_DIR, static_network, local_preflight
from host.transports import TransportError, SerialTransport, SSHTransport


class VirtualDevice:
    kind='serial'
    def __init__(self):
        self.target=PROJECT_DIR+'/470_CN470.json'
        self.identity='a'*32;self.active=True;self.fail_next_start=False
        self.files={self.target:(json.dumps(factory())+'\n').encode(),NETWORK_FILE:b'[Match]\nName=eth0\n[Network]\nDHCP=ipv4\n',
                    '/root/lorawanopen/gateway/gw.sh':b'original script', '/etc/systemd/system/gw.service':b'original unit'}
        self.original=dict(self.files);self.operations=[];self.corrupt_once=False;self.link_stages={}

    def read_file(self,path):return self.files[path]
    def write_file(self,path,data):
        self.operations.append(('write',path));self.files[path]=data
        if self.corrupt_once:self.corrupt_once=False;self.files[path]+=b'broken'
    def close(self):pass
    def run(self,command,timeout=None,check=True):
        self.operations.append(('run',command))
        if command=='cat /etc/machine-id':return self.identity
        if command.startswith('readlink -f '):return self.target
        if command=='hostname':return 'SIMULATED-E870'
        if command=='date -R':return 'SIMULATED DATE'
        if command.startswith('ip -4 addr show'):
            network=self.files[NETWORK_FILE].decode()
            return 'inet '+network.split('Address=')[1].splitlines()[0] if 'Address=' in network else ''
        if command.startswith('systemctl show'):return f"ActiveState={'active' if self.active else 'failed'}\nSubState={'running' if self.active else 'failed'}\nMainPID=1\nNRestarts=0"
        if command=='systemctl is-active gw':return 'active' if self.active else 'failed'
        if command=='systemctl stop gw':self.active=False;return ''
        if command=='systemctl start gw':
            self.active=not self.fail_next_start;self.fail_next_start=False;return ''
        if command=='systemctl restart systemd-networkd':return ''
        if command.startswith('mkdir -p '):return ''
        if command.startswith('ln -s '):
            parts=shlex.split(command);self.target=parts[2];return ''
        if command.startswith('journalctl'):
            return '# RF packets sent to concentrator: 3 (66 bytes)\n# TX errors: 0'
        raise AssertionError('Unhandled simulated command '+command)


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.device=VirtualDevice();self.manager=GatewayManager(self.device,self.tmp.name)
        self.sleep=mock.patch('host.management.time.sleep');self.sleep.start()
        self.preflight=mock.patch('host.management.local_preflight',return_value={});self.preflight.start()
    def tearDown(self):self.sleep.stop();self.preflight.stop();self.tmp.cleanup()

    def test_preview_never_mutates_device(self):
        preview=self.manager.preview('192.168.1.22')
        self.assertTrue(preview['tx_allowed']);self.assertEqual(self.device.files,self.device.original)
        self.assertFalse(any(x[0]=='write' for x in self.device.operations))

    def test_network_apply_readback_and_restore_exact(self):
        preview=self.manager.preview('192.168.1.22')
        result=self.manager.apply_network(preview);backup=result['backup']
        self.assertIn(b'Address=192.168.1.10/24',self.device.files[NETWORK_FILE])
        self.manager.restore(backup)
        for p,data in self.device.original.items():self.assertEqual(self.device.files[p],data)
        self.assertEqual(self.device.target,PROJECT_DIR+'/470_CN470.json')

    def test_project_apply_does_not_overwrite_factory(self):
        preview=self.manager.preview('192.168.1.22');result=self.manager.apply_project(preview)
        self.assertIn('/desktop_',self.device.target)
        self.assertEqual(self.device.files[PROJECT_DIR+'/470_CN470.json'],self.device.original[PROJECT_DIR+'/470_CN470.json'])
        self.assertTrue(Path(result['config_path']).exists())
        self.manager.restore(result['backup'])
        self.assertEqual(self.device.target,PROJECT_DIR+'/470_CN470.json')

    def test_failed_project_start_rolls_back_original_pointer(self):
        preview=self.manager.preview('192.168.1.22');self.device.fail_next_start=True
        with self.assertRaises(TransportError):self.manager.apply_project(preview)
        self.assertEqual(self.device.target,PROJECT_DIR+'/470_CN470.json');self.assertTrue(self.device.active)

    def test_corrupt_project_upload_rolls_back(self):
        preview=self.manager.preview('192.168.1.22');self.device.corrupt_once=True
        with self.assertRaises((TransportError,ValueError)):self.manager.apply_project(preview)
        self.assertEqual(self.device.target,PROJECT_DIR+'/470_CN470.json')
        self.assertEqual(self.device.files[self.device.target],self.device.original[self.device.target])

    def test_corrupt_network_upload_rolls_back(self):
        preview=self.manager.preview('192.168.1.22');self.device.corrupt_once=True
        with self.assertRaises(TransportError):self.manager.apply_network(preview)
        self.assertEqual(self.device.files[NETWORK_FILE],self.device.original[NETWORK_FILE])

    def test_preview_rejected_after_device_change(self):
        preview=self.manager.preview('192.168.1.22');self.device.identity='b'*32
        with self.assertRaises(TransportError):self.manager.apply_project(preview)
        self.assertFalse(any(x[0]=='write' for x in self.device.operations))

    def test_preview_rejected_after_external_config_edit(self):
        preview=self.manager.preview('192.168.1.22');self.device.files[self.device.target]+=b' '
        with self.assertRaises(TransportError):self.manager.apply_project(preview)

    def test_network_not_allowed_over_ssh(self):
        preview=self.manager.preview('192.168.1.22');self.device.kind='ssh'
        with self.assertRaises(TransportError):self.manager.apply_network(preview)
        folder=self.manager.backup()
        with self.assertRaises(TransportError):self.manager.restore(folder)

    def test_modified_or_other_device_backup_rejected(self):
        folder=self.manager.backup();self.device.identity='b'*32
        with self.assertRaises(TransportError):self.manager.restore(folder)
        self.device.identity='a'*32;(folder/'config.json').write_bytes(b'corrupt')
        with self.assertRaises(TransportError):self.manager.restore(folder)

    def test_tx_counter_requires_three_sends_and_no_error(self):
        self.assertTrue(self.manager.verify_tx('cursor')['passed'])
        self.device.run=lambda *a,**k:'# RF packets sent to concentrator: 3 (66 bytes)\n# TX errors: 1'
        self.assertFalse(self.manager.verify_tx('cursor')['passed'])
        self.device.run=lambda *a,**k:'# RF packets sent to concentrator: 0 (0 bytes)\n# TX errors: 0'
        self.assertFalse(self.manager.verify_tx('cursor')['passed'])

    def test_disconnected_transport_aborts_before_mutation(self):
        preview=self.manager.preview('192.168.1.22')
        self.device.run=mock.Mock(side_effect=TransportError('connection lost'))
        with self.assertRaises(TransportError):self.manager.apply_project(preview)
        self.assertFalse(any(x[0]=='write' for x in self.device.operations))


class NetworkValidationTests(unittest.TestCase):
    def test_address_validation(self):
        for address,pc in [('192.168.1.22/24','192.168.1.22'),('192.168.2.10/24','192.168.1.22'),('192.168.1.0/24','192.168.1.22'),('192.168.1.255/24','192.168.1.22'),('192.168.1.10/16','192.168.1.22')]:
            with self.assertRaises(ValueError):static_network(address,pc)
        self.assertIn(b'DHCP=no',static_network('192.168.1.10/24','192.168.1.22'))

    def test_conflicting_ip_and_missing_local_address(self):
        with mock.patch('host.management.inventory',return_value={'adapters':[]}):
            with self.assertRaises(ValueError):local_preflight('192.168.1.10/24','192.168.1.22')
        with mock.patch('host.management.inventory',return_value={'adapters':[{'address':'127.0.0.1'}]}),mock.patch('host.management.subprocess.run',return_value=mock.Mock(returncode=0,stdout=b"Reply from target TTL=64")):
            with self.assertRaisesRegex(ValueError,'已有设备响应'):local_preflight('127.0.0.2/24','127.0.0.1',port=0)

    def test_serial_busy_closes_port(self):
        with mock.patch('serial.Serial') as serial:
            serial.return_value.open.side_effect=OSError('port busy')
            with self.assertRaises(OSError):SerialTransport('COM99','root','secret')
            serial.return_value.close.assert_called_once()

    def test_bad_ssh_password_closes_connection(self):
        with tempfile.TemporaryDirectory() as folder,mock.patch('paramiko.SSHClient') as client:
            client.return_value.connect.side_effect=OSError('authentication failed')
            with self.assertRaises(OSError):SSHTransport('127.0.0.1','root','secret',Path(folder)/'known_hosts')
            client.return_value.close.assert_called_once()


class SerialPromptTests(unittest.TestCase):
    def test_last_login_banner_is_not_a_login_prompt(self):
        class Port:
            def __init__(self):self.chunks=iter([b"Last login:",b" Sat 1 Aug\r\n",b"root@device:~# "])
            @property
            def in_waiting(self):return 1
            def read(self,n):return next(self.chunks)
        transport=object.__new__(SerialTransport);transport.port=Port()
        text=transport._read_login(1)
        self.assertTrue(text.endswith("# "))


    def test_network_upload_keeps_networkd_read_permission(self):
        import threading
        data=b"[Network]\nDHCP=no\nAddress=192.168.1.10/24\n"
        transport=object.__new__(SerialTransport);transport.lock=threading.RLock()
        calls=[]
        def command(text,**kwargs):
            calls.append(text)
            if text.startswith("sha256sum "):return hashlib.sha256(data).hexdigest()+"  file"
            return ""
        transport.run=command
        transport.write_file(NETWORK_FILE,data)
        self.assertTrue(any(text.startswith("chmod 644 ") for text in calls))


class JournalBootTests(unittest.TestCase):
    def test_future_dated_old_boot_cannot_hide_current_tx_counts(self):
        class Device:
            boot="11111111-2222-3333-4444-555555555555"
            def __init__(self):self.calls=[]
            def run(self,cmd,**kwargs):
                self.calls.append(cmd)
                if cmd=="cat /proc/sys/kernel/random/boot_id":return self.boot
                if "--show-cursor" in cmd:return "-- cursor: current-boot-cursor"
                if "--boot="+self.boot.replace("-", "") in cmd:return "# RF packets sent to concentrator: 3 (57 bytes)\n# TX errors: 0"
                return "# RF packets sent to concentrator: 0 (0 bytes)\n# TX errors: 0"
        device=Device();manager=GatewayManager(device)
        cursor=manager.log_cursor()
        self.assertEqual(cursor,"current-boot-cursor")
        self.assertTrue(manager.verify_tx(cursor)["passed"])
        self.assertTrue(all("--boot="+device.boot.replace("-", "") in c for c in device.calls if c.startswith("journalctl")))

    def test_reboot_invalidates_log_evidence(self):
        device=mock.Mock()
        boot="11111111-2222-3333-4444-555555555555"
        device.run.side_effect=[boot,"-- cursor: test","aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"]
        manager=GatewayManager(device);cursor=manager.log_cursor()
        with self.assertRaisesRegex(TransportError,"重启"):manager.verify_tx(cursor)

    def test_log_view_is_current_boot_even_without_cursor(self):
        device=mock.Mock();device.run.return_value="logs"
        GatewayManager(device).logs()
        self.assertIn("journalctl -b -u gw",device.run.call_args.args[0])
