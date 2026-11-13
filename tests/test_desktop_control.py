"""Control-plane regressions, using UDP loopback and simulated nodes only."""
import base64
import json
import socket
import tempfile
import time
import unittest
from unittest import mock
from host.gateway import GatewayServer
from host.wire import Frame, STATUS, CONFIRM, STATUS_PAYLOAD, STOP, START, DATA
from host.configure_gateway import configure, DEVICE_PROFILE


def factory():
    return {"SX130x_conf":{"radio_0":{"type":"SX1250","enable":True,"tx_enable":True,
            "freq":470600000,"tx_freq_min":500000000,"tx_freq_max":510000000,
            "rssi_offset":-207,"rssi_tcomp":{"coeff_a":0},
            "tx_gain_lut":[{"rf_power":14,"pa_gain":1,"pwr_idx":9}]},
            "radio_1":{"enable":True,"type":"SX1250","freq":471400000,"tx_enable":False},
            "lorawan_public":True},"gateway_conf":{"gateway_ID":"0011223344556677","server_address":"127.0.0.1","serv_port_up":1700,"serv_port_down":1700}}


class ProfileTests(unittest.TestCase):
    def test_explicit_profile_preserves_factory_and_calibration(self):
        original=factory();before=json.dumps(original,sort_keys=True)
        result,supported=configure(original,"192.168.1.22",DEVICE_PROFILE)
        self.assertTrue(supported)
        self.assertEqual(result['SX130x_conf']['radio_0']['tx_freq_min'],470000000)
        for key in ['rssi_offset','rssi_tcomp','tx_gain_lut','type']:
            self.assertEqual(original['SX130x_conf']['radio_0'][key],result['SX130x_conf']['radio_0'][key])
        self.assertEqual(json.dumps(original,sort_keys=True),before)
        self.assertFalse(result['project_profile']['rf_test_passed'])
        self.assertFalse(configure(original,'192.168.1.22')[1])

    def test_unknown_profile_and_incomplete_hardware_rejected(self):
        with self.assertRaises(ValueError):configure(factory(),'192.168.1.22','unknown')
        original=factory();original['SX130x_conf']['radio_0'].pop('tx_gain_lut')
        with self.assertRaises(ValueError):configure(original,'192.168.1.22',DEVICE_PROFILE)
        with self.assertRaises(ValueError):configure(factory(),'bad-ip',DEVICE_PROFILE)


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.events=[]
        self.server=GatewayServer('127.0.0.1',0,self.temp.name,nodes=(1001,1002),emit=lambda k,v:self.events.append((k,v)),tx_range_verified=True)
        self.server.socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);self.server.socket.bind(('127.0.0.1',0))
        self.client=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);self.client.bind(('127.0.0.1',0));self.client.settimeout(1)
        self.server.downlink_peer=self.client.getsockname();self.server.peer_seen=time.monotonic()
        self.eui=bytes.fromhex('0011223344556677')

    def tearDown(self):
        self.server.close();self.client.close();self.temp.cleanup()

    def ready(self):
        for n in self.server.targets:
            self.server._frame(Frame(STATUS,n,flags=3,epoch_us=int(time.time()*1e6),payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,1,0)),{'datr':'SF6BW125'})

    def ack(self,token,error='NONE',peer=None):
        raw=b'\x02'+token+b'\x05'+self.eui+json.dumps({'txpk_ack':{'error':error}}).encode()
        self.server.handle_datagram(raw,peer or self.client.getsockname())

    def test_wrong_state_and_wrong_run_cannot_confirm_start(self):
        self.ready();identifier=self.server._request('start',{})
        run=self.server.run_id
        for frame_run,state in [(run,1),(run+1,2)]:
            self.server._frame(Frame(CONFIRM,1001,run=frame_run,payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,state,0)),{})
        self.assertEqual(self.server.awaiting_start,{1001,1002})
        for n in self.server.targets:
            self.server._frame(Frame(CONFIRM,n,run=run,payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,2,0)),{})
        self.assertFalse(self.server.awaiting_start)
        self.assertEqual(self.server.commands[identifier]['state'],'nodes_confirmed')

    def test_zero_run_confirmation_does_not_mark_node_confirmed(self):
        self.server._frame(Frame(CONFIRM,1001,run=0,payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,1,0)),{})
        self.assertFalse(self.server.nodes[1001].confirmed)

    def test_idle_stop_never_broadcasts_zero_run(self):
        with self.assertRaises(ValueError):self.server._request('stop',{})
        self.assertFalse(self.server.downlinks)

    def test_stop_retries_and_preserves_run_on_timeout(self):
        self.ready();self.server._request('start',{});run=self.server.run_id
        identifier=self.server._request('stop',{})
        self.assertEqual([x[1].kind for x in self.server.downlinks],[STOP]*3)
        self.assertTrue(all(x[1].run==run for x in self.server.downlinks))
        self.server.commands[identifier]['created_mono']-=31
        self.server.tx_ready=time.monotonic()+100
        self.server._schedule()
        self.assertEqual(self.server.commands[identifier]['state'],'timeout')
        self.assertEqual(self.server.run_id,run)
        self.assertFalse(any(x[1].kind==START for x in self.server.downlinks))

    def test_finish_requires_empty_queue_and_matching_run(self):
        self.ready();self.server._request('start',{});run=self.server.run_id
        self.server._request('stop',{})
        for n in self.server.targets:
            self.server._frame(Frame(STATUS,n,run=run,payload=STATUS_PAYLOAD.pack(32000,0,0,0,1,1,0)),{})
        self.assertEqual(self.server.run_id,run)
        for n in self.server.targets:
            self.server._frame(Frame(STATUS,n,run=run,payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,1,0)),{})
        self.assertEqual(self.server.run_id,0)

    def test_cancellation_blocks_next_run_until_stop_verified(self):
        self.ready();self.server._request('start',{})
        self.server.start_us=self.server.gnss_now_us+1000000
        self.server.tx_ready=time.monotonic()+100
        self.server._schedule()
        self.assertIsNotNone(self.server.cancel_run)
        with self.assertRaises(ValueError):self.server._request('start',{})
        run=self.server.cancel_run['run']
        for n in self.server.targets:
            self.server._frame(Frame(CONFIRM,n,run=run,payload=STATUS_PAYLOAD.pack(32000,0,0,0,0,1,0)),{})
        self.assertIsNone(self.server.cancel_run)

    def test_real_selftest_payload_cannot_be_control_command(self):
        identifier=self.server._request('self_test',{'antenna_confirmed':True})
        self.assertEqual(len(self.server.downlinks),3)
        times=[x[0] for x in self.server.downlinks]
        self.assertGreaterEqual(times[1]-times[0],1.99)
        self.assertGreaterEqual(times[2]-times[1],1.99)
        for item in self.server.downlinks:
            data=item[1].encode();self.assertTrue(data.startswith(b'E870-DIAG'))
            with self.assertRaises(ValueError):Frame.decode(data)
        item=self.server.downlinks[0];self.server._send(item[1],item[2])
        packet=self.client.recvfrom(2048)[0];token=packet[1:3]
        self.ack(token,'TX_FREQ')
        self.assertEqual(self.server.commands[identifier]['state'],'gateway_rejected')
        self.ack(token)
        self.assertEqual(self.server.commands[identifier]['state'],'gateway_rejected')

    def test_ack_timeout_and_wrong_peer(self):
        identifier=self.server._request('self_test',{'antenna_confirmed':True})
        item=self.server.downlinks[0];self.server._send(item[1],item[2]);packet=self.client.recvfrom(2048)[0];token=packet[1:3]
        with self.assertRaises(ValueError):self.ack(token,peer=('127.0.0.1',2))
        self.server.pending_tokens[token.hex()]['sent_mono']-=6
        self.server.tx_ready=time.monotonic()+100;self.server._schedule()
        self.assertEqual(self.server.commands[identifier]['state'],'timeout')
        self.ack(token)
        self.assertEqual(self.server.commands[identifier]['state'],'timeout')

    def test_gateway_ack_alone_cannot_complete_selftest(self):
        identifier=self.server._request('self_test',{'antenna_confirmed':True})
        for item in self.server.downlinks:
            self.server._send(item[1],item[2]);packet=self.client.recvfrom(2048)[0];self.ack(packet[1:3])
        self.assertNotEqual(self.server.commands[identifier]['state'],'completed')
        self.server._request('self_test_result',{'identifier':identifier,'passed':False,'evidence':{'sent':0}})
        self.assertEqual(self.server.commands[identifier]['state'],'failed')

    def test_no_nodes_readiness_is_explicit(self):
        reasons=self.server.readiness()
        self.assertTrue(any('节点未就绪' in x for x in reasons))
        self.assertTrue(any('GNSS' in x for x in reasons))

    def test_occupied_socket_cleans_failed_server(self):
        other=GatewayServer('127.0.0.1',self.server.socket.getsockname()[1],self.temp.name)
        with self.assertRaises(OSError):other.start()
        self.assertIsNone(other.socket);other.close()

    def test_worker_reconnect_releases_ports(self):
        self.server.close()
        for _ in range(3):
            self.server=GatewayServer('127.0.0.1',0,self.temp.name)
            self.server.start();address=('127.0.0.1',self.server.port)
            self.client.sendto(b'\x02ab\x02'+self.eui,address)
            self.assertEqual(self.client.recvfrom(1024)[0],b'\x02ab\x04')
            self.server.close();self.assertIsNone(self.server.thread)
            probe=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            probe.bind(address);probe.close()

    def test_query_timeout_does_not_claim_node_success(self):
        identifier=self.server._request('discover',{})
        self.server.commands[identifier]['created_mono']-=16
        self.server.tx_ready=time.monotonic()+100;self.server._schedule()
        self.assertEqual(self.server.commands[identifier]['message'],'暂未发现节点')

    def test_expected_gateway_identity_is_enforced(self):
        self.server.expected_eui='ffffffffffffffff'
        with self.assertRaises(ValueError):self.server.handle_datagram(b'\x02ab\x02'+self.eui,self.client.getsockname())


    def test_wrong_run_data_cannot_refresh_node_health(self):
        node=self.server.nodes[1001]
        before=(node.flags,node.last_seen,node.state,node.run)
        import struct
        self.server._frame(Frame(DATA,1001,flags=3,run=999,rate=50,count=1,payload=struct.pack("<hhh",1,2,3)),{})
        self.assertEqual((node.flags,node.last_seen,node.state,node.run),before)

    def test_all_three_acks_and_evidence_complete_selftest(self):
        identifier=self.server._request("self_test",{"antenna_confirmed":True})
        for item in self.server.downlinks:
            self.server._send(item[1],item[2]);packet=self.client.recvfrom(2048)[0];self.ack(packet[1:3])
        self.server._request("self_test_result",{"identifier":identifier,"passed":True,"evidence":{"sent":3,"errors":0}})
        self.assertEqual(self.server.commands[identifier]["state"],"completed")


    def test_retried_status_packet_is_not_counted_twice(self):
        stat={"time":"period-1","txnb":3,"dwnb":3}
        body=json.dumps({"stat":stat}).encode()
        started=time.monotonic()
        for token in (b"aa",b"bb"):
            self.server.handle_datagram(b"\x02"+token+b"\x00"+self.eui+body,self.client.getsockname())
            self.client.recvfrom(1024)
        self.assertEqual(self.server.tx_success_since(started)["sent"],3)
        self.assertEqual(len(self.server.stat_samples),1)
        self.assertEqual(self.server.tx_success_since(time.monotonic()+1)["sent"],0)

    def test_invalid_success_counter_is_not_execution_evidence(self):
        for i,value in enumerate([-1,True,"3"]):
            body=json.dumps({"stat":{"time":str(i),"txnb":value}}).encode()
            self.server.handle_datagram(b"\x02ab\x00"+self.eui+body,self.client.getsockname())
            self.client.recvfrom(1024)
        self.assertEqual(self.server.tx_success_since(0)["sent"],0)

    def test_selftest_reserves_downlink_from_other_commands(self):
        self.server._request("self_test",{"antenna_confirmed":True})
        with self.assertRaisesRegex(ValueError,"自检"):self.server._request("discover",{})
        self.assertEqual(len(self.server.downlinks),3)

    def test_selftest_scheduler_keeps_at_least_two_seconds_between_sends(self):
        with mock.patch("host.gateway.time.monotonic",return_value=100),mock.patch.object(self.server,"_send") as send:
            self.server.peer_seen=100;self.server.last_report=100
            self.server._request("self_test",{"antenna_confirmed":True})
            self.server._schedule()
            self.assertEqual(send.call_count,1)
            self.assertGreaterEqual(self.server.tx_ready,102.05)
        with mock.patch("host.gateway.time.monotonic",return_value=102.01),mock.patch.object(self.server,"_send") as send:
            self.server._schedule();send.assert_not_called()


    def test_old_data_cannot_restore_failed_clock_or_clear_reported_queue(self):
        import struct
        self.server.run_id=77
        status=Frame(STATUS,1001,run=77,flags=1,payload=STATUS_PAYLOAD.pack(63000,900000,12,3,128,4,0))
        self.server._frame(status,{})
        node=self.server.nodes[1001]
        seen=node.last_status_seen
        self.server._frame(Frame(DATA,1001,run=77,flags=3,rate=50,count=1,payload=struct.pack("<hhh",1,2,3)),{})
        self.assertEqual(node.flags,1)
        self.assertEqual(node.state,4)
        self.assertEqual(node.queued,128)
        self.assertEqual(node.clock_hz,63000)
        self.assertEqual(node.last_status_seen,seen)

    def test_new_data_does_not_make_stale_status_ready(self):
        self.ready()
        self.server.nodes[1001].last_status_seen=time.monotonic()-31
        self.server.nodes[1001].last_seen=time.monotonic()
        self.assertTrue(any("节点未就绪" in reason for reason in self.server.readiness()))
        with self.assertRaisesRegex(ValueError,"节点未就绪"):self.server._request("start",{})

    def test_data_after_fault_does_not_mark_node_capturing(self):
        import struct
        self.server.run_id=77
        self.server._frame(Frame(STATUS,1001,run=77,flags=1,payload=STATUS_PAYLOAD.pack(0,0,0,0,0,5,1)),{})
        self.server._frame(Frame(DATA,1001,run=77,flags=3,rate=50,count=1,payload=struct.pack("<hhh",1,2,3)),{})
        self.assertEqual(self.server.nodes[1001].state,5)
