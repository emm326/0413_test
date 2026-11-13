"""Gateway configuration transactions with exact backups and read-back verification."""
import hashlib
import ipaddress
import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from .configure_gateway import configure, strip_comments, DEVICE_PROFILE, supports_control_tx
from .transports import TransportError

CONFIG_LINK = "/root/lorawanopen/gateway/.gwsrc/global_conf.json"
NETWORK_FILE = "/etc/systemd/network/01-eth0.network"
PROJECT_DIR = "/root/lorawanopen/gateway/.gwsrc/config"


def inventory():
    import psutil
    from serial.tools import list_ports
    stats = psutil.net_if_stats()
    adapters = []
    for name, addresses in psutil.net_if_addrs().items():
        if not stats.get(name) or not stats[name].isup:
            continue
        for address in addresses:
            if address.family == socket.AF_INET and not address.address.startswith("127."):
                adapters.append({"name": name, "address": address.address, "netmask": address.netmask})
    return {"ports": [{"port": p.device, "description": p.description} for p in list_ports.comports()], "adapters": adapters}


def static_network(cidr, computer):
    interface = ipaddress.IPv4Interface(cidr)
    host = ipaddress.IPv4Address(computer)
    if interface.network.prefixlen != 24 or host not in interface.network or host == interface.ip:
        raise ValueError("网关与电脑须是同一 /24 网段的不同地址")
    if interface.ip in (interface.network.network_address, interface.network.broadcast_address):
        raise ValueError("网关不能使用网络地址或广播地址")
    return f"[Match]\nName=eth0\n\n[Network]\nDHCP=no\nAddress={interface}\n".encode()


def local_preflight(cidr, computer, port=1700, check_conflict=True):
    info = inventory()
    static_network(cidr, computer)
    if computer not in [a["address"] for a in info["adapters"]]:
        raise ValueError("电脑当前没有所选 IPv4 地址，请先选择实际连接网关的网卡")
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind((computer, int(port)))
    except OSError as exc:
        raise ValueError(f"UDP {port} 已占用或不能绑定，请先停止当前接收服务：{exc}") from exc
    finally:
        probe.close()
    gateway = str(ipaddress.IPv4Interface(cidr).ip)
    if check_conflict:
        # A responding address must never be silently overwritten; ARP/neighbor
        # verification is performed separately against the device MAC on reapply.
        flag = "-n" if os.name == "nt" else "-c"
        wait = ["-w", "600"] if os.name == "nt" else ["-W", "1"]
        process = subprocess.run(["ping", flag, "1", *wait, gateway], capture_output=True, timeout=4,
                                 creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        reply = process.stdout.decode("utf-8", "replace").lower()
        if process.returncode == 0 and "ttl=" in reply:
            raise ValueError(f"{gateway} 已有设备响应；请先读取并核对当前网关地址后再应用")
    return info


def changes(before, after, prefix=""):
    result = []
    for key in sorted(set(before) | set(after)):
        a, b = before.get(key), after.get(key)
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(a, dict) and isinstance(b, dict):
            result += changes(a, b, path)
        elif a != b:
            result.append({"field": path, "before": a, "after": b})
    return result


class GatewayManager:
    def __init__(self, transport, state_dir="gateway/state", emit=None):
        self.transport = transport
        self.state_dir = Path(state_dir)
        self.emit = emit or (lambda kind, data: None)
        self.lock = threading.RLock()
        self.snapshot = None
        self.last_backup = None
        self.last_tx_test = None

    def read(self):
        with self.lock:
            identity = self.transport.run("cat /etc/machine-id").strip()
            if not re.fullmatch(r"[0-9a-fA-F]{32}", identity):
                raise TransportError("未读到可靠的网关设备身份")
            target = self.transport.run("readlink -f " + CONFIG_LINK).strip()
            if PurePosixPath(target).parent != PurePosixPath(PROJECT_DIR) or not re.fullmatch(r"[A-Za-z0-9_.-]+\.json", PurePosixPath(target).name):
                raise TransportError("配置目标不在预期目录，停止自动操作")
            config_bytes = self.transport.read_file(target)
            network = self.transport.read_file(NETWORK_FILE)
            config = json.loads(strip_comments(config_bytes.decode("utf-8-sig")))
            info = {
                "device_id": identity, "target": target,
                "config": config, "config_bytes": config_bytes, "network_bytes": network,
                "config_sha256": hashlib.sha256(config_bytes).hexdigest(),
                "status": self.transport.run("systemctl show gw -p ActiveState -p SubState -p MainPID -p NRestarts"),
                "network": self.transport.run("ip -4 addr show dev eth0; cat /sys/class/net/eth0/address"),
                "date": self.transport.run("date -R"),
                "hostname": self.transport.run("hostname"),
                "tx_allowed": supports_control_tx(config),
            }
            self.snapshot = info
            return info

    def backup(self):
        with self.lock:
            snap = self.read()
            name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            folder = self.state_dir / "backups" / (name + "_" + snap["device_id"][:8])
            folder.mkdir(parents=True)
            (folder / "config.json").write_bytes(snap["config_bytes"])
            (folder / "network.conf").write_bytes(snap["network_bytes"])
            for name, remote in (("gw.sh", "/root/lorawanopen/gateway/gw.sh"), ("gw.service", "/etc/systemd/system/gw.service")):
                (folder / name).write_bytes(self.transport.read_file(remote))
            manifest = {k: snap[k] for k in ("device_id", "target", "hostname", "config_sha256", "date")}
            manifest["sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in folder.iterdir()}
            (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
            # An on-device copy remains accessible if the computer is disconnected.
            remote_backup = "/root/lorawanopen/gateway/project_backups/" + folder.name
            q = shlex.quote
            self.transport.run("mkdir -p " + q(remote_backup) + "; cp -p " + q(snap["target"]) + " " + q(remote_backup + "/config.json") + "; cp -p " + NETWORK_FILE + " " + q(remote_backup + "/network.conf"))
            self.last_backup = folder
            self.emit("management", {"operation": "backup", "state": "completed", "path": str(folder)})
            return folder

    def preview(self, server, cidr="192.168.1.10/24", device_profile=DEVICE_PROFILE):
        with self.lock:
            snap = self.read()
            network = static_network(cidr, server)
            proposed, allowed = configure(snap["config"], server, device_profile)
            return {"device_id": snap["device_id"], "source_hash": snap["config_sha256"],
                    "network_hash": hashlib.sha256(snap["network_bytes"]).hexdigest(),
                    "changes": changes(snap["config"], proposed),
                    "network_before": snap["network_bytes"].decode("utf-8", "replace"),
                    "network_after": network.decode(), "config": proposed, "tx_allowed": allowed,
                    "server": server, "cidr": cidr}

    def _check_preview(self, preview):
        snap = self.read()
        if snap["device_id"] != preview["device_id"] or snap["config_sha256"] != preview["source_hash"]:
            raise TransportError("设备或配置已变化，请重新读取并预览")
        if hashlib.sha256(snap["network_bytes"]).hexdigest() != preview["network_hash"]:
            raise TransportError("网口配置已变化，请重新预览")

    def apply_network(self, preview):
        with self.lock:
            if self.transport.kind != "serial":
                raise TransportError("网口改动及恢复必须通过 USB2 串口执行")
            self._check_preview(preview)
            local_preflight(preview["cidr"], preview["server"], check_conflict= not re.search(r"\binet " + re.escape(str(ipaddress.IPv4Interface(preview["cidr"]).ip)) + r"/", self.snapshot["network"]))
            backup = self.backup()
            expected = static_network(preview["cidr"], preview["server"])
            try:
                self.transport.write_file(NETWORK_FILE, expected)
                self.transport.run("systemctl restart systemd-networkd", timeout=20)
                address = str(ipaddress.IPv4Interface(preview["cidr"]).ip)
                seen = ""
                for _ in range(8):
                    seen = self.transport.run("ip -4 addr show dev eth0")
                    if address + "/24" in seen:
                        break
                    time.sleep(.3)
                if address + "/24" not in seen or self.transport.read_file(NETWORK_FILE) != expected:
                    raise TransportError("网口配置读回或 IPv4 生效检查失败")
                self.read()
                return {"operation": "network", "state": "completed", "backup": str(backup), "network": seen}
            except Exception:
                self.transport.write_file(NETWORK_FILE, (backup / "network.conf").read_bytes())
                self.transport.run("systemctl restart systemd-networkd", timeout=20)
                raise

    def apply_project(self, preview):
        with self.lock:
            self._check_preview(preview)
            backup = self.backup()
            target = PROJECT_DIR + "/desktop_" + uuid.uuid4().hex[:12] + ".json"
            data = (json.dumps(preview["config"], ensure_ascii=False, indent=2) + "\n").encode()
            try:
                self.transport.run("systemctl stop gw")
                self.transport.write_file(target, data)
                self._point(target)
                self.transport.run("systemctl start gw")
                self._wait_active()
                actual = self.read()
                if actual["config_bytes"] != data or actual["target"] != target:
                    raise TransportError("项目配置读回不一致")
                self.state_dir.mkdir(parents=True, exist_ok=True)
                local = self.state_dir / "active.local.json"
                local.write_bytes(data)
                self.last_tx_test = None
                return {"operation": "project", "state": "completed", "backup": str(backup), "config_path": str(local), "target": target}
            except Exception:
                self.restore(backup, network=False)
                raise

    def _point(self, target):
        stage = CONFIG_LINK + ".new-" + uuid.uuid4().hex[:8]
        self.transport.run("ln -s " + shlex.quote(target) + " " + shlex.quote(stage) + " && mv -Tf " + shlex.quote(stage) + " " + CONFIG_LINK)

    def _wait_active(self):
        for _ in range(10):
            result = self.transport.run("systemctl is-active gw", check=False).strip()
            if result == "active":
                # Catch immediate crash/restart, not just transient 'active'.
                time.sleep(1)
                status = self.transport.run("systemctl show gw -p ActiveState -p SubState -p NRestarts")
                if "ActiveState=active" in status and "SubState=running" in status:
                    return
            time.sleep(.3)
        raise TransportError("网关服务未稳定启动，自动恢复上一配置")

    def restore(self, folder, network=True):
        with self.lock:
            folder = Path(folder)
            manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
            if set(manifest["sha256"]) != {"config.json", "network.conf", "gw.sh", "gw.service"}:
                raise TransportError("备份清单内容不符合设备备份格式")
            for name, digest in manifest["sha256"].items():
                if hashlib.sha256((folder / name).read_bytes()).hexdigest() != digest:
                    raise TransportError("备份被修改，停止恢复")
            if self.transport.run("cat /etc/machine-id").strip() != manifest["device_id"]:
                raise TransportError("备份不属于当前网关")
            if network and self.transport.kind != "serial":
                raise TransportError("恢复网络需 USB2，避免 SSH 中断")
            target = manifest["target"]
            if PurePosixPath(target).parent != PurePosixPath(PROJECT_DIR) or not re.fullmatch(r"[A-Za-z0-9_.-]+\.json", PurePosixPath(target).name):
                raise TransportError("备份配置路径不在设备目录")
            self.transport.run("systemctl stop gw")
            original = (folder / "config.json").read_bytes()
            if self.transport.read_file(target) != original:
                self.transport.write_file(target, original)
            self._point(target)
            if network:
                self.transport.write_file(NETWORK_FILE, (folder / "network.conf").read_bytes())
                self.transport.run("systemctl restart systemd-networkd", timeout=20)
            self.transport.run("systemctl start gw")
            self._wait_active()
            snap = self.read()
            if snap["config_bytes"] != (folder / "config.json").read_bytes() or (network and snap["network_bytes"] != (folder / "network.conf").read_bytes()):
                raise TransportError("恢复读回失败，需要人工检查")
            self.last_tx_test = None
            self.state_dir.mkdir(parents=True, exist_ok=True)
            (self.state_dir / "active.local.json").write_bytes(snap["config_bytes"])
            return {"operation": "restore", "state": "completed", "target": target}

    def log_cursor(self):
        try:
            boot_id = uuid.UUID(self.transport.run("cat /proc/sys/kernel/random/boot_id").strip()).hex
        except ValueError as exc:
            raise TransportError("无法读取当前启动身份，不能验证发送日志") from exc
        self._log_boot_id = boot_id
        text = self.transport.run("journalctl --boot=" + shlex.quote(boot_id) + " -u gw -n 0 --show-cursor --no-pager -o cat")
        match = re.search(r"-- cursor: (\S+)", text)
        if not match:
            raise TransportError("无法获取日志游标，不能验证实际发送计数")
        return match.group(1)

    def logs(self, cursor=None):
        option = "--after-cursor=" + shlex.quote(cursor) if cursor else "-n 100"
        if cursor:
            boot_id = getattr(self, "_log_boot_id", None)
            if boot_id:
                current = uuid.UUID(self.transport.run("cat /proc/sys/kernel/random/boot_id").strip()).hex
                if current != boot_id:
                    raise TransportError("网关在测试期间重启，日志证据失效，请重新连接后测试")
                boot_option = "--boot=" + shlex.quote(boot_id)
            else:
                boot_option = "-b"
        else:
            boot_option = "-b"
        return self.transport.run("journalctl " + boot_option + " -u gw " + option + " --no-pager -o cat", timeout=15)

    def verify_tx(self, cursor, expected=3):
        text = self.logs(cursor)
        sent = sum(int(x) for x in re.findall(r"# RF packets sent to concentrator:\s*(\d+)", text))
        errors = sum(int(x) for x in re.findall(r"# TX errors:\s*(\d+)", text))
        rejected = re.findall(r"(?im)^.*(?:Packet REJECTED|lgw_send failed|ERROR:).*?$", text)
        return {"sent": sent, "errors": errors, "rejected": rejected, "passed": sent >= expected and errors == 0 and not rejected}

    def close(self):
        with self.lock:
            self.transport.close()
