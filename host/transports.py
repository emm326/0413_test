"""Framed, bounded device transports. Passwords never enter commands or logs."""
import base64
import hashlib
import re
import shlex
import threading
import time
import uuid
from pathlib import Path


class TransportError(RuntimeError):
    pass


class SerialTransport:
    kind = "serial"

    def __init__(self, port, username, password, timeout=12):
        import serial
        self.lock = threading.RLock()
        self.timeout = timeout
        self.port = serial.Serial(port=None, baudrate=115200, timeout=.1, write_timeout=2)
        self.port.port = port
        self.port.dtr = False
        self.port.rts = False
        try:
            self.port.open()
            self.port.reset_input_buffer()
            self.port.write(b"\r")
            text = self._read_login(2)
            if re.search(r"login:\s*$", text):
                self.port.write((username + "\r").encode())
                text = self._read_login(2)
            if re.search(r"Password:\s*$", text):
                self.port.write((password + "\r").encode())
                text = self._read_login(5)
            if "#" not in text and "$" not in text:
                raise TransportError("串口登录失败；请检查账号、密码或是否连接 USB2")
            self.run("stty -echo; export SYSTEMD_PAGER=cat SYSTEMD_COLORS=0 TERM=dumb")
        except Exception:
            self.port.close()
            raise

    def _read_login(self, timeout):
        deadline = time.monotonic() + timeout
        text = ""
        while time.monotonic() < deadline:
            text += self.port.read(max(self.port.in_waiting, 1)).decode("utf-8", "replace")
            if re.search(r"(?m)^(?:(?!Last )[\w.-]+ )?login:\s*$|^Password:\s*$|^[^\r\n]*[#$]\s*$", text):
                break
        return text

    def run(self, command, timeout=None, check=True):
        with self.lock:
            tag = uuid.uuid4().hex
            start, end = "__BEGIN_" + tag, "__END_" + tag
            wire = f"printf '\\n{start}\\n'; ( {command} ); r=$?; printf '\\n{end}:%s\\n' \"$r\"\r"
            if len(wire.encode()) > 3500:
                raise TransportError("串口命令过长")
            self.port.reset_input_buffer()
            self.port.write(wire.encode())
            deadline = time.monotonic() + (timeout or self.timeout)
            raw = bytearray()
            while time.monotonic() < deadline:
                raw.extend(self.port.read(max(self.port.in_waiting, 1)))
                text = raw.decode("utf-8", "replace").replace("\r", "")
                match = re.search(rf"(?m)^{end}:(\d+)\s*$", text)
                begin = re.search(rf"(?m)^{start}\n", text)
                if match and begin:
                    output = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", text[begin.end():match.start()]).rstrip("\n")
                    status = int(match.group(1))
                    if check and status:
                        raise TransportError(f"设备命令失败({status})：{output[-1000:]}")
                    return output
            raise TransportError("串口读取超时；已中止后续操作，请重新连接后读取实际状态")

    def read_file(self, path):
        data = base64.b64decode(self.run("base64 " + shlex.quote(path)), validate=False)
        digest = self.run("sha256sum " + shlex.quote(path)).split()[0]
        if hashlib.sha256(data).hexdigest() != digest:
            raise TransportError("串口文件校验不一致")
        return data

    def write_file(self, path, data):
        with self.lock:
            stage = path + ".upload-" + uuid.uuid4().hex[:8]
            encoded = base64.b64encode(data).decode()
            try:
                self.run(": > " + shlex.quote(stage))
                for offset in range(0, len(encoded), 800):
                    self.run("printf %s " + shlex.quote(encoded[offset:offset + 800]) + " >> " + shlex.quote(stage))
                self.run("base64 -d " + shlex.quote(stage) + " > " + shlex.quote(stage + ".decoded"))
                if self.run("sha256sum " + shlex.quote(stage + ".decoded")).split()[0] != hashlib.sha256(data).hexdigest():
                    raise TransportError("上传文件校验失败")
                mode = "644" if path == "/etc/systemd/network/01-eth0.network" else "600"
                self.run("chmod " + mode + " " + shlex.quote(stage + ".decoded") + "; mv -f " + shlex.quote(stage + ".decoded") + " " + shlex.quote(path))
            finally:
                self.run("rm -f -- " + shlex.quote(stage) + " " + shlex.quote(stage + ".decoded"), check=False)

    def close(self):
        try:
            if self.port.is_open:
                self.run("stty echo", timeout=2)
                self.port.write(b"exit\r")
                self.port.flush()
                self._read_login(3)
        except Exception:
            pass
        self.port.close()


class SSHTransport:
    kind = "ssh"

    def __init__(self, host, username, password, known_hosts, timeout=8):
        import paramiko
        self.lock = threading.RLock()
        self.client = paramiko.SSHClient()
        known_hosts = Path(known_hosts)
        known_hosts.parent.mkdir(parents=True, exist_ok=True)
        if known_hosts.exists():
            self.client.load_host_keys(str(known_hosts))
        # TOFU persists the key; subsequent changed host keys are rejected.
        class RememberKey(paramiko.MissingHostKeyPolicy):
            def missing_host_key(inner, client, hostname, key):
                client.get_host_keys().add(hostname, key.get_name(), key)
                client.save_host_keys(str(known_hosts))
        self.client.set_missing_host_key_policy(RememberKey())
        try:
            self.client.connect(host, username=username, password=password, timeout=timeout,
                                auth_timeout=timeout, banner_timeout=timeout,
                                look_for_keys=False, allow_agent=False)
            self.sftp = self.client.open_sftp()
        except Exception:
            self.client.close()
            raise

    def run(self, command, timeout=12, check=True):
        with self.lock:
            _, stdout, stderr = self.client.exec_command(command, timeout=timeout)
            output = stdout.read().decode("utf-8", "replace")
            error = stderr.read().decode("utf-8", "replace")
            status = stdout.channel.recv_exit_status()
            if check and status:
                raise TransportError(f"设备命令失败({status})：{error[-1000:] or output[-1000:]}")
            return output.rstrip("\n")

    def read_file(self, path):
        with self.lock:
            with self.sftp.open(path, "rb") as stream:
                data = stream.read()
            if hashlib.sha256(data).hexdigest() != self.run("sha256sum " + shlex.quote(path)).split()[0]:
                raise TransportError("SFTP 文件校验失败")
            return data

    def write_file(self, path, data):
        with self.lock:
            stage = path + ".upload-" + uuid.uuid4().hex[:8]
            try:
                with self.sftp.open(stage, "wb") as stream:
                    stream.write(data)
                if self.run("sha256sum " + shlex.quote(stage)).split()[0] != hashlib.sha256(data).hexdigest():
                    raise TransportError("SFTP 上传校验失败")
                mode = "644" if path == "/etc/systemd/network/01-eth0.network" else "600"
                self.run("chmod " + mode + " " + shlex.quote(stage) + "; mv -f " + shlex.quote(stage) + " " + shlex.quote(path))
            finally:
                self.run("rm -f -- " + shlex.quote(stage), check=False)

    def close(self):
        self.sftp.close()
        self.client.close()
