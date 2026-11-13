"""Semtech UDP v2 endpoint; all socket/state changes belong to its worker thread."""

import base64
import json
import math
import queue
import secrets
import socket
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass
from .wire import (
    ACK,
    ACK_ENTRY,
    CONFIRM,
    DATA,
    DISCOVER,
    GPS_HEALTHY,
    NODE_FREQUENCIES,
    STATUS,
    STATUS_PAYLOAD,
    STOP,
    BROADCAST,
    Frame,
    STATE_NAMES,
    RESULT_NAMES,
    airtime_seconds,
    start_frame,
    START,
)
from .recorder import Recorder


@dataclass(frozen=True)
class DiagnosticPacket:
    index: int
    session: bytes
    kind: int = 0
    run: int = 0

    def encode(self):
        return b"E870-DIAG\x01" + self.session + bytes([self.index])


@dataclass
class Node:
    node: int
    channel: float = 0
    state: int = 0
    flags: int = 0
    result: int = 0
    run: int = 0
    last_seen: float = 0
    last_status_seen: float = 0
    clock_hz: float = 0
    clock_ppm: float = 0
    queued: int = 0
    packets: int = 0
    samples: int = 0
    rssi: float = 0
    snr: float = 0
    confirmed: bool = False
    lost_samples: int = 0
    lost_packets: int = 0
    retransmissions: int = 0


class GatewayServer:
    def __init__(
        self,
        bind="0.0.0.0",
        port=1700,
        output="data/e870",
        nodes=tuple(range(1001, 1009)),
        emit=None,
        tx_range_verified=False,
    ):
        self.bind, self.port = bind, port
        self.targets = tuple(int(n) for n in nodes)
        if (
            not self.targets
            or len(set(self.targets)) != len(self.targets)
            or not all(n in NODE_FREQUENCIES for n in self.targets)
        ):
            raise ValueError("use unique node ids 1001..1008")
        self.nodes = {n: Node(n, channel=NODE_FREQUENCIES[n]) for n in self.targets}
        self.emit = emit or (lambda kind, payload: None)
        self.recorder = Recorder(output)
        self.requests = queue.Queue()
        self.stop_event = threading.Event()
        self.socket = None
        self.thread = None
        self.downlink_peer = None
        self.peer_seen = 0
        self.gateway_eui = None
        self.run_id = 0
        self.start_us = 0
        self.active_targets = self.targets
        self.offsets = deque(maxlen=9)
        self.pending_acks = {}
        self.downlinks = []
        self.pending_tokens = {}
        self.unknown_nodes = {}
        self.tx_range_verified = bool(tx_range_verified)
        self.ack_cycle = -1
        self.tx_ready = 0
        self.awaiting_start = set()
        self.start_reported = False
        self.last_report = 0
        self.commands = {}
        self.command_context = {}
        self.current_command = None
        self.start_command = None
        self.stop_command = None
        self.cancel_run = None
        self.stats = {"push": 0, "pull": 0, "tx_ack": 0, "invalid": 0, "tx": 0}
        self.gateway_stat = {}
        self.stat_samples = deque(maxlen=128)
        self.stat_fingerprints = {}
        self.expected_eui = None

    @property
    def gnss_now_us(self):
        return int(
            (time.time() + (statistics.median(self.offsets) if self.offsets else 0)) * 1_000_000
        )

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.socket.bind((self.bind, self.port))
        except Exception:
            self.socket.close()
            self.socket = None
            raise
        self.port = self.socket.getsockname()[1]
        self.socket.settimeout(0.05)
        self.thread = threading.Thread(target=self._loop, name="e870-receiver", daemon=True)
        self.thread.start()
        self.emit("log", f"监听UDP {self.bind}:{self.port}")

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)
            if self.thread.is_alive():
                raise RuntimeError("接收线程未结束，不能关闭文件或重新占用端口")
        if self.socket:
            self.socket.close()
            self.socket = None
        self.thread = None
        self.recorder.close()

    def request(self, action, **options):
        identifier = secrets.token_hex(6)
        self.requests.put((action, dict(options, _command_id=identifier)))
        return identifier

    def _command(self, identifier, state, **values):
        if identifier not in self.commands:
            return
        item = self.commands[identifier]
        if item["state"] in ("completed", "failed", "cancelled", "timeout", "gateway_rejected") and state in ("sent", "gateway_accepted", "waiting_nodes"):
            return
        item.update(values, state=state, updated_utc=time.time())
        self.emit("command", dict(item))

    def readiness(self, targets=None):
        reasons = []
        targets = self.targets if targets is None else tuple(targets)
        if not targets or len(set(targets)) != len(targets) or any(n not in self.nodes for n in targets):
            reasons.append("请选择有效目标节点")
        if self.run_id or self.cancel_run:
            reasons.append("上一运行或取消尚未确认结束")
        if not self.tx_range_verified:
            reasons.append("当前配置未允许470.3MHz下发")
        if not self.downlink_peer or time.monotonic() - self.peer_seen > 30:
            reasons.append("尚未收到有效网关PULL心跳")
        missing = [n for n in targets if n in self.nodes and
                   (self.nodes[n].state != 1 or self.nodes[n].flags & 3 != 3 or
                    time.monotonic() - self.nodes[n].last_status_seen > 30)]
        if missing:
            reasons.append("节点未就绪（在线/待机/GNSS/PPS/时钟）：" + ",".join(map(str, missing)))
        if not self.offsets:
            reasons.append("尚无节点GNSS时间")
        return reasons

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                raw, address = self.socket.recvfrom(65535)
                self.handle_datagram(raw, address)
            except socket.timeout:
                pass
            except (ValueError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
                self.stats["invalid"] += 1
                self.emit("log", f"丢弃无效报文：{exc}")
            except OSError as exc:
                if not self.stop_event.is_set():
                    self.emit("log", str(exc))
            while True:
                try:
                    action, options = self.requests.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._request(action, options)
                except (ValueError, RuntimeError, OSError) as exc:
                    self.emit("log", str(exc))
            try:
                self._schedule()
            except (RuntimeError, OSError) as exc:
                self.emit("log", "接收服务后台错误：" + str(exc))

    def handle_datagram(self, raw, address):
        if len(raw) < 12 or raw[0] != 2:
            raise ValueError("expected Semtech UDP v2 gateway header")
        token = raw[1:3]
        kind = raw[3]
        eui = raw[4:12].hex()
        if self.expected_eui and eui != self.expected_eui:
            raise ValueError("gateway EUI does not match selected device configuration")
        if self.gateway_eui and eui != self.gateway_eui:
            raise ValueError("this session already belongs to another gateway")
        self.gateway_eui = eui
        if kind == 2:
            self.stats["pull"] += 1
            self.downlink_peer = address
            self.peer_seen = time.monotonic()
            self.socket.sendto(bytes([2]) + token + bytes([4]), address)
            self.emit("gateway", {"eui": eui, "peer": address})
        elif kind == 0:
            self.stats["push"] += 1
            self.socket.sendto(bytes([2]) + token + bytes([1]), address)
            body = json.loads(raw[12:].decode("utf-8"))
            if "stat" in body:
                stat = body["stat"]
                if not isinstance(stat, dict):
                    raise ValueError("invalid gateway status")
                self.gateway_stat = dict(stat)
                fingerprint = json.dumps(stat, sort_keys=True, separators=(",", ":"))
                if fingerprint not in self.stat_fingerprints:
                    self.stat_fingerprints[fingerprint] = None
                    if len(self.stat_fingerprints) > 128:
                        self.stat_fingerprints.pop(next(iter(self.stat_fingerprints)))
                    self.stat_samples.append({"received_mono": time.monotonic(), "stat": dict(stat)})
                    self.emit("gateway_stat", dict(self.gateway_stat))
            for rx in body.get("rxpk", []):
                if rx.get("stat") != 1:
                    continue
                payload = base64.b64decode(rx["data"], validate=True)
                if rx.get("size", len(payload)) != len(payload):
                    raise ValueError("rxpk size mismatch")
                try:
                    frame = Frame.decode(payload)
                except ValueError:
                    self.stats["invalid"] += 1
                    continue
                if frame.node not in self.nodes:
                    count = self.unknown_nodes.get(frame.node, 0) + 1
                    self.unknown_nodes[frame.node] = count
                    self.emit("unknown", {"node": frame.node, "count": count})
                    if count == 1:
                        self.emit("log", f"忽略非目标节点{frame.node}的数据")
                    continue
                self._frame(frame, rx)
        elif kind == 5:
            if address != self.downlink_peer:
                raise ValueError("TX_ACK from unexpected peer")
            result = json.loads(raw[12:].decode("utf-8")) if len(raw) > 12 else {}
            error = result.get("txpk_ack", {}).get("error", "NONE")
            pending = self.pending_tokens.pop(token.hex(), None)
            if pending is None:
                self.emit("log", "忽略迟到、重复或未知token的TX_ACK")
                return
            self.stats["tx_ack"] += 1
            identifier = pending["command_id"]
            self.emit("tx_ack", {"token": token.hex(), "error": error, "command_id": identifier,
                                 "purpose": pending["purpose"], "utc": time.time()})
            if identifier in self.commands:
                command = self.commands[identifier]
                command.setdefault("gateway_results", {})[token.hex()] = error
                if error != "NONE":
                    self._command(identifier, "gateway_rejected", error=error)
                elif command.get("state") not in ("completed", "timeout", "failed", "cancelled"):
                    self._command(identifier, "gateway_accepted", accepted=len(command["gateway_results"]))
            self.emit("log", f"网关发送结果 {pending['purpose']}: {error}（节点确认另行显示）")
        else:
            raise ValueError("unsupported gateway message type")

    def tx_success_since(self, started_mono):
        samples = []
        total = 0
        for sample in list(self.stat_samples):
            if sample["received_mono"] < started_mono:
                continue
            stat = sample["stat"]
            value = stat.get("txnb")
            if type(value) is not int or value < 0:
                continue
            total += value
            samples.append({"received_mono": sample["received_mono"], "time": stat.get("time"),
                            "txnb": value, "dwnb": stat.get("dwnb")})
        return {"sent": total, "samples": samples}

    def _frame(self, frame, rx):
        if frame.kind == DATA and frame.run != self.run_id:
            self.emit("log", f"忽略非当前运行数据：节点{frame.node} run={frame.run}")
            return
        node = self.nodes[frame.node]
        node.last_seen = time.monotonic()
        node.rssi = rx.get("rssi", 0)
        node.snr = rx.get("lsnr", 0)
        if frame.kind in (STATUS, CONFIRM):
            if len(frame.payload) != STATUS_PAYLOAD.size:
                raise ValueError("invalid status payload")
            node.flags = frame.flags
            node.last_status_seen = time.monotonic()
            (
                node.clock_hz,
                node.clock_ppm,
                lost_samples,
                lost_packets,
                node.queued,
                node.state,
                node.result,
            ) = STATUS_PAYLOAD.unpack(frame.payload)
            node.run = frame.run
            node.lost_samples = lost_samples
            node.lost_packets = lost_packets
            if (frame.kind == CONFIRM and self.run_id and self.awaiting_start and
                    frame.node in self.active_targets and frame.run == self.run_id and
                    node.result == 0 and node.state == 2 and not self.stop_command):
                self.awaiting_start.discard(frame.node)
                node.confirmed = True
                self._command(self.start_command, "waiting_nodes", missing=sorted(self.awaiting_start))
                if not self.awaiting_start:
                    self._command(self.start_command, "nodes_confirmed", missing=[])
                    self.emit("log", f"run={self.run_id} 的全部目标节点已确认")
            if frame.epoch_us and frame.flags & GPS_HEALTHY:
                datr = rx.get("datr", "SF6BW125")
                sf = int(datr.split("BW")[0][2:])
                bw = int(datr.split("BW")[1])
                offset = (
                    frame.epoch_us / 1_000_000
                    + airtime_seconds(30 + len(frame.payload), sf, bw)
                    - time.time()
                )
                if math.isfinite(offset):
                    self.offsets.append(offset)
            for identifier, command in list(self.commands.items()):
                if (command["action"] == "discover" and command.get("query_run") == frame.run and
                        command["state"] not in ("completed", "timeout", "failed")):
                    found = set(command.get("found", []))
                    found.add(frame.node)
                    self._command(identifier, "waiting_nodes", found=sorted(found))
            if self.cancel_run and frame.run == self.cancel_run["run"] and node.state == 1 and not node.queued:
                self.cancel_run["missing"].discard(frame.node)
                if not self.cancel_run["missing"]:
                    self.cancel_run = None
                    self._command(self.stop_command, "completed", missing=[])
                    self.stop_command = None
            self._finish_run_if_idle()
            self.emit(
                "log",
                f"节点{frame.node}：{STATE_NAMES.get(node.state,node.state)}，{RESULT_NAMES.get(node.result,node.result)}，丢样{lost_samples}，丢包{lost_packets}",
            )
        elif frame.kind == DATA:
            if frame.run != self.run_id:
                self.emit("log", f"忽略非当前运行数据：节点{frame.node} run={frame.run}")
                return
            if frame.node not in self.active_targets:
                count = self.unknown_nodes.get(frame.node, 0) + 1
                self.unknown_nodes[frame.node] = count
                self.emit("unknown", {"node": frame.node, "count": count})
                self.emit("log", f"忽略非本次目标节点{frame.node}的数据")
                return
            node.run = frame.run
            if node.state not in (4, 5):
                node.state = 3
            self.pending_acks.setdefault(frame.node, set()).add(frame.sequence)
            rx = dict(rx, received_utc_us=self.gnss_now_us)
            if self.recorder.record(frame, rx):
                node.packets += 1
                node.samples += frame.count
            else:
                node.retransmissions += 1
        self.emit("node", dict(vars(node)))

    def _finish_run_if_idle(self):
        if not self.run_id or self.awaiting_start or not self.start_reported:
            return
        if not all(
            self.nodes[n].run == self.run_id and self.nodes[n].state == 1 and not self.nodes[n].queued
            for n in self.active_targets
        ):
            return
        finished_run = self.run_id
        for n in self.active_targets:
            self.nodes[n].confirmed = False
        self.run_id = 0
        self.start_us = 0
        self.active_targets = self.targets
        self.pending_acks.clear()
        self.ack_cycle = -1
        self.downlinks = [item for item in self.downlinks if not (item[1].kind == STOP and item[1].run == finished_run)]
        self._command(self.stop_command, "completed", missing=[])
        self.stop_command = None
        self.emit("log", f"run={finished_run} 的全部目标节点已回到待机，运行结束")

    def _request(self, action, options):
        if action == "self_test_result":
            identifier = options["identifier"]
            command = self.commands.get(identifier, {})
            acknowledgments = command.get("gateway_results", {})
            passed = (options.get("passed") and len(acknowledgments) == 3 and
                      all(x == "NONE" for x in acknowledgments.values()) and
                      command.get("state") not in ("failed", "timeout", "gateway_rejected"))
            self._command(identifier, "completed" if passed else "failed", evidence=options.get("evidence"),
                          error="" if passed else "下发自检证据不完整或失败")
            return identifier
        options = dict(options)
        identifier = options.pop("_command_id", None) or secrets.token_hex(6)
        self.commands[identifier] = {"id": identifier, "action": action, "run": self.run_id,
                                     "targets": list(options.get("nodes", self.targets)),
                                     "created_utc": time.time(), "created_mono": time.monotonic(),
                                     "state": "queued", "gateway_results": {}}
        self.current_command = identifier
        self._command(identifier, "queued")
        try:
            self._request_core(action, options)
        except (ValueError, RuntimeError, OSError) as exc:
            self._command(identifier, "failed", error=str(exc))
            raise
        finally:
            self.current_command = None
        return identifier

    def _request_core(self, action, options):
        if action in ("discover", "start", "stop") and any(
            command["action"] == "self_test" and command["state"] not in
            ("completed", "failed", "timeout", "gateway_rejected")
            for command in self.commands.values()
        ):
            raise ValueError("射频自检进行中，暂不发送其他命令")
        if action == "discover":
            if not self.tx_range_verified or not self.downlink_peer or time.monotonic()-self.peer_seen > 30:
                raise RuntimeError("查询需要有效470.3MHz配置和网关下行心跳")
            query_run = secrets.randbits(32) or 1
            self.commands[self.current_command]["query_run"] = query_run
            self._queue(Frame(DISCOVER, BROADCAST, run=query_run), "查询节点", control_only=bool(self.start_us))
        elif action == "self_test":
            if self.run_id or self.cancel_run:
                raise ValueError("运行或取消待确认期间不能执行射频自检")
            if any(c['action'] == 'self_test' and c['id'] != self.current_command and c['state'] not in
                   ('completed','failed','timeout','gateway_rejected') for c in self.commands.values()):
                raise ValueError("已有下发自检进行中")
            if not options.get("antenna_confirmed"):
                raise ValueError("先确认LoRa天线已接好")
            if self.downlinks or self.pending_tokens:
                raise ValueError("其他下发尚未结束，请等待后再自检")
            if not self.tx_range_verified or not self.downlink_peer or time.monotonic()-self.peer_seen > 30:
                raise RuntimeError("下行配置或网关通道未就绪")
            session = secrets.token_bytes(8)
            for index in range(3):
                self._queue(DiagnosticPacket(index, session), "专用下发自检", time.monotonic()+2*index)
            self.commands[self.current_command]["expected_acks"] = 3
        elif action == "start":
            if self.run_id or self.cancel_run:
                raise ValueError(f"已有运行 run={self.run_id} 或取消尚未结束，拒绝重复START")
            targets = tuple(options.get("nodes", self.targets))
            if (
                not targets
                or len(set(targets)) != len(targets)
                or any(n not in self.nodes for n in targets)
            ):
                raise ValueError("启动节点必须是当前配置中的唯一节点")
            rate = options.get("rate", 50)
            sf = options.get("sf", 6)
            if (rate, sf) not in ((50, 6), (100, 5)):
                raise ValueError("50Hz必须使用SF6；100Hz必须显式选择SF5")
            if not self.tx_range_verified:
                raise RuntimeError("尚未验证E870原厂配置支持470.3MHz下行，禁止启动采集")
            missing = [
                n
                for n in targets
                if n not in self.nodes
                or self.nodes[n].state != 1
                or self.nodes[n].flags & 3 != 3
                or time.monotonic() - self.nodes[n].last_status_seen > 30
            ]
            if missing:
                raise ValueError(f"节点未就绪：{missing}；等待60秒有效PPS计数后重新查询")
            if not self.offsets:
                raise ValueError("尚未从节点状态取得GNSS时间")
            start_frame(
                1,
                1,
                rate,
                options.get("range_g", 4),
                sf,
                options.get("duration", 0),
                node_count=len(targets),
            )
            if not self.downlink_peer or time.monotonic() - self.peer_seen > 30:
                raise RuntimeError("网关下行未连接，不能发起采集")
            self.run_id = secrets.randbits(32) or 1
            self.start_us = ((self.gnss_now_us + 10_000_000 + 999999) // 1_000_000) * 1_000_000
            self.active_targets = targets
            self.pending_acks.clear()
            self.ack_cycle = -1
            self.awaiting_start = set(targets)
            self.start_command = self.current_command
            self._command(self.start_command, "queued", run=self.run_id, missing=sorted(targets))
            for node in self.nodes.values():
                node.confirmed = False
            self.start_reported = False
            mask = sum(1 << (n - 1001) for n in targets)
            for repeat in range(3):
                f = start_frame(
                    self.run_id,
                    self.start_us,
                    rate,
                    options.get("range_g", 4),
                    sf,
                    options.get("duration", 0),
                    node_count=len(targets),
                    node_mask=mask,
                )
                self._queue(f, "START广播", time.monotonic() + repeat * 2.5)
            self.emit("log", f"开始请求 run={self.run_id}，UTC启动={self.start_us}，等待各节点确认")
        elif action == "stop":
            if not self.run_id and not self.cancel_run:
                raise ValueError("当前没有运行，禁止广播无运行编号的STOP")
            if self.stop_command and self.commands[self.stop_command]["state"] not in ("timeout", "failed", "gateway_rejected"):
                raise ValueError("停止请求正在等待节点回复")
            for item in self.downlinks:
                if item[1].kind == START:
                    self.command_context.pop(id(item[1]), None)
            self.downlinks = [item for item in self.downlinks if item[1].kind != START]
            self.awaiting_start.clear()
            self.start_reported = True
            self.stop_command = self.current_command
            run = self.run_id or self.cancel_run["run"]
            self._command(self.stop_command, "queued", run=run)
            for repeat in range(3):
                self._queue(Frame(STOP, BROADCAST, run=run), "STOP", time.monotonic()+repeat*5,
                            control_only=bool(self.start_us))
        else:
            raise ValueError("unknown host action")

    def _queue(self, frame, purpose, when=None, control_only=False):
        self.command_context.pop(id(frame), None)
        if self.current_command:
            self.command_context[id(frame)] = self.current_command
        self.downlinks.append(
            (time.monotonic() if when is None else when, frame, purpose, control_only)
        )

    def _send(self, frame, purpose):
        if not self.tx_range_verified:
            raise RuntimeError("E870的470.3MHz下行范围尚未通过原厂配置验证")
        if not self.downlink_peer or time.monotonic() - self.peer_seen > 30:
            raise RuntimeError("尚未建立网关下行通道，请核对服务器下行端口1700")
        payload = frame.encode()
        token = secrets.token_bytes(2)
        while token.hex() in self.pending_tokens:
            token = secrets.token_bytes(2)
        tx = {
            "imme": True,
            "freq": 470.3,
            "rfch": 0,
            "powe": 14,
            "modu": "LORA",
            "datr": "SF6BW125",
            "codr": "4/5",
            "ipol": False,
            "prea": 12,
            "ncrc": False,
            "size": len(payload),
            "data": base64.b64encode(payload).decode("ascii"),
        }
        packet = bytes([2]) + token + bytes([3]) + json.dumps({"txpk": tx}).encode("utf-8")
        self.socket.sendto(packet, self.downlink_peer)
        self.stats["tx"] += 1
        identifier = self.command_context.pop(id(frame), None)
        self.pending_tokens[token.hex()] = {"purpose": purpose, "command_id": identifier, "sent_mono": time.monotonic()}
        self._command(identifier, "sent", token=token.hex(), sent_utc=time.time())

    def _schedule(self):
        now = time.monotonic()
        phase = None
        cycle = -1
        for token, pending in list(self.pending_tokens.items()):
            if now - pending["sent_mono"] > 5:
                self.pending_tokens.pop(token)
                self._command(pending["command_id"], "timeout", error="TX_ACK超时", token=token)
        for identifier, command in list(self.commands.items()):
            elapsed = now - command["created_mono"]
            if command["action"] == "discover" and elapsed > 15 and command["state"] not in ("completed", "timeout", "failed", "gateway_rejected"):
                self._command(identifier, "completed", found=command.get("found", []), message="查询结束" if command.get("found") else "暂未发现节点")
            if command["action"] == "stop" and elapsed > 30 and command["state"] not in ("completed", "timeout", "failed"):
                missing = sorted(self.cancel_run["missing"]) if self.cancel_run else [n for n in self.active_targets if self.nodes[n].run != self.run_id or self.nodes[n].state != 1 or self.nodes[n].queued]
                self._command(identifier, "timeout", missing=missing, error="停止未确认，保留运行")
            if command["action"] == "self_test" and elapsed > 50 and command["state"] not in ("completed", "failed", "timeout", "gateway_rejected"):
                self._command(identifier, "timeout", error="下发自检未获得完整证据")
        if len(self.commands) > 500:
            for identifier in list(self.commands)[:100]:
                if self.commands[identifier]["state"] in ("completed", "timeout", "failed", "cancelled"):
                    self.commands.pop(identifier)
        if now - getattr(self, "last_transport_emit", 0) >= .5:
            self.last_transport_emit = now
            self.emit("transport", dict(self.stats, connected=bool(self.downlink_peer and now-self.peer_seen <= 30)))
        if now - self.last_report > 10:
            self.recorder.save_report()
            self.last_report = now
        if self.start_us and not self.start_reported:
            if self.awaiting_start and self.gnss_now_us >= self.start_us - 2_000_000:
                missing = sorted(self.awaiting_start)
                cancelled_run = self.run_id
                self.downlinks = [item for item in self.downlinks if item[1].kind != START]
                self.cancel_run = {"run": cancelled_run, "missing": set(self.active_targets)}
                self._command(self.start_command, "cancelled", missing=missing, error="开始前未收齐确认，正在取消")
                previous = self.current_command
                self.current_command = secrets.token_hex(6)
                self.commands[self.current_command] = {"id": self.current_command, "action": "stop", "run": cancelled_run,
                    "state": "queued", "created_mono": now, "created_utc": time.time(), "targets": list(self.active_targets), "gateway_results": {}}
                self.stop_command = self.current_command
                for repeat in range(3):
                    self._queue(Frame(STOP, BROADCAST, run=cancelled_run), "取消未完整确认的启动", now+repeat*5)
                self.current_command = previous
                self.emit("log", f"取消run={cancelled_run}：节点未确认 {missing}")
                self.start_reported = True
                self.start_us = 0
                self.run_id = 0
                self.active_targets = self.targets
                self.awaiting_start.clear()
                self.pending_acks.clear()
            elif not self.awaiting_start and self.gnss_now_us >= self.start_us:
                self.start_reported = True
                self._command(self.start_command, "completed", run=self.run_id, message="全部节点已确认，已到预约时间")
                self.emit("log", f"run={self.run_id} 已由全部目标节点确认并开始采集")
        if self.start_us and self.gnss_now_us >= self.start_us:
            elapsed = self.gnss_now_us - self.start_us
            phase = elapsed % 5_000_000
            cycle = elapsed // 5_000_000
        if phase is not None and 4_030_000 <= phase < 4_100_000 and self.ack_cycle != cycle:
            self.ack_cycle = cycle
            entries = []
            for node in self.active_targets:
                sequences = self.pending_acks.get(node)
                if not sequences:
                    continue
                first = min(sequences)
                covered = {seq for seq in sequences if first <= seq < first + 32}
                bitmap = sum(1 << (seq - first) for seq in covered)
                entries.append(ACK_ENTRY.pack(node, first, bitmap))
                sequences.difference_update(covered)
                if not sequences:
                    self.pending_acks.pop(node, None)
            if entries:
                self._queue(
                    Frame(ACK, BROADCAST, run=self.run_id, payload=b"".join(entries)),
                    "8节点批量确认",
                    now,
                )
        due = []
        remaining = []
        for item in self.downlinks:
            when, frame, purpose, control_only = item
            context = self.commands.get(self.command_context.get(id(frame)), {})
            if context.get("action") == "self_test" and context.get("state") in ("failed", "timeout", "gateway_rejected"):
                self.command_context.pop(id(frame), None)
                continue
            in_window = phase is None or 4_000_000 <= phase < 4_250_000
            if when <= now and (not control_only or in_window):
                due.append(item)
            else:
                remaining.append(item)
        self.downlinks = remaining
        # Keep radio transmissions apart; the gateway has one transmit path.
        if due and now >= self.tx_ready:
            item = due.pop(0)
            try:
                self._send(item[1], item[2])
            except (RuntimeError, OSError) as exc:
                self._command(self.command_context.pop(id(item[1]), None), "failed", error=str(exc))
                self.emit("log", str(exc))
            delay = airtime_seconds(len(item[1].encode())) + 0.03
            if item[1].kind == 0:
                delay = max(delay, 2.05)
            self.tx_ready = time.monotonic() + delay
            for i, (_, f, p, c) in enumerate(due):
                self.downlinks.append((now + delay * (i + 1), f, p, c))
        else:
            self.downlinks.extend(due)
