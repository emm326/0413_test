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
from collections import OrderedDict, deque
from dataclasses import dataclass
from .wire import (
    ACK,
    ACK_ENTRY,
    CONFIRM,
    DATA,
    DISCOVER,
    GPS_HEALTHY,
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


@dataclass
class Node:
    node: int
    state: int = 0
    flags: int = 0
    result: int = 0
    run: int = 0
    last_seen: float = 0
    clock_hz: float = 0
    clock_ppm: float = 0
    queued: int = 0
    packets: int = 0
    samples: int = 0
    rssi: float = 0
    snr: float = 0


class GatewayServer:
    def __init__(
        self, bind="0.0.0.0", port=1700, output="data/e870", nodes=(1001, 1002), emit=None
    ):
        self.bind, self.port = bind, port
        self.targets = tuple(int(n) for n in nodes)
        if (
            not self.targets
            or len(set(self.targets)) != len(self.targets)
            or not all(1001 <= n <= 1010 for n in self.targets)
        ):
            raise ValueError("use unique node ids 1001..1010")
        self.nodes = {n: Node(n) for n in self.targets}
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
        self.offsets = deque(maxlen=9)
        self.pending_acks = OrderedDict()
        self.downlinks = []
        self.pending_tokens = {}
        self.ack_cycle = -1
        self.tx_ready = 0
        self.awaiting_start = set()
        self.start_reported = False
        self.last_report = 0

    @property
    def gnss_now_us(self):
        return int(
            (time.time() + (statistics.median(self.offsets) if self.offsets else 0)) * 1_000_000
        )

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.bind, self.port))
        self.port = self.socket.getsockname()[1]
        self.socket.settimeout(0.05)
        self.thread = threading.Thread(target=self._loop, name="e870-receiver", daemon=True)
        self.thread.start()
        self.emit("log", f"监听UDP {self.bind}:{self.port}")

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=3)
        if self.socket:
            self.socket.close()
        self.recorder.close()

    def request(self, action, **options):
        self.requests.put((action, options))

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                raw, address = self.socket.recvfrom(65535)
                self.handle_datagram(raw, address)
            except socket.timeout:
                pass
            except (ValueError, KeyError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
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
                except (ValueError, RuntimeError) as exc:
                    self.emit("log", str(exc))
            self._schedule()

    def handle_datagram(self, raw, address):
        if len(raw) < 12 or raw[0] != 2:
            raise ValueError("expected Semtech UDP v2 gateway header")
        token = raw[1:3]
        kind = raw[3]
        eui = raw[4:12].hex()
        if self.gateway_eui and eui != self.gateway_eui:
            raise ValueError("this session already belongs to another gateway")
        self.gateway_eui = eui
        if kind == 2:
            self.downlink_peer = address
            self.peer_seen = time.monotonic()
            self.socket.sendto(bytes([2]) + token + bytes([4]), address)
            self.emit("gateway", {"eui": eui, "peer": address})
        elif kind == 0:
            self.socket.sendto(bytes([2]) + token + bytes([1]), address)
            body = json.loads(raw[12:].decode("utf-8"))
            for rx in body.get("rxpk", []):
                if rx.get("stat") != 1:
                    continue
                payload = base64.b64decode(rx["data"], validate=True)
                if rx.get("size", len(payload)) != len(payload):
                    raise ValueError("rxpk size mismatch")
                frame = Frame.decode(payload)
                if frame.node not in self.nodes:
                    continue
                self._frame(frame, rx)
        elif kind == 5:
            result = json.loads(raw[12:].decode("utf-8")) if len(raw) > 12 else {}
            error = result.get("txpk_ack", {}).get("error", "NONE")
            purpose = self.pending_tokens.pop(token.hex(), "downlink")
            self.emit("log", f"网关发送结果 {purpose}: {error}（节点确认另行显示）")
        else:
            raise ValueError("unsupported gateway message type")

    def _frame(self, frame, rx):
        node = self.nodes[frame.node]
        node.flags = frame.flags
        node.last_seen = time.monotonic()
        node.rssi = rx.get("rssi", 0)
        node.snr = rx.get("lsnr", 0)
        if frame.kind in (STATUS, CONFIRM):
            if len(frame.payload) != STATUS_PAYLOAD.size:
                raise ValueError("invalid status payload")
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
            if frame.kind == CONFIRM and frame.run == self.run_id and node.result == 0:
                self.awaiting_start.discard(frame.node)
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
            self.emit(
                "log",
                f"节点{frame.node}：{STATE_NAMES.get(node.state,node.state)}，{RESULT_NAMES.get(node.result,node.result)}，丢样{lost_samples}，丢包{lost_packets}",
            )
        elif frame.kind == DATA:
            if frame.run != self.run_id:
                self.emit("log", f"忽略非当前运行数据：节点{frame.node} run={frame.run}")
                return
            node.run = frame.run
            node.state = 3
            key = (frame.node, (frame.sequence // 32) * 32)
            self.pending_acks[key] = self.pending_acks.get(key, 0) | (1 << (frame.sequence % 32))
            rx = dict(rx, received_utc_us=self.gnss_now_us)
            if self.recorder.record(frame, rx):
                node.packets += 1
                node.samples += frame.count
        self.emit("node", dict(vars(node)))

    def _request(self, action, options):
        if action == "discover":
            self._queue(Frame(DISCOVER, BROADCAST), "查询节点")
        elif action == "start":
            targets = tuple(options.get("nodes", self.targets))
            missing = [
                n
                for n in targets
                if n not in self.nodes
                or self.nodes[n].state != 1
                or self.nodes[n].flags & 3 != 3
                or time.monotonic() - self.nodes[n].last_seen > 30
            ]
            if missing:
                raise ValueError(f"节点未就绪：{missing}；等待60秒有效PPS计数后重新查询")
            if not self.offsets:
                raise ValueError("尚未从节点状态取得GNSS时间")
            start_frame(
                1,
                1,
                options.get("rate", 100),
                options.get("range_g", 4),
                options.get("sf", 6),
                options.get("duration", 0),
                node_count=len(targets),
            )
            if not self.downlink_peer or time.monotonic() - self.peer_seen > 30:
                raise RuntimeError("网关下行未连接，不能发起采集")
            self.run_id = secrets.randbits(32) or 1
            self.start_us = ((self.gnss_now_us + 10_000_000 + 999999) // 1_000_000) * 1_000_000
            self.pending_acks.clear()
            self.ack_cycle = -1
            self.awaiting_start = set(targets)
            self.start_reported = False
            mask = sum(1 << (n - 1001) for n in targets)
            for repeat in range(3):
                f = start_frame(
                    self.run_id,
                    self.start_us,
                    options.get("rate", 100),
                    options.get("range_g", 4),
                    options.get("sf", 6),
                    options.get("duration", 0),
                    node_count=len(targets),
                    node_mask=mask,
                )
                self._queue(f, "START广播", time.monotonic() + repeat * 2.5)
            self.emit("log", f"开始请求 run={self.run_id}，UTC启动={self.start_us}，等待各节点确认")
        elif action == "stop":
            self.downlinks = [item for item in self.downlinks if item[1].kind != START]
            self.awaiting_start.clear()
            self._queue(
                Frame(STOP, BROADCAST, run=self.run_id), "STOP", control_only=bool(self.start_us)
            )
        else:
            raise ValueError("unknown host action")

    def _queue(self, frame, purpose, when=None, control_only=False):
        self.downlinks.append(
            (time.monotonic() if when is None else when, frame, purpose, control_only)
        )

    def _send(self, frame, purpose):
        if not self.downlink_peer or time.monotonic() - self.peer_seen > 30:
            raise RuntimeError("尚未建立网关下行通道，请核对服务器下行端口1700")
        payload = frame.encode()
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
        self.pending_tokens[token.hex()] = purpose
        if len(self.pending_tokens) > 256:
            self.pending_tokens.pop(next(iter(self.pending_tokens)))

    def _schedule(self):
        now = time.monotonic()
        phase = None
        cycle = -1
        if now - self.last_report > 10:
            self.recorder.save_report()
            self.last_report = now
        if self.start_us and not self.start_reported and self.gnss_now_us >= self.start_us:
            self.start_reported = True
            self.emit("log", f"到达启动时间，未确认节点：{sorted(self.awaiting_start)}")
        if self.start_us and self.gnss_now_us >= self.start_us:
            elapsed = self.gnss_now_us - self.start_us
            phase = elapsed % 5_000_000
            cycle = elapsed // 5_000_000
        if phase is not None and 4_030_000 <= phase < 4_100_000 and self.ack_cycle != cycle:
            self.ack_cycle = cycle
            for i in range(2):
                entries = []
                while self.pending_acks and len(entries) < 10:
                    (node, first), bitmap = self.pending_acks.popitem(last=False)
                    entries.append(ACK_ENTRY.pack(node, first, bitmap))
                if entries:
                    self._queue(
                        Frame(ACK, BROADCAST, run=self.run_id, payload=b"".join(entries)),
                        "批量确认",
                        now + i * 0.15,
                    )
        due = []
        remaining = []
        for item in self.downlinks:
            when, frame, purpose, control_only = item
            in_window = phase is None or 4_000_000 <= phase < 4_270_000
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
            except RuntimeError as exc:
                self.emit("log", str(exc))
            delay = airtime_seconds(len(item[1].encode())) + 0.03
            self.tx_ready = now + delay
            for i, (_, f, p, c) in enumerate(due):
                self.downlinks.append((now + delay * (i + 1), f, p, c))
        else:
            self.downlinks.extend(due)
