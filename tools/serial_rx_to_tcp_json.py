#!/usr/bin/env python3
"""
Read LoRa RX serial output and upload one JSON record per sample over TCP.

Supported serial data formats:
1. Multi-device RX output:
   dev_id,timestamp_ms,ax_raw,ay_raw,az_raw
2. Legacy single-device RX output:
   timestamp_ms,ax_raw,ay_raw,az_raw

TCP payload format:
{"id":"1001","time":"2026/4/18 19:52:21.5026","x":0.631800,"y":0.058500,"z":0.830700}
"""

from __future__ import annotations

import argparse
import json
import queue
import re
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

try:
    import serial
    from serial import SerialException
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pyserial is required.\n"
        r"Try: C:\Users\26223\.platformio\penv\Scripts\python.exe "
        r"tools\serial_rx_to_tcp_json.py --port COM6 --host 1.2.3.4 --server-port 9000"
    ) from exc


MULTI_DEVICE_RE = re.compile(r"^\s*(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
LEGACY_RE = re.compile(r"^\s*(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
DEFAULT_SCALE_G = 0.0039
DEFAULT_DEVICE_ID = "1001"


@dataclass
class UploadConfig:
    serial_port: str
    serial_baud: int
    host: str
    server_port: int
    device_id: str
    scale: float
    base_time: datetime | None
    connect_timeout: float
    reconnect_interval: float
    serial_retry_interval: float
    queue_size: int
    echo_json: bool
    log_serial: bool


@dataclass
class ParsedSample:
    device_id: str
    sensor_ms: int
    ax_raw: int
    ay_raw: int
    az_raw: int


@dataclass
class TimeAnchor:
    base_time: datetime | None = None
    anchor_wallclock: datetime | None = None
    anchor_sensor_ms: int | None = None
    last_sensor_ms: int | None = None

    def to_datetime(self, sensor_ms: int) -> tuple[datetime, bool]:
        reanchored = False
        if self.anchor_sensor_ms is None:
            self.anchor_sensor_ms = sensor_ms
            self.anchor_wallclock = self.base_time or datetime.now()
            reanchored = True
        elif self.last_sensor_ms is not None and sensor_ms < self.last_sensor_ms:
            self.anchor_sensor_ms = sensor_ms
            self.anchor_wallclock = datetime.now()
            reanchored = True

        self.last_sensor_ms = sensor_ms
        assert self.anchor_wallclock is not None
        assert self.anchor_sensor_ms is not None
        delta_ms = sensor_ms - self.anchor_sensor_ms
        return self.anchor_wallclock + timedelta(milliseconds=delta_ms), reanchored


class TcpJsonSender:
    def __init__(self, config: UploadConfig):
        self.config = config
        self.sock: socket.socket | None = None
        self.lock = threading.Lock()

    def close(self) -> None:
        with self.lock:
            if self.sock is None:
                return
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def ensure_connected(self, stop_event: threading.Event) -> bool:
        while not stop_event.is_set():
            with self.lock:
                if self.sock is not None:
                    return True

            try:
                sock = socket.create_connection(
                    (self.config.host, self.config.server_port),
                    timeout=self.config.connect_timeout,
                )
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with self.lock:
                    self.sock = sock
                print(
                    f"[info] TCP connected to {self.config.host}:{self.config.server_port}"
                )
                return True
            except OSError as exc:
                print(f"[warn] TCP connect failed: {exc}")
                if stop_event.wait(self.config.reconnect_interval):
                    return False

        return False

    def send_line(self, payload: str, stop_event: threading.Event) -> bool:
        while not stop_event.is_set():
            if not self.ensure_connected(stop_event):
                return False

            try:
                assert self.sock is not None
                self.sock.sendall((payload + "\n").encode("utf-8"))
                return True
            except OSError as exc:
                print(f"[warn] TCP send failed: {exc}")
                self.close()
                if stop_event.wait(self.config.reconnect_interval):
                    return False

        return False


def parse_user_time(text: str) -> datetime:
    text = text.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise SystemExit(
        "Invalid --base-time format. Example: 2026/4/18 19:52:21.5026"
    )


def format_datetime_4(dt: datetime) -> str:
    fraction4 = dt.microsecond // 100
    return (
        f"{dt.year}/{dt.month}/{dt.day} "
        f"{dt.hour}:{dt.minute:02d}:{dt.second:02d}.{fraction4:04d}"
    )


def build_json_line(
    device_id: str,
    time_text: str,
    x_value: float,
    y_value: float,
    z_value: float,
) -> str:
    return (
        "{"
        f"\"id\":{json.dumps(str(device_id), ensure_ascii=False)},"
        f"\"time\":{json.dumps(time_text, ensure_ascii=False)},"
        f"\"x\":{x_value:.6f},"
        f"\"y\":{y_value:.6f},"
        f"\"z\":{z_value:.6f}"
        "}"
    )


def parse_serial_sample(line: str, fallback_device_id: str) -> ParsedSample | None:
    match = MULTI_DEVICE_RE.match(line)
    if match:
        return ParsedSample(
            device_id=match.group(1),
            sensor_ms=int(match.group(2)),
            ax_raw=int(match.group(3)),
            ay_raw=int(match.group(4)),
            az_raw=int(match.group(5)),
        )

    match = LEGACY_RE.match(line)
    if match:
        return ParsedSample(
            device_id=fallback_device_id,
            sensor_ms=int(match.group(1)),
            ax_raw=int(match.group(2)),
            ay_raw=int(match.group(3)),
            az_raw=int(match.group(4)),
        )

    return None


def parse_args() -> UploadConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Read RX serial samples and upload one JSON record per sample to a TCP "
            "server."
        )
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM6")
    parser.add_argument(
        "--baud", type=int, default=115200, help="Serial baud rate, default 115200"
    )
    parser.add_argument("--host", required=True, help="Remote TCP server host")
    parser.add_argument(
        "--server-port", type=int, required=True, help="Remote TCP server port"
    )
    parser.add_argument(
        "--device-id",
        default=DEFAULT_DEVICE_ID,
        help=(
            "Fallback device id used only for legacy 4-field serial lines. "
            f"Default: {DEFAULT_DEVICE_ID}"
        ),
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=DEFAULT_SCALE_G,
        help="Raw to g scale factor. Default: 0.0039",
    )
    parser.add_argument(
        "--base-time",
        default="",
        help="Optional anchor time. Example: 2026/4/18 19:52:21.5026",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=5.0,
        help="TCP connect timeout in seconds. Default: 5",
    )
    parser.add_argument(
        "--reconnect-interval",
        type=float,
        default=10.0,
        help="TCP reconnect interval in seconds. Default: 10",
    )
    parser.add_argument(
        "--serial-retry-interval",
        type=float,
        default=5.0,
        help="Serial reopen interval in seconds. Default: 5",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=2048,
        help="In-memory payload queue size. Default: 2048",
    )
    parser.add_argument(
        "--echo-json",
        action="store_true",
        help="Print each successfully sent JSON line",
    )
    parser.add_argument(
        "--log-serial",
        action="store_true",
        help="Print non-data serial lines for troubleshooting",
    )
    args = parser.parse_args()

    return UploadConfig(
        serial_port=args.port,
        serial_baud=args.baud,
        host=args.host,
        server_port=args.server_port,
        device_id=str(args.device_id),
        scale=args.scale,
        base_time=parse_user_time(args.base_time) if args.base_time else None,
        connect_timeout=args.connect_timeout,
        reconnect_interval=args.reconnect_interval,
        serial_retry_interval=args.serial_retry_interval,
        queue_size=args.queue_size,
        echo_json=args.echo_json,
        log_serial=args.log_serial,
    )


def serial_reader(
    config: UploadConfig,
    payload_queue: queue.Queue[str],
    stop_event: threading.Event,
) -> None:
    anchors: dict[str, TimeAnchor] = {}
    dropped_count = 0

    while not stop_event.is_set():
        try:
            ser = serial.Serial(config.serial_port, config.serial_baud, timeout=1)
            print(f"[info] Serial connected on {config.serial_port} @ {config.serial_baud}")
        except SerialException as exc:
            print(f"[warn] Serial open failed: {exc}")
            stop_event.wait(config.serial_retry_interval)
            continue

        with ser:
            try:
                while not stop_event.is_set():
                    raw_line = ser.readline()
                    if not raw_line:
                        continue

                    line = raw_line.decode("utf-8", errors="ignore").strip()
                    sample = parse_serial_sample(line, config.device_id)
                    if sample is None:
                        if config.log_serial and line:
                            print(f"[serial] {line}")
                        continue

                    anchor = anchors.setdefault(
                        sample.device_id, TimeAnchor(base_time=config.base_time)
                    )
                    wallclock, reanchored = anchor.to_datetime(sample.sensor_ms)
                    if reanchored:
                        print(
                            "[info] Time anchor set for device "
                            f"{sample.device_id}: sensor_ms={sample.sensor_ms}, "
                            f"wallclock={format_datetime_4(wallclock)}"
                        )

                    json_line = build_json_line(
                        sample.device_id,
                        format_datetime_4(wallclock),
                        sample.ax_raw * config.scale,
                        sample.ay_raw * config.scale,
                        sample.az_raw * config.scale,
                    )

                    try:
                        payload_queue.put_nowait(json_line)
                    except queue.Full:
                        dropped_count += 1
                        if dropped_count == 1 or dropped_count % 100 == 0:
                            print(
                                f"[warn] Payload queue full, dropped {dropped_count} samples"
                            )
            except SerialException as exc:
                print(f"[warn] Serial read failed: {exc}")
                stop_event.wait(config.serial_retry_interval)


def tcp_sender(
    config: UploadConfig,
    payload_queue: queue.Queue[str],
    stop_event: threading.Event,
) -> None:
    sender = TcpJsonSender(config)

    try:
        while not stop_event.is_set():
            try:
                payload = payload_queue.get(timeout=1)
            except queue.Empty:
                continue

            sent = sender.send_line(payload, stop_event)
            if not sent and stop_event.is_set():
                break

            if config.echo_json and sent:
                print(payload)
    finally:
        sender.close()


def main() -> int:
    config = parse_args()
    payload_queue: queue.Queue[str] = queue.Queue(maxsize=config.queue_size)
    stop_event = threading.Event()

    print("[info] Starting serial -> TCP JSON uploader")
    print(f"[info] Serial: {config.serial_port} @ {config.serial_baud}")
    print(f"[info] Server: {config.host}:{config.server_port}")
    print(f"[info] Legacy fallback device id: {config.device_id}")
    print(f"[info] Scale: {config.scale}")
    if config.base_time is not None:
        print(
            "[info] Each device will use the provided base time as its first anchor: "
            f"{format_datetime_4(config.base_time)}"
        )

    serial_thread = threading.Thread(
        target=serial_reader,
        name="serial-reader",
        args=(config, payload_queue, stop_event),
        daemon=True,
    )
    tcp_thread = threading.Thread(
        target=tcp_sender,
        name="tcp-sender",
        args=(config, payload_queue, stop_event),
        daemon=True,
    )

    serial_thread.start()
    tcp_thread.start()

    try:
        while serial_thread.is_alive() and tcp_thread.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[info] Stopping uploader")
        stop_event.set()

    serial_thread.join(timeout=2)
    tcp_thread.join(timeout=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
