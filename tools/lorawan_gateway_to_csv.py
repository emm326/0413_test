#!/usr/bin/env python3
"""
Receive LoRaWAN gateway uplinks and write ADXL345 samples to per-device CSV files.

The expected application payload is the same binary vibration frame used by the
existing P2P firmware:
  dev_id, seq, t0_ms, fs_x10, count, int16 ax/ay/az samples, crc16

Supported inputs:
  - stdin JSON lines or raw base64/hex payloads
  - HTTP POST JSON bodies
  - TCP JSON lines
  - MQTT messages when paho-mqtt is installed
"""

from __future__ import annotations

import argparse
import base64
import binascii
import csv
import importlib
import json
import re
import socketserver
import struct
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable


FRAME_MAGIC = 0xA55A
FRAME_VERSION = 0x01
FRAME_TYPE_VIBRATION_RAW = 0x01
HEADER_STRUCT = struct.Struct("<HBBHHIHBB")
SAMPLE_STRUCT = struct.Struct("<hhh")
CRC_SIZE = 2
DEFAULT_SCALE_G = 0.0039
DEFAULT_ROTATE_MINUTES = 30
DEFAULT_OUTPUT = Path("data")
EXAMPLES_TEXT = r"""
LoRaWAN gateway to CSV examples

MQTT, typical ChirpStack-style integration:
  python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-topic "application/+/device/+/event/up" --output data

MQTT with username and password:
  python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-username your_user --mqtt-password your_password --mqtt-topic "application/+/device/+/event/up" --output data

HTTP POST receiver:
  python tools\lorawan_gateway_to_csv.py --http --http-port 8080 --http-path /uplink --output data

TCP line receiver:
  python tools\lorawan_gateway_to_csv.py --tcp-port 9000 --output data

Manual stdin smoke test:
  echo WqUBAekDBwBA4gEA6AMBAKIADwDXAGh8 | python tools\lorawan_gateway_to_csv.py --stdin --output data\lorawan_smoke --rotate-minutes 0 --base-time "2026/4/18 19:52:21.5126" --echo-data
"""


@dataclass
class Sample:
    ax_raw: int
    ay_raw: int
    az_raw: int


@dataclass
class VibrationPacket:
    dev_id: int
    seq: int
    t0_ms: int
    fs_x10: int
    samples: list[Sample]


@dataclass
class GatewayMeta:
    received_at: datetime
    rssi: float | None = None
    snr: float | None = None
    freq: float | None = None
    fcnt: int | None = None
    payload_len: int = 0


@dataclass
class ActiveCsvFile:
    path: Path
    fp: Any
    writer: csv.writer


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
        return (
            self.anchor_wallclock + timedelta(milliseconds=sensor_ms - self.anchor_sensor_ms),
            reanchored,
        )


def crc16_ccitt(data: bytes) -> int:
    crc = 0xFFFF
    for value in data:
        crc ^= value << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def decode_vibration_payload(payload: bytes) -> VibrationPacket:
    min_len = HEADER_STRUCT.size + SAMPLE_STRUCT.size + CRC_SIZE
    if len(payload) < min_len:
        raise ValueError("payload too short")

    magic, version, frame_type, dev_id, seq, t0_ms, fs_x10, count, _reserved = (
        HEADER_STRUCT.unpack_from(payload, 0)
    )
    if magic != FRAME_MAGIC:
        raise ValueError("bad frame magic")
    if version != FRAME_VERSION:
        raise ValueError("bad frame version")
    if frame_type != FRAME_TYPE_VIBRATION_RAW:
        raise ValueError("bad frame type")
    if count == 0:
        raise ValueError("empty sample packet")

    expected_len = HEADER_STRUCT.size + count * SAMPLE_STRUCT.size + CRC_SIZE
    if len(payload) != expected_len:
        raise ValueError(f"payload length mismatch: got {len(payload)}, expected {expected_len}")

    actual_crc = struct.unpack_from("<H", payload, expected_len - CRC_SIZE)[0]
    expected_crc = crc16_ccitt(payload[: expected_len - CRC_SIZE])
    if actual_crc != expected_crc:
        raise ValueError(
            f"crc mismatch: expected 0x{expected_crc:04X}, got 0x{actual_crc:04X}"
        )

    samples: list[Sample] = []
    offset = HEADER_STRUCT.size
    for _ in range(count):
        ax, ay, az = SAMPLE_STRUCT.unpack_from(payload, offset)
        samples.append(Sample(ax_raw=ax, ay_raw=ay, az_raw=az))
        offset += SAMPLE_STRUCT.size

    return VibrationPacket(
        dev_id=dev_id,
        seq=seq,
        t0_ms=t0_ms,
        fs_x10=fs_x10,
        samples=samples,
    )


def try_decode_payload_text(text: str) -> bytes | None:
    cleaned = text.strip()
    if not cleaned:
        return None

    if re.fullmatch(r"(?:0x)?[0-9a-fA-F][0-9a-fA-F\s,:-]*", cleaned):
        hex_text = re.sub(r"[^0-9a-fA-F]", "", cleaned)
        if len(hex_text) >= 2 and len(hex_text) % 2 == 0:
            try:
                return bytes.fromhex(hex_text)
            except ValueError:
                pass

    try:
        return base64.b64decode(cleaned, validate=True)
    except binascii.Error:
        return None


def iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from iter_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_dicts(child)


def iter_payload_candidates(value: Any) -> Iterable[bytes]:
    if isinstance(value, str):
        decoded = try_decode_payload_text(value)
        if decoded is not None:
            yield decoded
        return

    if isinstance(value, dict):
        preferred_keys = (
            "frm_payload",
            "data",
            "payload",
            "payload_raw",
            "payloadRaw",
            "bytes",
            "object",
        )
        for key in preferred_keys:
            if key in value:
                yield from iter_payload_candidates(value[key])
        for child in value.values():
            yield from iter_payload_candidates(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_payload_candidates(child)


def number_from_any(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def first_number_for_keys(value: Any, keys: set[str]) -> float | None:
    for item in iter_dicts(value):
        for key, child in item.items():
            if key.lower() in keys:
                found = number_from_any(child)
                if found is not None:
                    return found
    return None


def extract_gateway_meta(value: Any, payload_len: int) -> GatewayMeta:
    return GatewayMeta(
        received_at=datetime.now(),
        rssi=first_number_for_keys(value, {"rssi"}),
        snr=first_number_for_keys(value, {"snr", "lsnr"}),
        freq=first_number_for_keys(value, {"freq", "frequency"}),
        fcnt=(
            int(fcnt)
            if (fcnt := first_number_for_keys(value, {"fcnt", "f_cnt", "fcntup"})) is not None
            else None
        ),
        payload_len=payload_len,
    )


def parse_gateway_message(raw: bytes | str) -> tuple[VibrationPacket, GatewayMeta]:
    text = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else raw
    text = text.strip()
    if not text:
        raise ValueError("empty message")

    parsed: Any
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        payload = try_decode_payload_text(text)
        if payload is None:
            raise ValueError("message is not JSON, base64, or hex")
        packet = decode_vibration_payload(payload)
        return packet, GatewayMeta(received_at=datetime.now(), payload_len=len(payload))

    last_error: Exception | None = None
    seen: set[bytes] = set()
    for payload in iter_payload_candidates(parsed):
        if payload in seen:
            continue
        seen.add(payload)
        try:
            packet = decode_vibration_payload(payload)
            return packet, extract_gateway_meta(parsed, len(payload))
        except ValueError as exc:
            last_error = exc

    if last_error is not None:
        raise ValueError(f"no decodable vibration payload found: {last_error}") from last_error
    raise ValueError("no payload field found")


def parse_user_time(text: str) -> datetime:
    text = text.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise SystemExit("Invalid --base-time format. Example: 2026/4/18 19:52:21.5126")


def format_datetime_4(dt: datetime) -> str:
    fraction4 = dt.microsecond // 100
    return (
        f"{dt.year}/{dt.month}/{dt.day} "
        f"{dt.hour}:{dt.minute:02d}:{dt.second:02d}.{fraction4:04d}"
    )


def date_folder_name(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def file_time_name(dt: datetime) -> str:
    return dt.strftime("%H%M%S")


def floor_datetime(dt: datetime, minutes: int) -> datetime:
    minute_bucket = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute_bucket, second=0, microsecond=0)


def build_rotated_output_path(
    output_root: Path,
    device_id: int,
    sample_time: datetime,
    rotate_minutes: int,
) -> Path:
    bucket_start = floor_datetime(sample_time, rotate_minutes)
    bucket_end = bucket_start + timedelta(minutes=rotate_minutes) - timedelta(seconds=1)
    filename = f"{file_time_name(bucket_start)}_to_{file_time_name(bucket_end)}.csv"
    return output_root / f"sensor{device_id}" / date_folder_name(bucket_start) / filename


def sample_interval_ms(sample_index: int, fs_x10: int) -> int:
    return (sample_index * 10000 + fs_x10 // 2) // fs_x10


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class CsvWriterManager:
    def __init__(
        self,
        output: Path,
        rotate_minutes: int,
        scale: float,
        write_header: bool,
        include_id: bool,
    ) -> None:
        self.output = output
        self.rotate_minutes = rotate_minutes
        self.scale = scale
        self.write_header = write_header
        self.include_id = include_id
        self.active_files: dict[int, ActiveCsvFile] = {}
        self.lock = threading.Lock()

    def close(self) -> None:
        with self.lock:
            for active in self.active_files.values():
                active.fp.flush()
                active.fp.close()
            self.active_files.clear()

    def write_sample(self, device_id: int, sample_time: datetime, sample: Sample) -> None:
        row: list[str] = []
        if self.include_id:
            row.append(str(device_id))
        row.extend(
            [
                format_datetime_4(sample_time),
                f"{sample.ax_raw * self.scale:.6f}",
                f"{sample.ay_raw * self.scale:.6f}",
                f"{sample.az_raw * self.scale:.6f}",
            ]
        )

        with self.lock:
            active = self._active_file(device_id, sample_time)
            active.writer.writerow(row)
            active.fp.flush()

    def _active_file(self, device_id: int, sample_time: datetime) -> ActiveCsvFile:
        path = self._target_path(device_id, sample_time)
        active = self.active_files.get(device_id)
        if active is not None and active.path == path:
            return active

        if active is not None:
            active.fp.flush()
            active.fp.close()

        ensure_parent_dir(path)
        file_exists = path.exists()
        fp = path.open("a", newline="", encoding="utf-8")
        writer = csv.writer(fp)
        active = ActiveCsvFile(path=path, fp=fp, writer=writer)
        self.active_files[device_id] = active
        if self.write_header and (not file_exists or path.stat().st_size == 0):
            header = ["datetime", "ax", "ay", "az"]
            if self.include_id:
                header.insert(0, "id")
            writer.writerow(header)
            fp.flush()
        print(f"[info] Output file: {path}")
        return active

    def _target_path(self, device_id: int, sample_time: datetime) -> Path:
        if self.rotate_minutes > 0:
            return build_rotated_output_path(
                self.output, device_id, sample_time, self.rotate_minutes
            )
        return self.output / f"sensor{device_id}.csv"


class QualityWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        self.fp: Any | None = None
        self.writer: csv.writer | None = None
        self.last_seq: dict[int, int] = {}
        self.lock = threading.Lock()

    def close(self) -> None:
        with self.lock:
            if self.fp is not None:
                self.fp.flush()
                self.fp.close()
                self.fp = None
                self.writer = None

    def write_packet(self, packet: VibrationPacket, meta: GatewayMeta) -> None:
        if self.path is None:
            return

        with self.lock:
            if self.fp is None:
                ensure_parent_dir(self.path)
                file_exists = self.path.exists()
                self.fp = self.path.open("a", newline="", encoding="utf-8")
                self.writer = csv.writer(self.fp)
                if not file_exists or self.path.stat().st_size == 0:
                    self.writer.writerow(
                        [
                            "received_at",
                            "dev_id",
                            "seq",
                            "count",
                            "fs_hz",
                            "rssi",
                            "snr",
                            "freq",
                            "fcnt",
                            "lost_packets",
                            "payload_len",
                        ]
                    )

            lost = 0
            previous_seq = self.last_seq.get(packet.dev_id)
            if previous_seq is not None:
                lost = (packet.seq - previous_seq - 1) & 0xFFFF
            self.last_seq[packet.dev_id] = packet.seq

            assert self.writer is not None
            self.writer.writerow(
                [
                    format_datetime_4(meta.received_at),
                    packet.dev_id,
                    packet.seq,
                    len(packet.samples),
                    f"{packet.fs_x10 / 10.0:.1f}",
                    "" if meta.rssi is None else f"{meta.rssi:.1f}",
                    "" if meta.snr is None else f"{meta.snr:.1f}",
                    "" if meta.freq is None else f"{meta.freq:.6f}",
                    "" if meta.fcnt is None else meta.fcnt,
                    lost,
                    meta.payload_len,
                ]
            )
            self.fp.flush()


class PacketProcessor:
    def __init__(
        self,
        output: Path,
        rotate_minutes: int,
        scale: float,
        base_time: datetime | None,
        write_header: bool,
        include_id: bool,
        echo_data: bool,
        quality_log: Path | None,
    ) -> None:
        self.csv_writer = CsvWriterManager(
            output=output,
            rotate_minutes=rotate_minutes,
            scale=scale,
            write_header=write_header,
            include_id=include_id,
        )
        self.quality_writer = QualityWriter(quality_log)
        self.anchors: dict[int, TimeAnchor] = {}
        self.anchor_lock = threading.Lock()
        self.base_time = base_time
        self.echo_data = echo_data

    def close(self) -> None:
        self.csv_writer.close()
        self.quality_writer.close()

    def process_raw(self, raw: bytes | str) -> bool:
        try:
            packet, meta = parse_gateway_message(raw)
        except ValueError as exc:
            print(f"[warn] skipped message: {exc}")
            return False

        with self.anchor_lock:
            anchor = self.anchors.setdefault(packet.dev_id, TimeAnchor(self.base_time))
            t0_wallclock, reanchored = anchor.to_datetime(packet.t0_ms)

        if reanchored:
            print(
                f"[info] time anchor set for device {packet.dev_id}: "
                f"{format_datetime_4(t0_wallclock)}"
            )

        for index, sample in enumerate(packet.samples):
            sensor_ms = packet.t0_ms + sample_interval_ms(index, packet.fs_x10)
            sample_time, _ = self.anchors[packet.dev_id].to_datetime(sensor_ms)
            self.csv_writer.write_sample(packet.dev_id, sample_time, sample)
            if self.echo_data:
                print(
                    f"[{packet.dev_id}] {format_datetime_4(sample_time)},"
                    f"{sample.ax_raw * self.csv_writer.scale:.6f},"
                    f"{sample.ay_raw * self.csv_writer.scale:.6f},"
                    f"{sample.az_raw * self.csv_writer.scale:.6f}"
                )

        self.quality_writer.write_packet(packet, meta)
        print(
            f"[info] packet dev={packet.dev_id},seq={packet.seq},"
            f"count={len(packet.samples)},fs={packet.fs_x10 / 10.0:.1f}"
        )
        return True


def run_stdin(processor: PacketProcessor) -> int:
    print("[info] Reading gateway JSON/base64/hex messages from stdin")
    try:
        for line in sys.stdin:
            processor.process_raw(line)
    except KeyboardInterrupt:
        print("\n[info] stopped by user")
    finally:
        processor.close()
    return 0


def run_tcp(processor: PacketProcessor, host: str, port: int) -> int:
    class Handler(socketserver.StreamRequestHandler):
        def handle(self) -> None:
            peer = f"{self.client_address[0]}:{self.client_address[1]}"
            print(f"[info] TCP client connected: {peer}")
            for line in self.rfile:
                processor.process_raw(line)

    with socketserver.ThreadingTCPServer((host, port), Handler) as server:
        server.daemon_threads = True
        print(f"[info] TCP listening on {host}:{port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\n[info] stopped by user")
        finally:
            processor.close()
    return 0


def run_http(processor: PacketProcessor, host: str, port: int, path: str) -> int:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.path != path:
                self.send_response(404)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            ok = processor.process_raw(body)
            self.send_response(200 if ok else 400)
            self.end_headers()
            self.wfile.write(b"ok\n" if ok else b"bad payload\n")

        def log_message(self, _format: str, *args: Any) -> None:
            return

    with ThreadingHTTPServer((host, port), Handler) as server:
        print(f"[info] HTTP listening on http://{host}:{port}{path}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\n[info] stopped by user")
        finally:
            processor.close()
    return 0


def run_mqtt(
    processor: PacketProcessor,
    host: str,
    port: int,
    topic: str,
    username: str | None,
    password: str | None,
) -> int:
    try:
        mqtt = importlib.import_module("paho.mqtt.client")
    except ImportError as exc:
        raise SystemExit(
            "paho-mqtt is required for MQTT mode.\n"
            "Install it with: python -m pip install paho-mqtt\n"
            "HTTP and TCP modes do not need this dependency."
        ) from exc

    client = mqtt.Client()
    if username:
        client.username_pw_set(username, password)

    def on_connect(_client: Any, _userdata: Any, _flags: Any, rc: int) -> None:
        print(f"[info] MQTT connected rc={rc}, subscribing {topic}")
        client.subscribe(topic)

    def on_message(_client: Any, _userdata: Any, message: Any) -> None:
        processor.process_raw(message.payload)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[info] stopped by user")
    finally:
        client.disconnect()
        processor.close()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive LoRaWAN gateway uplinks and write per-device CSV files.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rotate-minutes", type=int, default=DEFAULT_ROTATE_MINUTES)
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE_G)
    parser.add_argument("--base-time", default="")
    parser.add_argument("--no-header", action="store_true")
    parser.add_argument("--include-id", action="store_true")
    parser.add_argument("--echo-data", action="store_true")
    parser.add_argument(
        "--quality-log",
        type=Path,
        default=None,
        help="Optional quality CSV path. Default: output/quality.csv",
    )
    parser.add_argument("--stdin", action="store_true", help="Read messages from stdin")
    parser.add_argument("--tcp-host", default="0.0.0.0")
    parser.add_argument("--tcp-port", type=int, default=0)
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--http-host", default="0.0.0.0")
    parser.add_argument("--http-port", type=int, default=8080)
    parser.add_argument("--http-path", default="/uplink")
    parser.add_argument("--mqtt-host", default="")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-topic", default="application/+/device/+/event/up")
    parser.add_argument("--mqtt-username", default="")
    parser.add_argument("--mqtt-password", default="")
    parser.add_argument(
        "--example",
        action="store_true",
        help="Print common MQTT/HTTP/TCP/stdin commands and exit",
    )
    return parser.parse_args()


def input_mode_description(args: argparse.Namespace) -> str:
    if args.mqtt_host:
        auth = "with auth" if args.mqtt_username else "without auth"
        return (
            f"MQTT host={args.mqtt_host}, port={args.mqtt_port}, "
            f"topic={args.mqtt_topic}, {auth}"
        )
    if args.http:
        return f"HTTP listen=http://{args.http_host}:{args.http_port}{args.http_path}"
    if args.tcp_port:
        return f"TCP listen={args.tcp_host}:{args.tcp_port}"
    return "stdin JSON/base64/hex lines"


def validate_input_mode(args: argparse.Namespace) -> None:
    selected = 0
    selected += 1 if args.mqtt_host else 0
    selected += 1 if args.http else 0
    selected += 1 if args.tcp_port else 0
    selected += 1 if args.stdin else 0
    if selected > 1:
        raise SystemExit("Select only one input mode: --mqtt-host, --http, --tcp-port, or --stdin")


def main() -> int:
    args = parse_args()
    if args.example:
        print(EXAMPLES_TEXT.strip())
        return 0

    validate_input_mode(args)
    if args.rotate_minutes < 0:
        raise SystemExit("--rotate-minutes must be 0 or greater")

    base_time = parse_user_time(args.base_time) if args.base_time else None
    quality_log = args.quality_log
    if quality_log is None:
        quality_log = args.output / "quality.csv"

    processor = PacketProcessor(
        output=args.output,
        rotate_minutes=args.rotate_minutes,
        scale=args.scale,
        base_time=base_time,
        write_header=not args.no_header,
        include_id=args.include_id,
        echo_data=args.echo_data,
        quality_log=quality_log,
    )

    print("[info] LoRaWAN gateway CSV receiver starting")
    print(f"[info] Input mode: {input_mode_description(args)}")
    print(f"[info] Output root: {args.output.resolve()}")
    print(f"[info] Rotate minutes: {args.rotate_minutes}")
    print(f"[info] Data scale: raw * {args.scale}")
    print(f"[info] CSV header: {'on' if not args.no_header else 'off'}")
    print(f"[info] Include device id in data CSV: {'on' if args.include_id else 'off'}")
    print(f"[info] Quality log: {quality_log.resolve() if quality_log is not None else 'off'}")
    print("[info] Use --example to print ready-to-copy commands")

    if args.mqtt_host:
        return run_mqtt(
            processor,
            args.mqtt_host,
            args.mqtt_port,
            args.mqtt_topic,
            args.mqtt_username or None,
            args.mqtt_password or None,
        )
    if args.http:
        return run_http(processor, args.http_host, args.http_port, args.http_path)
    if args.tcp_port:
        return run_tcp(processor, args.tcp_host, args.tcp_port)
    return run_stdin(processor)


if __name__ == "__main__":
    raise SystemExit(main())
