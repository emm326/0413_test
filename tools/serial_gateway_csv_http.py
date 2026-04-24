#!/usr/bin/env python3
r"""
Multi-port LoRa RX gateway:
- read one or more RX serial ports
- route samples by transmitter dev_id
- write rotated CSV files per device
- upload completed CSV files to a remote HTTP server

Supported serial line formats:
1. Multi-device RX output:
   dev_id,timestamp_ms,ax_raw,ay_raw,az_raw
2. Legacy single-device RX output:
   timestamp_ms,ax_raw,ay_raw,az_raw

Example:
  C:\Users\26223\.platformio\penv\Scripts\python.exe ^
    tools\serial_gateway_csv_http.py ^
    --ports COM6,COM7 ^
    --gateway-ids rx01,rx02 ^
    --output-root data ^
    --rotation-minutes 30 ^
    --upload-url http://127.0.0.1:8000/upload
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import mimetypes
import queue
import re
import shutil
import threading
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import TextIO
from urllib import error, request

try:
    import serial
    from serial import SerialException
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pyserial is required.\n"
        r"Try: C:\Users\26223\.platformio\penv\Scripts\python.exe "
        r"tools\serial_gateway_csv_http.py --ports COM6 --gateway-ids rx01 "
        r"--output-root data --rotation-minutes 30 "
        r"--upload-url http://127.0.0.1:8000/upload"
    ) from exc


MULTI_DEVICE_RE = re.compile(r"^\s*(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
LEGACY_RE = re.compile(r"^\s*(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
FILE_NAME_RE = re.compile(
    r"^(?P<device_id>\d+)_(?P<start>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_to_"
    r"(?P<end>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})(?:_seg\d+)?$"
)

DEFAULT_SCALE_G = 0.0039
DEFAULT_DEVICE_ID = "1001"
DEFAULT_ROTATION_MINUTES = 30
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_RETRY_INTERVAL = 10.0
DEFAULT_SCAN_INTERVAL = 10.0
DEFAULT_STATUS_INTERVAL = 5.0
DEFAULT_SAMPLE_QUEUE_SIZE = 8192
DEFAULT_MULTIPART_FIELD = "file"
DEFAULT_FLUSH_EVERY_ROWS = 20

LOGGER = logging.getLogger("serial-gateway-csv-http")


@dataclass(frozen=True)
class SerialSource:
    port: str
    gateway_id: str


@dataclass
class GatewayConfig:
    sources: list[SerialSource]
    serial_baud: int
    output_root: Path
    rotation_minutes: int
    upload_url: str
    scale: float
    base_time: datetime | None
    connect_timeout: float
    retry_interval: float
    scan_interval: float
    status_interval: float
    sample_queue_size: int
    multipart_field: str
    log_level: str
    echo_samples: bool
    log_serial: bool
    legacy_device_id: str
    device_map: dict[str, str]


@dataclass
class ParsedSample:
    device_id: str
    sensor_ms: int
    ax_raw: int
    ay_raw: int
    az_raw: int


@dataclass
class SampleEvent:
    device_id: str
    sensor_ms: int
    ax_raw: int
    ay_raw: int
    az_raw: int
    gateway_id: str
    port: str


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


@dataclass
class SegmentMetadata:
    device_id: str
    gateway_ids: list[str]
    serial_ports: list[str]
    bucket_start: str
    bucket_end: str
    first_sample_at: str
    last_sample_at: str
    rows: int
    created_at: str
    upload_attempts: int = 0
    last_error: str = ""


@dataclass
class ActiveSegment:
    csv_final_path: Path
    csv_part_path: Path
    meta_final_path: Path
    meta_part_path: Path
    bucket_start: datetime
    bucket_end: datetime
    first_sample_at: datetime
    last_sample_at: datetime
    gateway_ids: set[str]
    serial_ports: set[str]
    fp: TextIO
    writer: csv.writer
    rows: int = 0
    rows_since_flush: int = 0


@dataclass
class DeviceSession:
    anchor: TimeAnchor
    active_segment: ActiveSegment | None = None


@dataclass
class UploadTask:
    csv_path: Path
    meta_path: Path
    metadata: SegmentMetadata


class GatewayStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total_samples = 0
        self.dropped_samples = 0
        self.uploaded_files = 0
        self.upload_failures = 0
        self.serial_errors = 0
        self.non_data_lines = 0
        self.active_devices = 0
        self.samples_by_device: Counter[str] = Counter()
        self.samples_by_gateway: Counter[str] = Counter()
        self.last_upload_status = "idle"

    def record_sample(self, gateway_id: str, device_id: str) -> None:
        with self._lock:
            self.total_samples += 1
            self.samples_by_gateway[gateway_id] += 1
            self.samples_by_device[device_id] += 1

    def record_drop(self) -> None:
        with self._lock:
            self.dropped_samples += 1

    def record_upload_success(self, task: UploadTask) -> None:
        with self._lock:
            self.uploaded_files += 1
            self.last_upload_status = (
                f"uploaded {task.csv_path.name} rows={task.metadata.rows}"
            )

    def record_upload_failure(self, task: UploadTask, message: str) -> None:
        with self._lock:
            self.upload_failures += 1
            self.last_upload_status = f"upload failed {task.csv_path.name}: {message}"

    def record_serial_error(self, gateway_id: str, message: str) -> None:
        with self._lock:
            self.serial_errors += 1
            self.last_upload_status = f"serial {gateway_id}: {message}"

    def record_non_data_line(self) -> None:
        with self._lock:
            self.non_data_lines += 1

    def set_active_devices(self, count: int) -> None:
        with self._lock:
            self.active_devices = count

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "total_samples": self.total_samples,
                "dropped_samples": self.dropped_samples,
                "uploaded_files": self.uploaded_files,
                "upload_failures": self.upload_failures,
                "serial_errors": self.serial_errors,
                "non_data_lines": self.non_data_lines,
                "active_devices": self.active_devices,
                "samples_by_device": dict(self.samples_by_device),
                "samples_by_gateway": dict(self.samples_by_gateway),
                "last_upload_status": self.last_upload_status,
            }


def parse_args() -> GatewayConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Read multiple LoRa RX serial ports, package samples by transmitter "
            "device id into rotated CSV files, then upload the closed CSV files "
            "to a remote HTTP server."
        )
    )
    parser.add_argument(
        "--ports",
        required=True,
        help="Comma-separated serial ports, for example COM6,COM7,COM8",
    )
    parser.add_argument(
        "--gateway-ids",
        default="",
        help=(
            "Comma-separated gateway ids matching --ports, for example "
            "rx01,rx02,rx03. If omitted, ids will be generated automatically."
        ),
    )
    parser.add_argument(
        "--baud",
        type=int,
        default=115200,
        help="Serial baud rate for all RX ports. Default: 115200",
    )
    parser.add_argument(
        "--output-root",
        required=True,
        help="Root directory for CSV files, for example data",
    )
    parser.add_argument(
        "--rotation-minutes",
        type=int,
        default=DEFAULT_ROTATION_MINUTES,
        help=f"CSV rotation window in minutes. Default: {DEFAULT_ROTATION_MINUTES}",
    )
    parser.add_argument(
        "--upload-url",
        required=True,
        help="Remote HTTP upload URL",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=DEFAULT_SCALE_G,
        help=f"Raw to g scale factor. Default: {DEFAULT_SCALE_G}",
    )
    parser.add_argument(
        "--base-time",
        default="",
        help="Optional wall-clock anchor, example: 2026/4/18 19:52:21.5026",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=DEFAULT_CONNECT_TIMEOUT,
        help=f"HTTP connect/read timeout in seconds. Default: {DEFAULT_CONNECT_TIMEOUT}",
    )
    parser.add_argument(
        "--retry-interval",
        type=float,
        default=DEFAULT_RETRY_INTERVAL,
        help=f"Retry wait in seconds after upload or serial failure. Default: {DEFAULT_RETRY_INTERVAL}",
    )
    parser.add_argument(
        "--scan-interval",
        type=float,
        default=DEFAULT_SCAN_INTERVAL,
        help=f"Pending file scan interval in seconds. Default: {DEFAULT_SCAN_INTERVAL}",
    )
    parser.add_argument(
        "--status-interval",
        type=float,
        default=DEFAULT_STATUS_INTERVAL,
        help=f"Status print interval in seconds. Default: {DEFAULT_STATUS_INTERVAL}",
    )
    parser.add_argument(
        "--sample-queue-size",
        type=int,
        default=DEFAULT_SAMPLE_QUEUE_SIZE,
        help=f"Sample event queue size. Default: {DEFAULT_SAMPLE_QUEUE_SIZE}",
    )
    parser.add_argument(
        "--multipart-field",
        default=DEFAULT_MULTIPART_FIELD,
        help=f"Multipart field name for the CSV file. Default: {DEFAULT_MULTIPART_FIELD}",
    )
    parser.add_argument(
        "--legacy-device-id",
        default=DEFAULT_DEVICE_ID,
        help=(
            "Fallback device id for legacy 4-field serial lines. "
            f"Default: {DEFAULT_DEVICE_ID}"
        ),
    )
    parser.add_argument(
        "--device-map",
        default="",
        help=(
            "Optional CSV or JSON file mapping transmitter device ids to gateway ids. "
            "Mismatch will be logged as a warning."
        ),
    )
    parser.add_argument(
        "--echo-samples",
        action="store_true",
        help="Print each parsed sample to the console",
    )
    parser.add_argument(
        "--log-serial",
        action="store_true",
        help="Print non-data serial lines for troubleshooting",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level. Default: INFO",
    )
    args = parser.parse_args()

    ports = [item.strip() for item in args.ports.split(",") if item.strip()]
    if not ports:
        raise SystemExit("At least one serial port is required via --ports")

    if args.gateway_ids:
        gateway_ids = [
            item.strip() for item in args.gateway_ids.split(",") if item.strip()
        ]
        if len(gateway_ids) != len(ports):
            raise SystemExit(
                "--gateway-ids count must match --ports count when provided"
            )
    else:
        gateway_ids = [f"rx{i + 1:02d}" for i in range(len(ports))]

    if args.rotation_minutes <= 0:
        raise SystemExit("--rotation-minutes must be greater than 0")

    if args.sample_queue_size <= 0:
        raise SystemExit("--sample-queue-size must be greater than 0")

    config = GatewayConfig(
        sources=[
            SerialSource(port=port, gateway_id=gateway_id)
            for port, gateway_id in zip(ports, gateway_ids, strict=True)
        ],
        serial_baud=args.baud,
        output_root=Path(args.output_root),
        rotation_minutes=args.rotation_minutes,
        upload_url=args.upload_url,
        scale=args.scale,
        base_time=parse_user_time(args.base_time) if args.base_time else None,
        connect_timeout=args.connect_timeout,
        retry_interval=args.retry_interval,
        scan_interval=args.scan_interval,
        status_interval=args.status_interval,
        sample_queue_size=args.sample_queue_size,
        multipart_field=args.multipart_field,
        log_level=args.log_level,
        echo_samples=args.echo_samples,
        log_serial=args.log_serial,
        legacy_device_id=str(args.legacy_device_id),
        device_map=load_device_map(Path(args.device_map)) if args.device_map else {},
    )
    return config


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="[%(levelname)s] %(message)s",
    )


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


def load_device_map(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"Device map file not found: {path}")

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise SystemExit("JSON device map must be an object: {\"1001\": \"rx01\"}")
        return {str(key): str(value) for key, value in data.items()}

    mapping: dict[str, str] = {}
    with path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        for row in reader:
            if not row:
                continue
            if row[0].strip().startswith("#"):
                continue
            if len(row) < 2:
                raise SystemExit(
                    "CSV device map rows must contain at least device_id,gateway_id"
                )
            mapping[str(row[0]).strip()] = str(row[1]).strip()
    return mapping


def format_datetime_4(dt: datetime) -> str:
    fraction4 = dt.microsecond // 100
    return (
        f"{dt.year}/{dt.month}/{dt.day} "
        f"{dt.hour}:{dt.minute:02d}:{dt.second:02d}.{fraction4:04d}"
    )


def format_file_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d_%H-%M-%S")


def floor_datetime(dt: datetime, minutes: int) -> datetime:
    minute_bucket = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute_bucket, second=0, microsecond=0)


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


def count_pending_csv_files(output_root: Path) -> int:
    if not output_root.exists():
        return 0
    return sum(1 for _ in output_root.rglob("pending/*.csv"))


def ensure_device_dirs(output_root: Path, device_id: str) -> dict[str, Path]:
    base = output_root / device_id
    dirs = {
        "pending": base / "pending",
        "uploaded": base / "uploaded",
        "failed": base / "failed",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def final_meta_path_for(csv_path: Path) -> Path:
    return csv_path.with_suffix(".meta.json")


def part_meta_path_for(csv_part_path: Path) -> Path:
    final_csv_path = csv_part_path.with_suffix("")
    return Path(str(final_meta_path_for(final_csv_path)) + ".part")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_segment_file_name(device_id: str, bucket_start: datetime, bucket_end: datetime) -> str:
    return (
        f"{device_id}_{format_file_timestamp(bucket_start)}_to_"
        f"{format_file_timestamp(bucket_end)}.csv"
    )


def infer_metadata_from_csv(csv_path: Path) -> SegmentMetadata | None:
    match = FILE_NAME_RE.match(csv_path.stem)
    if not match:
        return None

    row_count = 0
    with csv_path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        next(reader, None)
        for _ in reader:
            row_count += 1

    return SegmentMetadata(
        device_id=match.group("device_id"),
        gateway_ids=[],
        serial_ports=[],
        bucket_start=match.group("start"),
        bucket_end=match.group("end"),
        first_sample_at="",
        last_sample_at="",
        rows=row_count,
        created_at=format_datetime_4(datetime.fromtimestamp(csv_path.stat().st_mtime)),
    )


def recover_pending_part_files(output_root: Path) -> None:
    if not output_root.exists():
        return

    for csv_part_path in output_root.rglob("pending/*.csv.part"):
        final_csv_path = csv_part_path.with_suffix("")
        meta_part_path = part_meta_path_for(csv_part_path)
        final_meta_path = final_meta_path_for(final_csv_path)

        if final_csv_path.exists():
            continue

        LOGGER.warning("Recovering partial CSV segment: %s", csv_part_path)
        csv_part_path.rename(final_csv_path)

        if meta_part_path.exists():
            meta_part_path.rename(final_meta_path)
            continue

        inferred = infer_metadata_from_csv(final_csv_path)
        if inferred is None:
            failed_dir = final_csv_path.parent.parent / "failed"
            failed_dir.mkdir(parents=True, exist_ok=True)
            failed_path = failed_dir / final_csv_path.name
            shutil.move(str(final_csv_path), failed_path)
            LOGGER.error("Moved unrecoverable partial file to failed: %s", failed_path)
            continue

        write_json(final_meta_path, asdict(inferred))


def load_upload_task(csv_path: Path) -> UploadTask | None:
    meta_path = final_meta_path_for(csv_path)
    metadata: SegmentMetadata | None = None

    if meta_path.exists():
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
            metadata = SegmentMetadata(**payload)
        except (OSError, ValueError, TypeError) as exc:
            LOGGER.error("Failed to read metadata for %s: %s", csv_path, exc)

    if metadata is None:
        metadata = infer_metadata_from_csv(csv_path)
        if metadata is None:
            LOGGER.error("Cannot infer metadata from file name: %s", csv_path)
            return None
        write_json(meta_path, asdict(metadata))

    return UploadTask(csv_path=csv_path, meta_path=meta_path, metadata=metadata)


class CsvSegmentManager:
    def __init__(self, config: GatewayConfig, stats: GatewayStats):
        self.config = config
        self.stats = stats
        self.sessions: dict[str, DeviceSession] = {}

    def close_all(self) -> None:
        for device_id in list(self.sessions.keys()):
            session = self.sessions[device_id]
            if session.active_segment is not None:
                self._finalize_segment(device_id, session.active_segment)
                session.active_segment = None
        self.stats.set_active_devices(0)

    def process_sample(self, sample: SampleEvent) -> None:
        session = self.sessions.setdefault(
            sample.device_id, DeviceSession(anchor=TimeAnchor(base_time=self.config.base_time))
        )

        wallclock, reanchored = session.anchor.to_datetime(sample.sensor_ms)
        if reanchored:
            LOGGER.info(
                "Time anchor set for device %s via %s (%s): sensor_ms=%s, wallclock=%s",
                sample.device_id,
                sample.gateway_id,
                sample.port,
                sample.sensor_ms,
                format_datetime_4(wallclock),
            )

        expected_gateway = self.config.device_map.get(sample.device_id)
        if expected_gateway and expected_gateway != sample.gateway_id:
            LOGGER.warning(
                "Device map mismatch: device %s expected %s but received from %s",
                sample.device_id,
                expected_gateway,
                sample.gateway_id,
            )

        bucket_start = floor_datetime(wallclock, self.config.rotation_minutes)
        bucket_end = bucket_start + timedelta(minutes=self.config.rotation_minutes) - timedelta(
            seconds=1
        )

        if (
            session.active_segment is None
            or session.active_segment.bucket_start != bucket_start
        ):
            if session.active_segment is not None:
                self._finalize_segment(sample.device_id, session.active_segment)
            session.active_segment = self._open_segment(
                sample.device_id,
                bucket_start,
                bucket_end,
                wallclock,
                sample.gateway_id,
                sample.port,
            )
            self.stats.set_active_devices(
                sum(1 for entry in self.sessions.values() if entry.active_segment is not None)
            )

        segment = session.active_segment
        assert segment is not None

        segment.last_sample_at = wallclock
        segment.gateway_ids.add(sample.gateway_id)
        segment.serial_ports.add(sample.port)
        segment.writer.writerow(
            [
                format_datetime_4(wallclock),
                f"{sample.ax_raw * self.config.scale:.6f}",
                f"{sample.ay_raw * self.config.scale:.6f}",
                f"{sample.az_raw * self.config.scale:.6f}",
            ]
        )
        segment.rows += 1
        segment.rows_since_flush += 1

        if segment.rows_since_flush >= DEFAULT_FLUSH_EVERY_ROWS:
            segment.fp.flush()
            segment.rows_since_flush = 0

        self.stats.record_sample(sample.gateway_id, sample.device_id)

        if self.config.echo_samples:
            LOGGER.info(
                "sample dev=%s gw=%s time=%s ax=%.6f ay=%.6f az=%.6f",
                sample.device_id,
                sample.gateway_id,
                format_datetime_4(wallclock),
                sample.ax_raw * self.config.scale,
                sample.ay_raw * self.config.scale,
                sample.az_raw * self.config.scale,
            )

    def _open_segment(
        self,
        device_id: str,
        bucket_start: datetime,
        bucket_end: datetime,
        first_sample_at: datetime,
        gateway_id: str,
        port: str,
    ) -> ActiveSegment:
        device_dirs = ensure_device_dirs(self.config.output_root, device_id)
        csv_final_path = self._build_unique_final_path(
            device_dirs["pending"], device_id, bucket_start, bucket_end
        )
        csv_part_path = Path(str(csv_final_path) + ".part")
        meta_final_path = final_meta_path_for(csv_final_path)
        meta_part_path = part_meta_path_for(csv_part_path)

        fp = csv_part_path.open("w", newline="", encoding="utf-8")
        writer = csv.writer(fp)
        writer.writerow(["datetime", "ax", "ay", "az"])
        fp.flush()

        LOGGER.info("Opened CSV segment for device %s: %s", device_id, csv_final_path.name)
        return ActiveSegment(
            csv_final_path=csv_final_path,
            csv_part_path=csv_part_path,
            meta_final_path=meta_final_path,
            meta_part_path=meta_part_path,
            bucket_start=bucket_start,
            bucket_end=bucket_end,
            first_sample_at=first_sample_at,
            last_sample_at=first_sample_at,
            gateway_ids={gateway_id},
            serial_ports={port},
            fp=fp,
            writer=writer,
        )

    def _finalize_segment(self, device_id: str, segment: ActiveSegment) -> None:
        metadata = SegmentMetadata(
            device_id=device_id,
            gateway_ids=sorted(segment.gateway_ids),
            serial_ports=sorted(segment.serial_ports),
            bucket_start=format_datetime_4(segment.bucket_start),
            bucket_end=format_datetime_4(segment.bucket_end),
            first_sample_at=format_datetime_4(segment.first_sample_at),
            last_sample_at=format_datetime_4(segment.last_sample_at),
            rows=segment.rows,
            created_at=format_datetime_4(datetime.now()),
        )

        segment.fp.flush()
        segment.fp.close()
        write_json(segment.meta_part_path, asdict(metadata))

        segment.csv_part_path.rename(segment.csv_final_path)
        segment.meta_part_path.rename(segment.meta_final_path)

        LOGGER.info(
            "Closed CSV segment for device %s: %s rows=%s",
            device_id,
            segment.csv_final_path.name,
            segment.rows,
        )

    @staticmethod
    def _build_unique_final_path(
        pending_dir: Path,
        device_id: str,
        bucket_start: datetime,
        bucket_end: datetime,
    ) -> Path:
        base_name = build_segment_file_name(device_id, bucket_start, bucket_end)
        candidate = pending_dir / base_name
        if not candidate.exists() and not Path(str(candidate) + ".part").exists():
            return candidate

        stem = candidate.stem
        suffix = candidate.suffix
        index = 2
        while True:
            candidate = pending_dir / f"{stem}_seg{index:02d}{suffix}"
            if not candidate.exists() and not Path(str(candidate) + ".part").exists():
                return candidate
            index += 1


class MultipartHttpUploader:
    def __init__(self, config: GatewayConfig):
        self.config = config

    def upload(self, task: UploadTask) -> tuple[bool, str]:
        file_bytes = task.csv_path.read_bytes()
        boundary = f"----LoRaGatewayBoundary{uuid.uuid4().hex}"
        content_type = (
            f"multipart/form-data; boundary={boundary}"
        )
        body = bytearray()

        fields = {
            "device_id": task.metadata.device_id,
            "gateway_ids": ",".join(task.metadata.gateway_ids),
            "serial_ports": ",".join(task.metadata.serial_ports),
            "bucket_start": task.metadata.bucket_start,
            "bucket_end": task.metadata.bucket_end,
            "started_at": task.metadata.first_sample_at,
            "ended_at": task.metadata.last_sample_at,
            "rows": str(task.metadata.rows),
            "format": "csv",
        }

        for name, value in fields.items():
            self._append_form_field(body, boundary, name, value)

        guessed_type = mimetypes.guess_type(task.csv_path.name)[0] or "text/csv"
        self._append_file_field(
            body,
            boundary,
            self.config.multipart_field,
            task.csv_path.name,
            guessed_type,
            file_bytes,
        )
        body.extend(f"--{boundary}--\r\n".encode("utf-8"))

        req = request.Request(
            self.config.upload_url,
            data=bytes(body),
            method="POST",
            headers={"Content-Type": content_type},
        )

        try:
            with request.urlopen(req, timeout=self.config.connect_timeout) as resp:
                status = getattr(resp, "status", resp.getcode())
                response_body = resp.read(200).decode("utf-8", errors="ignore")
        except error.HTTPError as exc:
            response_body = exc.read(200).decode("utf-8", errors="ignore")
            return False, f"HTTP {exc.code}: {response_body or exc.reason}"
        except error.URLError as exc:
            return False, str(exc.reason)
        except OSError as exc:
            return False, str(exc)

        if 200 <= status < 300:
            return True, response_body
        return False, f"HTTP {status}: {response_body}"

    @staticmethod
    def _append_form_field(
        body: bytearray, boundary: str, field_name: str, value: str
    ) -> None:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{field_name}"\r\n\r\n'.encode(
                "utf-8"
            )
        )
        body.extend(str(value).encode("utf-8"))
        body.extend(b"\r\n")

    @staticmethod
    def _append_file_field(
        body: bytearray,
        boundary: str,
        field_name: str,
        filename: str,
        content_type: str,
        payload: bytes,
    ) -> None:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            (
                f'Content-Disposition: form-data; name="{field_name}"; '
                f'filename="{filename}"\r\n'
            ).encode("utf-8")
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(payload)
        body.extend(b"\r\n")


def serial_reader(
    source: SerialSource,
    config: GatewayConfig,
    sample_queue: queue.Queue[SampleEvent],
    stop_event: threading.Event,
    stats: GatewayStats,
) -> None:
    dropped_since_last_log = 0

    while not stop_event.is_set():
        try:
            ser = serial.Serial(source.port, config.serial_baud, timeout=1)
            LOGGER.info("Serial connected: %s (%s)", source.gateway_id, source.port)
        except SerialException as exc:
            stats.record_serial_error(source.gateway_id, str(exc))
            LOGGER.warning(
                "Serial open failed for %s (%s): %s",
                source.gateway_id,
                source.port,
                exc,
            )
            stop_event.wait(config.retry_interval)
            continue

        with ser:
            try:
                while not stop_event.is_set():
                    raw_line = ser.readline()
                    if not raw_line:
                        continue

                    line = raw_line.decode("utf-8", errors="ignore").strip()
                    sample = parse_serial_sample(line, config.legacy_device_id)
                    if sample is None:
                        stats.record_non_data_line()
                        if config.log_serial and line:
                            LOGGER.info(
                                "serial %s (%s): %s",
                                source.gateway_id,
                                source.port,
                                line,
                            )
                        continue

                    event = SampleEvent(
                        device_id=sample.device_id,
                        sensor_ms=sample.sensor_ms,
                        ax_raw=sample.ax_raw,
                        ay_raw=sample.ay_raw,
                        az_raw=sample.az_raw,
                        gateway_id=source.gateway_id,
                        port=source.port,
                    )

                    try:
                        sample_queue.put_nowait(event)
                    except queue.Full:
                        stats.record_drop()
                        dropped_since_last_log += 1
                        if dropped_since_last_log == 1 or dropped_since_last_log % 100 == 0:
                            LOGGER.warning(
                                "Sample queue full, dropped %s samples so far from %s (%s)",
                                dropped_since_last_log,
                                source.gateway_id,
                                source.port,
                            )
            except SerialException as exc:
                stats.record_serial_error(source.gateway_id, str(exc))
                LOGGER.warning(
                    "Serial read failed for %s (%s): %s",
                    source.gateway_id,
                    source.port,
                    exc,
                )
                stop_event.wait(config.retry_interval)


def router_worker(
    config: GatewayConfig,
    sample_queue: queue.Queue[SampleEvent],
    stop_event: threading.Event,
    stats: GatewayStats,
) -> None:
    manager = CsvSegmentManager(config, stats)
    try:
        while not stop_event.is_set() or not sample_queue.empty():
            try:
                sample = sample_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            manager.process_sample(sample)
    finally:
        manager.close_all()


def move_task_to_dir(task: UploadTask, target_dir_name: str) -> None:
    device_root = task.csv_path.parent.parent
    target_dir = device_root / target_dir_name
    target_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(task.csv_path), target_dir / task.csv_path.name)
    if task.meta_path.exists():
        shutil.move(str(task.meta_path), target_dir / task.meta_path.name)


def uploader_worker(
    config: GatewayConfig,
    stop_event: threading.Event,
    stats: GatewayStats,
) -> None:
    uploader = MultipartHttpUploader(config)
    recover_pending_part_files(config.output_root)

    while not stop_event.is_set():
        pending_tasks: list[UploadTask] = []
        for csv_path in sorted(
            config.output_root.rglob("pending/*.csv"),
            key=lambda item: item.stat().st_mtime,
        ):
            task = load_upload_task(csv_path)
            if task is None:
                failed_dir = csv_path.parent.parent / "failed"
                failed_dir.mkdir(parents=True, exist_ok=True)
                shutil.move(str(csv_path), failed_dir / csv_path.name)
                continue
            pending_tasks.append(task)

        if not pending_tasks:
            stop_event.wait(config.scan_interval)
            continue

        for task in pending_tasks:
            if stop_event.is_set():
                return

            success, message = uploader.upload(task)
            if success:
                move_task_to_dir(task, "uploaded")
                stats.record_upload_success(task)
                LOGGER.info("Uploaded CSV: %s", task.csv_path.name)
            else:
                task.metadata.upload_attempts += 1
                task.metadata.last_error = message
                write_json(task.meta_path, asdict(task.metadata))
                stats.record_upload_failure(task, message)
                LOGGER.warning("Upload failed for %s: %s", task.csv_path.name, message)
                stop_event.wait(config.retry_interval)
                break


def status_reporter(
    config: GatewayConfig,
    stop_event: threading.Event,
    stats: GatewayStats,
) -> None:
    while not stop_event.wait(config.status_interval):
        snapshot = stats.snapshot()
        pending_files = count_pending_csv_files(config.output_root)
        gateway_summary = ", ".join(
            f"{gateway}={count}"
            for gateway, count in sorted(snapshot["samples_by_gateway"].items())
        )
        device_items = sorted(snapshot["samples_by_device"].items())[:8]
        device_summary = ", ".join(
            f"{device}={count}" for device, count in device_items
        )
        LOGGER.info(
            "status total_samples=%s active_devices=%s pending_files=%s uploaded_files=%s "
            "drops=%s serial_errors=%s last=%s",
            snapshot["total_samples"],
            snapshot["active_devices"],
            pending_files,
            snapshot["uploaded_files"],
            snapshot["dropped_samples"],
            snapshot["serial_errors"],
            snapshot["last_upload_status"],
        )
        if gateway_summary:
            LOGGER.info("status gateways: %s", gateway_summary)
        if device_summary:
            LOGGER.info("status devices: %s", device_summary)


def main() -> int:
    config = parse_args()
    configure_logging(config.log_level)

    LOGGER.info("Starting multi-port serial gateway")
    LOGGER.info("Sources: %s", ", ".join(f"{s.gateway_id}:{s.port}" for s in config.sources))
    LOGGER.info("Serial baud: %s", config.serial_baud)
    LOGGER.info("Output root: %s", config.output_root)
    LOGGER.info("Rotation window: %s minutes", config.rotation_minutes)
    LOGGER.info("Upload URL: %s", config.upload_url)
    LOGGER.info("Scale: %s", config.scale)
    if config.base_time is not None:
        LOGGER.info(
            "Base time for the first sample of each device: %s",
            format_datetime_4(config.base_time),
        )
    if config.device_map:
        LOGGER.info("Loaded device map entries: %s", len(config.device_map))

    sample_queue: queue.Queue[SampleEvent] = queue.Queue(maxsize=config.sample_queue_size)
    stop_event = threading.Event()
    stats = GatewayStats()

    threads: list[threading.Thread] = []
    for source in config.sources:
        thread = threading.Thread(
            target=serial_reader,
            name=f"serial-{source.gateway_id}",
            args=(source, config, sample_queue, stop_event, stats),
            daemon=True,
        )
        threads.append(thread)

    threads.append(
        threading.Thread(
            target=router_worker,
            name="router",
            args=(config, sample_queue, stop_event, stats),
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=uploader_worker,
            name="uploader",
            args=(config, stop_event, stats),
            daemon=True,
        )
    )
    threads.append(
        threading.Thread(
            target=status_reporter,
            name="status-reporter",
            args=(config, stop_event, stats),
            daemon=True,
        )
    )

    for thread in threads:
        thread.start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        LOGGER.info("Stopping gateway")
        stop_event.set()

    for thread in threads:
        thread.join(timeout=3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
