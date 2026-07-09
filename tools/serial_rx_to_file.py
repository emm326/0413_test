#!/usr/bin/env python3
"""
Capture RX serial samples and write CSV/TXT rows with wall-clock timestamps.

Supported serial data formats:
1. Multi-device RX output:
   dev_id,timestamp_ms,ax_raw,ay_raw,az_raw
2. Legacy single-device RX output:
   timestamp_ms,ax_raw,ay_raw,az_raw
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable


def add_platformio_site_packages() -> bool:
    """系统 Python 没装 pyserial 时，借用 PlatformIO 自带环境里的 pyserial。"""
    candidates = [
        Path.home() / ".platformio" / "penv" / "Lib" / "site-packages",
        Path.home() / ".platformio" / "penv" / "lib" / "site-packages",
    ]
    for path in candidates:
        if path.exists() and str(path) not in sys.path:
            sys.path.insert(0, str(path))
            return True
    return False


try:
    import serial
    from serial import SerialException
except ImportError as exc:  # pragma: no cover
    if add_platformio_site_packages():
        try:
            import serial
            from serial import SerialException
        except ImportError as retry_exc:
            raise SystemExit(
                "pyserial is required.\n"
                r"Try: C:\Users\26223\.platformio\penv\Scripts\python.exe "
                r"tools\serial_rx_to_file.py --port COM6 --output rx.csv"
            ) from retry_exc
    else:
        raise SystemExit(
            "pyserial is required.\n"
            r"Try: C:\Users\26223\.platformio\penv\Scripts\python.exe "
            r"tools\serial_rx_to_file.py --port COM6 --output rx.csv"
        ) from exc


MULTI_DEVICE_RE = re.compile(r"^\s*(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
LEGACY_RE = re.compile(r"^\s*(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
# IIM-42352 default TX firmware uses +-4g full scale: 4g / 32768 counts.
# Legacy ADXL345 nodes can still use --scale 0.0039.
DEFAULT_SCALE_G = 0.0001220703125
DEFAULT_RANGE_G = 4
SUPPORTED_RANGE_G = (4, 8, 16)
DEFAULT_DEVICE_ID = "1001"
DEFAULT_ROTATE_MINUTES = 0
DEFAULT_START_DELAY_SECONDS = 5.0
DEFAULT_SAMPLE_RATE_HZ = 100.0
DEFAULT_SERIAL_BAUD = 460800
DEFAULT_SERIAL_STARTUP_DELAY_SECONDS = 2.0
DEFAULT_GAP_FACTOR = 2.0
DEFAULT_GAP_SAMPLES_PER_PACKET = 24
DEFAULT_GAP_TOP = 5
P2P_VIBRATION_HEADER_BYTES = 16
P2P_ACCEL_RAW_SAMPLE_BYTES = 6
P2P_CRC_BYTES = 2
EPOCH_PLAUSIBILITY_PAST_SECONDS = 30.0
EPOCH_PLAUSIBILITY_FUTURE_SECONDS = 300.0
SUPPORTED_SAMPLE_RATES_HZ = (50.0, 100.0, 200.0, 400.0)
EPOCH_US_THRESHOLD = 1_000_000_000_000
TK_RELAUNCH_ENV = "SERIAL_RX_TO_FILE_TK_RELAUNCHED"


@dataclass
class CaptureConfig:
    mode: str
    ports: list[str]
    baud: int
    output: Path
    rotate_minutes: int
    range_g: int
    scale: float
    base_time: datetime | None
    append: bool
    echo_data: bool
    write_header: bool
    include_id: bool
    legacy_device_id: str
    send_start_at: bool
    start_delay_seconds: float
    sample_rate_hz: float
    serial_startup_delay_seconds: float
    gap_csv_files: list[Path]
    gap_factor: float
    gap_samples_per_packet: int
    gap_top: int
    start_epoch_us: int | None = None


@dataclass
class ActiveCsvFile:
    path: Path
    fp: object
    writer: csv.writer


@dataclass
class ParsedSample:
    device_id: str
    sensor_ms: int
    ax_raw: int
    ay_raw: int
    az_raw: int
    from_legacy_line: bool
    time_is_epoch_us: bool


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
class GapInfo:
    from_line: int
    to_line: int
    from_time: datetime
    to_time: datetime
    gap_ms: float
    missing_samples: int
    missing_packets: int


@dataclass
class CsvGapReport:
    path: Path
    rows: int
    first_time: datetime | None
    last_time: datetime | None
    gap_threshold_ms: float
    gap_count: int
    max_gap_ms: float
    max_gap_from_line: int
    max_gap_to_line: int
    max_gap_missing_samples: int
    max_gap_missing_packets: int
    gaps: list[GapInfo]


def parse_ports_text(text: str) -> list[str]:
    ports = [item.strip() for item in re.split(r"[,;\s]+", text) if item.strip()]
    unique_ports: list[str] = []
    seen: set[str] = set()
    for port in ports:
        key = port.upper()
        if key not in seen:
            unique_ports.append(port)
            seen.add(key)
    return unique_ports


def parse_gap_csv_paths_text(text: str) -> list[Path]:
    raw_paths = [item.strip() for item in re.split(r"[;\n\r]+", text) if item.strip()]
    paths: list[Path] = []
    seen: set[str] = set()
    for raw_path in raw_paths:
        key = raw_path.lower()
        if key not in seen:
            paths.append(Path(raw_path))
            seen.add(key)
    return paths


def is_supported_sample_rate(sample_rate_hz: float) -> bool:
    return any(abs(sample_rate_hz - value) < 0.001 for value in SUPPORTED_SAMPLE_RATES_HZ)


def parse_gap_analysis_options(
    sample_rate_text: str,
    samples_per_packet_text: str,
    top_gaps_text: str,
) -> tuple[float, int, int]:
    try:
        sample_rate_hz = float(sample_rate_text.strip())
        samples_per_packet = int(samples_per_packet_text.strip())
        top_gaps = int(top_gaps_text.strip())
    except ValueError as exc:
        raise ValueError(f"Gap analysis option is invalid: {exc}") from exc

    if sample_rate_hz <= 0:
        raise ValueError("Analyze Hz must be greater than 0")
    if not is_supported_sample_rate(sample_rate_hz):
        allowed = ", ".join(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ)
        raise ValueError(f"Analyze Hz must be one of: {allowed}")
    if samples_per_packet <= 0:
        raise ValueError("Samples per packet must be greater than 0")
    if top_gaps < 0:
        raise ValueError("Top gaps must be 0 or greater")

    return sample_rate_hz, samples_per_packet, top_gaps


def actual_iim_sample_rate_hz(sample_rate_hz: float) -> float:
    return sample_rate_hz * 1.024


def scale_for_range_g(range_g: int) -> float:
    return range_g / 32768.0


def format_range_scale_text(range_g: int) -> str:
    current = scale_for_range_g(range_g)
    table = "  ".join(
        f"+/-{value}g: {scale_for_range_g(value):.12f} g/count"
        for value in SUPPORTED_RANGE_G
    )
    return f"Current +/-{range_g}g = {current:.12f} g/count\n{table}"


def resolved_scale(args: argparse.Namespace) -> float:
    return args.scale if args.scale is not None else scale_for_range_g(args.range_g)


def calc_p2p_packet_size_bytes(samples_per_packet: int) -> int:
    return P2P_VIBRATION_HEADER_BYTES + samples_per_packet * P2P_ACCEL_RAW_SAMPLE_BYTES + P2P_CRC_BYTES


def parse_sample_datetime(text: str) -> datetime:
    text = text.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"invalid datetime: {text!r}")


def parse_args() -> CaptureConfig | None:
    parser = argparse.ArgumentParser(
        description=(
            "Read RX serial samples, restore wall-clock time and save CSV/TXT rows."
        )
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Open a simple GUI for selecting serial and CSV options",
    )
    parser.add_argument(
        "--analyze-gaps",
        nargs="+",
        metavar="CSV",
        help=(
            "Analyze timestamp gaps in one or more existing sensor CSV files "
            "instead of opening a serial port"
        ),
    )
    parser.add_argument("--port", help="Single serial port, for example COM6")
    parser.add_argument(
        "--ports",
        help="Multiple serial ports, for example COM6,COM7,COM8",
    )
    parser.add_argument(
        "--baud",
        type=int,
        default=DEFAULT_SERIAL_BAUD,
        help=f"Serial baud rate, default {DEFAULT_SERIAL_BAUD}",
    )
    parser.add_argument("--output", help="Output file path or rotated output folder")
    parser.add_argument(
        "--rotate-minutes",
        type=int,
        default=DEFAULT_ROTATE_MINUTES,
        help=(
            "Rotate CSV files by this many minutes. "
            "Use 30 to create folders like sensor1001/20260424 and files named "
            "150000_to_152959.csv. Default: disabled"
        ),
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=None,
        help=(
            "Raw to g scale factor. Default is derived from --range-g "
            f"(4g: {DEFAULT_SCALE_G}). Use 0.0039 for legacy ADXL345 nodes"
        ),
    )
    parser.add_argument(
        "--range-g",
        type=int,
        choices=SUPPORTED_RANGE_G,
        default=DEFAULT_RANGE_G,
        help="IIM-42352 accelerometer full-scale range in g. Default: 4",
    )
    parser.add_argument(
        "--base-time",
        default="",
        help="Optional anchor time. Example: 2026/3/13 1:27:28.0320",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the output file instead of overwriting it",
    )
    parser.add_argument(
        "--echo-data",
        action="store_true",
        help="Print converted rows to the console",
    )
    parser.add_argument(
        "--with-header",
        action="store_true",
        help="Write a header row to the output file",
    )
    parser.add_argument(
        "--include-id",
        action="store_true",
        help="Write device id as the first column",
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
        "--no-start-at",
        action="store_true",
        help="Do not send START_AT to the RX board when opening the serial port",
    )
    parser.add_argument(
        "--start-delay-seconds",
        type=float,
        default=DEFAULT_START_DELAY_SECONDS,
        help="Future delay used for START_AT. Default: 5.0",
    )
    parser.add_argument(
        "--sample-rate-hz",
        type=float,
        default=DEFAULT_SAMPLE_RATE_HZ,
        help="Sample rate announced in START_AT. Supported: 50, 100, 200, 400. Default: 100",
    )
    parser.add_argument(
        "--serial-startup-delay-seconds",
        type=float,
        default=DEFAULT_SERIAL_STARTUP_DELAY_SECONDS,
        help="Delay after opening serial before START_AT, useful when the board resets. Default: 2.0",
    )
    parser.add_argument(
        "--gap-factor",
        type=float,
        default=DEFAULT_GAP_FACTOR,
        help="Gap threshold multiplier for analysis mode. Default: 2.0",
    )
    parser.add_argument(
        "--samples-per-packet",
        type=int,
        default=DEFAULT_GAP_SAMPLES_PER_PACKET,
        help=(
            "Samples per TX packet for gap packet-loss estimates. "
            f"Default: {DEFAULT_GAP_SAMPLES_PER_PACKET} "
            f"(P2P packet {calc_p2p_packet_size_bytes(DEFAULT_GAP_SAMPLES_PER_PACKET)} bytes)"
        ),
    )
    parser.add_argument(
        "--top-gaps",
        type=int,
        default=DEFAULT_GAP_TOP,
        help="Largest gaps to print per CSV in analysis mode. Default: 5",
    )
    args = parser.parse_args()

    if args.gui or len(sys.argv) == 1:
        return None

    if args.sample_rate_hz <= 0:
        raise SystemExit("--sample-rate-hz must be greater than 0")
    if not is_supported_sample_rate(args.sample_rate_hz):
        allowed = ", ".join(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ)
        raise SystemExit(f"--sample-rate-hz must be one of: {allowed}")

    if args.analyze_gaps:
        if args.gap_factor <= 0:
            raise SystemExit("--gap-factor must be greater than 0")
        if args.samples_per_packet <= 0:
            raise SystemExit("--samples-per-packet must be greater than 0")
        if args.top_gaps < 0:
            raise SystemExit("--top-gaps must be 0 or greater")
        return CaptureConfig(
            mode="analyze_gaps",
            ports=[],
            baud=args.baud,
            output=Path(args.output) if args.output else Path(),
            rotate_minutes=args.rotate_minutes,
            range_g=int(args.range_g),
            scale=resolved_scale(args),
            base_time=None,
            append=args.append,
            echo_data=args.echo_data,
            write_header=args.with_header,
            include_id=args.include_id,
            legacy_device_id=str(args.device_id),
            send_start_at=not args.no_start_at,
            start_delay_seconds=float(args.start_delay_seconds),
            sample_rate_hz=float(args.sample_rate_hz),
            serial_startup_delay_seconds=float(args.serial_startup_delay_seconds),
            gap_csv_files=[Path(item) for item in args.analyze_gaps],
            gap_factor=float(args.gap_factor),
            gap_samples_per_packet=int(args.samples_per_packet),
            gap_top=int(args.top_gaps),
        )

    port_text = args.ports or args.port or ""
    ports = parse_ports_text(port_text)
    if not ports:
        parser.error("--port or --ports is required unless --gui is used")
    if not args.output:
        parser.error("--output is required unless --gui is used")

    base_time = parse_user_time(args.base_time) if args.base_time else None
    if args.rotate_minutes < 0:
        raise SystemExit("--rotate-minutes must be 0 or greater")
    if args.start_delay_seconds <= 0:
        raise SystemExit("--start-delay-seconds must be greater than 0")
    if args.serial_startup_delay_seconds < 0:
        raise SystemExit("--serial-startup-delay-seconds must be 0 or greater")

    return CaptureConfig(
        mode="capture",
        ports=ports,
        baud=args.baud,
        output=Path(args.output),
        rotate_minutes=args.rotate_minutes,
        range_g=int(args.range_g),
        scale=resolved_scale(args),
        base_time=base_time,
        append=args.append,
        echo_data=args.echo_data,
        write_header=args.with_header,
        include_id=args.include_id,
        legacy_device_id=str(args.device_id),
        send_start_at=not args.no_start_at,
        start_delay_seconds=float(args.start_delay_seconds),
        sample_rate_hz=float(args.sample_rate_hz),
        serial_startup_delay_seconds=float(args.serial_startup_delay_seconds),
        gap_csv_files=[],
        gap_factor=float(args.gap_factor),
        gap_samples_per_packet=int(args.samples_per_packet),
        gap_top=int(args.top_gaps),
    )


def parse_user_time(text: str) -> datetime:
    text = text.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise SystemExit(
        "Invalid --base-time format. Example: 2026/3/13 1:27:28.0320"
    )


def format_datetime_4(dt: datetime) -> str:
    fraction4 = dt.microsecond // 100
    return (
        f"{dt.year}/{dt.month}/{dt.day} "
        f"{dt.hour}:{dt.minute:02d}:{dt.second:02d}.{fraction4:04d}"
    )


def analyze_csv_file(
    path: Path,
    *,
    sample_rate_hz: float = DEFAULT_SAMPLE_RATE_HZ,
    gap_factor: float = DEFAULT_GAP_FACTOR,
    samples_per_packet: int = DEFAULT_GAP_SAMPLES_PER_PACKET,
) -> CsvGapReport:
    actual_rate_hz = actual_iim_sample_rate_hz(sample_rate_hz)
    expected_period_ms = 1000.0 / actual_rate_hz
    gap_threshold_ms = expected_period_ms * gap_factor
    gaps: list[GapInfo] = []
    first_time: datetime | None = None
    last_time: datetime | None = None
    previous_time: datetime | None = None
    previous_line = 0
    rows = 0

    with path.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        if "datetime" not in (reader.fieldnames or []):
            raise ValueError(f"{path}: CSV must contain a datetime column")

        for line_number, row in enumerate(reader, start=2):
            sample_time = parse_sample_datetime(row["datetime"])
            rows += 1
            if first_time is None:
                first_time = sample_time

            if previous_time is not None:
                gap_ms = (sample_time - previous_time).total_seconds() * 1000.0
                if gap_ms > gap_threshold_ms:
                    intervals = max(1, round(gap_ms / expected_period_ms))
                    missing_samples = max(0, intervals - 1)
                    missing_packets = int(math.ceil(missing_samples / samples_per_packet))
                    gaps.append(
                        GapInfo(
                            from_line=previous_line,
                            to_line=line_number,
                            from_time=previous_time,
                            to_time=sample_time,
                            gap_ms=gap_ms,
                            missing_samples=missing_samples,
                            missing_packets=missing_packets,
                        )
                    )

            previous_time = sample_time
            previous_line = line_number
            last_time = sample_time

    max_gap = max(gaps, key=lambda item: item.gap_ms, default=None)
    return CsvGapReport(
        path=path,
        rows=rows,
        first_time=first_time,
        last_time=last_time,
        gap_threshold_ms=gap_threshold_ms,
        gap_count=len(gaps),
        max_gap_ms=max_gap.gap_ms if max_gap else 0.0,
        max_gap_from_line=max_gap.from_line if max_gap else 0,
        max_gap_to_line=max_gap.to_line if max_gap else 0,
        max_gap_missing_samples=max_gap.missing_samples if max_gap else 0,
        max_gap_missing_packets=max_gap.missing_packets if max_gap else 0,
        gaps=gaps,
    )


def format_gap_report_lines(report: CsvGapReport, top: int) -> list[str]:
    lines = [
        f"FILE={report.path}",
        "rows={rows} first={first} last={last} gap_threshold_ms={threshold:.4f}".format(
            rows=report.rows,
            first=format_datetime_4(report.first_time) if report.first_time else "-",
            last=format_datetime_4(report.last_time) if report.last_time else "-",
            threshold=report.gap_threshold_ms,
        ),
        "gaps={gaps} max_gap_ms={max_gap:.4f} from_line={from_line} "
        "to_line={to_line} missing_samples={samples} missing_packets={packets}".format(
            gaps=report.gap_count,
            max_gap=report.max_gap_ms,
            from_line=report.max_gap_from_line,
            to_line=report.max_gap_to_line,
            samples=report.max_gap_missing_samples,
            packets=report.max_gap_missing_packets,
        ),
    ]
    if top > 0 and report.gaps:
        lines.append(f"top_{top}_gaps:")
        for gap in sorted(report.gaps, key=lambda item: item.gap_ms, reverse=True)[:top]:
            lines.append(
                "  gap_ms={gap:.4f} from_line={from_line} from={from_time} "
                "to_line={to_line} to={to_time} missing_samples={samples} "
                "missing_packets={packets}".format(
                    gap=gap.gap_ms,
                    from_line=gap.from_line,
                    from_time=format_datetime_4(gap.from_time),
                    to_line=gap.to_line,
                    to_time=format_datetime_4(gap.to_time),
                    samples=gap.missing_samples,
                    packets=gap.missing_packets,
                )
            )
    return lines


def print_gap_report(report: CsvGapReport, top: int) -> None:
    for line in format_gap_report_lines(report, top):
        print(line)
    print()


def analyze_gaps(config: CaptureConfig) -> int:
    for csv_file in config.gap_csv_files:
        report = analyze_csv_file(
            csv_file,
            sample_rate_hz=config.sample_rate_hz,
            gap_factor=config.gap_factor,
            samples_per_packet=config.gap_samples_per_packet,
        )
        print_gap_report(report, config.gap_top)
    return 0


def convert_value(raw: int, scale: float) -> float:
    return raw * scale


def date_folder_name(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def file_time_name(dt: datetime) -> str:
    return dt.strftime("%H%M%S")


def floor_datetime(dt: datetime, minutes: int) -> datetime:
    minute_bucket = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute_bucket, second=0, microsecond=0)


def build_rotated_output_path(
    output_root: Path,
    device_id: str,
    sample_time: datetime,
    rotate_minutes: int,
) -> Path:
    bucket_start = floor_datetime(sample_time, rotate_minutes)
    bucket_end = bucket_start + timedelta(minutes=rotate_minutes) - timedelta(seconds=1)
    filename = f"{file_time_name(bucket_start)}_to_{file_time_name(bucket_end)}.csv"
    return output_root / f"sensor{device_id}" / date_folder_name(bucket_start) / filename


def ensure_parent_dir(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def open_output(path: Path, append: bool):
    ensure_parent_dir(path)
    mode = "a" if append else "w"
    return path.open(mode, newline="", encoding="utf-8")


def parse_serial_sample(line: str, fallback_device_id: str) -> ParsedSample | None:
    match = MULTI_DEVICE_RE.match(line)
    if match:
        time_value = int(match.group(2))
        return ParsedSample(
            device_id=match.group(1),
            sensor_ms=time_value,
            ax_raw=int(match.group(3)),
            ay_raw=int(match.group(4)),
            az_raw=int(match.group(5)),
            from_legacy_line=False,
            time_is_epoch_us=time_value >= EPOCH_US_THRESHOLD,
        )

    match = LEGACY_RE.match(line)
    if match:
        return ParsedSample(
            device_id=fallback_device_id,
            sensor_ms=int(match.group(1)),
            ax_raw=int(match.group(2)),
            ay_raw=int(match.group(3)),
            az_raw=int(match.group(4)),
            from_legacy_line=True,
            time_is_epoch_us=False,
        )

    return None


def build_header(include_id: bool) -> list[str]:
    if include_id:
        return ["id", "datetime", "ax", "ay", "az"]
    return ["datetime", "ax", "ay", "az"]


def epoch_us_to_datetime(epoch_us: int) -> datetime:
    return datetime.fromtimestamp(epoch_us / 1_000_000)


def invalid_epoch_sample_reason(
    sample_epoch_us: int,
    start_epoch_us: int | None,
    now_epoch_us: int | None = None,
) -> str | None:
    if now_epoch_us is None:
        now_epoch_us = int(time.time() * 1_000_000)

    future_limit_us = now_epoch_us + int(EPOCH_PLAUSIBILITY_FUTURE_SECONDS * 1_000_000)
    if sample_epoch_us > future_limit_us:
        delta_s = (sample_epoch_us - now_epoch_us) / 1_000_000.0
        return f"future by {delta_s:.1f}s"

    if start_epoch_us is not None:
        past_limit_us = start_epoch_us - int(EPOCH_PLAUSIBILITY_PAST_SECONDS * 1_000_000)
        if sample_epoch_us < past_limit_us:
            delta_s = (start_epoch_us - sample_epoch_us) / 1_000_000.0
            return f"before START_AT by {delta_s:.1f}s"

    return None


def send_start_at_command(ser: serial.Serial, port: str, config: CaptureConfig, log: Callable[[str], None] | None) -> None:
    if config.serial_startup_delay_seconds > 0:
        emit(
            f"[info] {port}: waiting {config.serial_startup_delay_seconds:.1f}s before START_AT",
            log,
        )
        time.sleep(config.serial_startup_delay_seconds)

    start_epoch_us = int((time.time() + config.start_delay_seconds) * 1_000_000)
    delay_ms = int(config.start_delay_seconds * 1000)
    config.start_epoch_us = start_epoch_us
    command = (
        f"START_AT {start_epoch_us} {config.sample_rate_hz:g} "
        f"{delay_ms} {config.range_g}\n"
    )
    ser.write(command.encode("ascii"))
    ser.flush()
    emit(
        f"[info] {port}: sent {command.strip()} "
        f"({format_datetime_4(epoch_us_to_datetime(start_epoch_us))}, "
        f"actual_iim_fs={actual_iim_sample_rate_hz(config.sample_rate_hz):g}Hz)",
        log,
    )


def emit(message: str, log: Callable[[str], None] | None = None) -> None:
    if log is not None:
        log(message)
    else:
        print(message)


class CsvWriterManager:
    def __init__(
        self,
        config: CaptureConfig,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config
        self.log = log
        self.active_files: dict[str, ActiveCsvFile] = {}
        self.lock = threading.Lock()

    def close(self) -> None:
        with self.lock:
            for active_file in self.active_files.values():
                active_file.fp.flush()
                active_file.fp.close()
            self.active_files.clear()

    def write_row(self, device_id: str, sample_time: datetime, row: list[str]) -> None:
        with self.lock:
            target_path = self._target_path(device_id, sample_time)
            active_file = self.active_files.get(device_id)
            if active_file is None or active_file.path != target_path:
                if active_file is not None:
                    active_file.fp.flush()
                    active_file.fp.close()
                active_file = self._open(device_id, target_path)

            active_file.writer.writerow(row)
            active_file.fp.flush()

    def _target_path(self, device_id: str, sample_time: datetime) -> Path:
        if self.config.rotate_minutes > 0:
            return build_rotated_output_path(
                self.config.output,
                device_id,
                sample_time,
                self.config.rotate_minutes,
            )
        return self.config.output

    def _open(self, device_id: str, path: Path) -> ActiveCsvFile:
        ensure_parent_dir(path)
        file_exists = path.exists()
        mode = "a" if self.config.append or self.config.rotate_minutes > 0 else "w"
        fp = path.open(mode, newline="", encoding="utf-8")
        writer = csv.writer(fp)
        active_file = ActiveCsvFile(path=path, fp=fp, writer=writer)
        self.active_files[device_id] = active_file

        if self.config.write_header and (not file_exists or path.stat().st_size == 0):
            writer.writerow(build_header(self.config.include_id))
            fp.flush()

        emit(f"[info] Output file: {path}", self.log)
        return active_file


def read_serial_port(
    port: str,
    config: CaptureConfig,
    anchors: dict[str, TimeAnchor],
    anchor_lock: threading.Lock,
    writer_manager: CsvWriterManager,
    stop_event: threading.Event,
    log: Callable[[str], None] | None = None,
) -> int:
    omitted_id_warned = False

    try:
        ser = serial.Serial(port, config.baud, timeout=1)
    except SerialException as exc:
        emit(f"[error] {port}: serial open failed: {exc}", log)
        return 1

    with ser:
        emit(f"[info] {port}: listening @ {config.baud}", log)
        if config.send_start_at:
            send_start_at_command(ser, port, config, log)

        while not stop_event.is_set():
            try:
                raw_line = ser.readline()
            except SerialException as exc:
                emit(f"[error] {port}: serial read failed: {exc}", log)
                return 1

            if not raw_line:
                continue

            line = raw_line.decode("utf-8", errors="ignore").strip()
            sample = parse_serial_sample(line, config.legacy_device_id)
            if sample is None:
                if line:
                    emit(f"[{port} serial] {line}", log)
                continue

            with anchor_lock:
                if sample.time_is_epoch_us:
                    invalid_reason = invalid_epoch_sample_reason(
                        sample.sensor_ms,
                        start_epoch_us=config.start_epoch_us if config.send_start_at else None,
                    )
                    if invalid_reason is not None:
                        emit(
                            f"[warn] {port}: dropped implausible timestamp for device "
                            f"{sample.device_id}: epoch_us={sample.sensor_ms} "
                            f"({invalid_reason})",
                            log,
                        )
                        continue
                    sample_time = epoch_us_to_datetime(sample.sensor_ms)
                    reanchored = False
                else:
                    anchor = anchors.setdefault(
                        sample.device_id, TimeAnchor(base_time=config.base_time)
                    )
                    sample_time, reanchored = anchor.to_datetime(sample.sensor_ms)

            if reanchored:
                emit(
                    f"[info] {port}: time anchor set for device {sample.device_id}: "
                    f"{format_datetime_4(sample_time)}",
                    log,
                )

            if not config.include_id and not sample.from_legacy_line and not omitted_id_warned:
                emit(
                    f"[info] {port}: device id is used for folder routing. "
                    "CSV rows keep datetime,ax,ay,az unless --include-id is enabled.",
                    log,
                )
                omitted_id_warned = True

            row = []
            if config.include_id:
                row.append(sample.device_id)
            row.extend(
                [
                    format_datetime_4(sample_time),
                    f"{convert_value(sample.ax_raw, config.scale):.6f}",
                    f"{convert_value(sample.ay_raw, config.scale):.6f}",
                    f"{convert_value(sample.az_raw, config.scale):.6f}",
                ]
            )

            writer_manager.write_row(sample.device_id, sample_time, row)

            if config.echo_data:
                emit(f"[{port}] " + ",".join(row), log)

    emit(f"[info] {port}: capture stopped", log)
    return 0


def capture(
    config: CaptureConfig,
    stop_event: threading.Event | None = None,
    log: Callable[[str], None] | None = None,
) -> int:
    if stop_event is None:
        stop_event = threading.Event()

    anchors: dict[str, TimeAnchor] = {}
    anchor_lock = threading.Lock()
    results: dict[str, int] = {}
    results_lock = threading.Lock()
    writer_manager = CsvWriterManager(config, log)

    emit(f"[info] Listening ports: {', '.join(config.ports)}", log)
    if config.rotate_minutes > 0:
        emit(
            f"[info] Rotating files every {config.rotate_minutes} minutes under: "
            f"{config.output}",
            log,
        )
        emit(
            "[info] File layout: output/sensor1001/20260424/150000_to_152959.csv",
            log,
        )
    else:
        emit(f"[info] Output file: {config.output}", log)
    emit(f"[info] Scale: {config.scale}", log)
    emit(f"[info] Include id column: {config.include_id}", log)
    if config.send_start_at:
        emit(
            f"[info] START_AT enabled: delay={config.start_delay_seconds:.1f}s, "
            f"sample_rate={config.sample_rate_hz:g}Hz, "
            f"actual_iim_fs={actual_iim_sample_rate_hz(config.sample_rate_hz):g}Hz",
            log,
        )
    else:
        emit("[info] START_AT disabled", log)
    if config.base_time is None:
        emit("[info] The first sample from each device will anchor to current PC time", log)
    else:
        emit(
            "[info] The first sample from each device will anchor to: "
            f"{format_datetime_4(config.base_time)}",
            log,
        )

    def worker(port: str) -> None:
        code = read_serial_port(
            port,
            config,
            anchors,
            anchor_lock,
            writer_manager,
            stop_event,
            log,
        )
        with results_lock:
            results[port] = code

    threads = [
        threading.Thread(target=worker, args=(port,), name=f"serial-{port}", daemon=True)
        for port in config.ports
    ]

    for thread in threads:
        thread.start()

    try:
        while any(thread.is_alive() for thread in threads):
            for thread in threads:
                thread.join(timeout=0.2)
            if stop_event.is_set():
                break
    except KeyboardInterrupt:
        emit("\n[info] Capture stopped by user", log)
        stop_event.set()
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=2)
        writer_manager.close()

    with results_lock:
        result_codes = list(results.values())

    if result_codes and any(code == 0 for code in result_codes):
        emit("[info] Capture stopped", log)
        return 0

    emit("[error] No serial port captured data successfully", log)
    return 1


def current_serial_site_packages() -> Path | None:
    try:
        return Path(serial.__file__).resolve().parents[1]
    except (AttributeError, IndexError, OSError):
        return None


def desktop_python_candidates() -> list[Path]:
    candidates = [
        Path(r"C:\Program Files\Python313\python.exe"),
        Path(r"C:\Program Files\Python312\python.exe"),
        Path(r"C:\Program Files\Python311\python.exe"),
    ]

    local_appdata = os.environ.get("LOCALAPPDATA")
    if local_appdata:
        base = Path(local_appdata) / "Programs" / "Python"
        candidates.extend(
            [
                base / "Python313" / "python.exe",
                base / "Python312" / "python.exe",
                base / "Python311" / "python.exe",
            ]
        )

    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return unique


def relaunch_gui_with_desktop_python(reason: str) -> int | None:
    """PlatformIO Python 没有 tkinter 时，自动切到系统 Python 打开本地 GUI。"""
    if os.environ.get(TK_RELAUNCH_ENV) == "1":
        return None

    current_python = Path(sys.executable).resolve()
    pyserial_site = current_serial_site_packages()
    script_path = Path(__file__).resolve()

    for python_exe in desktop_python_candidates():
        if not python_exe.exists():
            continue
        try:
            if python_exe.resolve() == current_python:
                continue
        except OSError:
            continue

        env = os.environ.copy()
        env[TK_RELAUNCH_ENV] = "1"
        if pyserial_site is not None:
            existing_pythonpath = env.get("PYTHONPATH", "")
            env["PYTHONPATH"] = (
                str(pyserial_site)
                if not existing_pythonpath
                else str(pyserial_site) + os.pathsep + existing_pythonpath
            )

        print(f"[info] Current Python has no tkinter: {reason}")
        print(f"[info] Relaunching desktop GUI with: {python_exe}")
        return subprocess.call([str(python_exe), str(script_path), *sys.argv[1:]], env=env)

    return None


def run_gui() -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, scrolledtext, ttk
        from serial.tools import list_ports
    except ImportError as exc:
        exit_code = relaunch_gui_with_desktop_python(str(exc))
        if exit_code is not None:
            return exit_code
        raise SystemExit(
            "无法打开本地桌面 GUI：当前 Python 没有 tkinter。\n"
            "建议直接运行系统 Python，例如：\n"
            r'& "C:\Program Files\Python313\python.exe" '
            r"tools\serial_rx_to_file.py --gui"
        ) from exc

    root = tk.Tk()
    root.title("LoRa RX CSV Capture")
    root.geometry("820x580")
    root.minsize(760, 540)

    log_queue: "queue.Queue[object]"
    import queue

    log_queue = queue.Queue()
    worker_thread: threading.Thread | None = None
    stop_event: threading.Event | None = None

    port_var = tk.StringVar()
    baud_var = tk.StringVar(value=str(DEFAULT_SERIAL_BAUD))
    output_var = tk.StringVar(value=str(Path.cwd() / "data"))
    rotate_var = tk.StringVar(value="30")
    range_g_var = tk.StringVar(value=str(DEFAULT_RANGE_G))
    scale_var = tk.StringVar(value=f"{scale_for_range_g(DEFAULT_RANGE_G):.12f}")
    base_time_var = tk.StringVar()
    device_id_var = tk.StringVar(value=DEFAULT_DEVICE_ID)
    start_at_var = tk.BooleanVar(value=True)
    start_delay_var = tk.StringVar(value=str(DEFAULT_START_DELAY_SECONDS))
    sample_rate_var = tk.StringVar(value=str(DEFAULT_SAMPLE_RATE_HZ))
    serial_startup_delay_var = tk.StringVar(value=str(DEFAULT_SERIAL_STARTUP_DELAY_SECONDS))
    gap_files_var = tk.StringVar()
    gap_sample_rate_var = tk.StringVar(value=str(DEFAULT_SAMPLE_RATE_HZ))
    gap_samples_per_packet_var = tk.StringVar(value=str(DEFAULT_GAP_SAMPLES_PER_PACKET))
    gap_top_var = tk.StringVar(value=str(DEFAULT_GAP_TOP))
    header_var = tk.BooleanVar(value=True)
    include_id_var = tk.BooleanVar(value=False)
    append_var = tk.BooleanVar(value=False)
    echo_var = tk.BooleanVar(value=False)
    range_scale_text_var = tk.StringVar(value=format_range_scale_text(DEFAULT_RANGE_G))

    def sync_scale_from_range(*_args: object) -> None:
        try:
            range_g = int(range_g_var.get())
            scale_var.set(f"{scale_for_range_g(range_g):.12f}")
            range_scale_text_var.set(format_range_scale_text(range_g))
        except ValueError:
            pass

    range_g_var.trace_add("write", sync_scale_from_range)

    def append_log(message: str) -> None:
        log_text.configure(state="normal")
        log_text.insert("end", message + "\n")
        log_text.see("end")
        log_text.configure(state="disabled")

    def set_running(is_running: bool) -> None:
        start_button.configure(state="disabled" if is_running else "normal")
        stop_button.configure(state="normal" if is_running else "disabled")
        refresh_button.configure(state="disabled" if is_running else "normal")
        browse_button.configure(state="disabled" if is_running else "normal")
        choose_gap_files_button.configure(state="disabled" if is_running else "normal")
        analyze_gaps_button.configure(state="disabled" if is_running else "normal")
        status_var.set("Running" if is_running else "Stopped")

    def refresh_ports() -> None:
        ports = [port.device for port in list_ports.comports()]
        values = ports[:]
        if len(ports) > 1:
            values.insert(0, ",".join(ports))
        port_combo.configure(values=values)
        if ports and not port_var.get():
            port_var.set(",".join(ports) if len(ports) > 1 else ports[0])
        append_log(
            "[info] Available ports: "
            f"{', '.join(ports) if ports else '(none)'}"
        )

    def choose_output_dir() -> None:
        selected = filedialog.askdirectory(
            title="Choose CSV output folder",
            initialdir=output_var.get() or str(Path.cwd()),
        )
        if selected:
            output_var.set(selected)

    def choose_gap_files() -> None:
        selected = filedialog.askopenfilenames(
            title="Choose sensor CSV files",
            initialdir=output_var.get() or str(Path.cwd()),
            filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
        )
        if selected:
            gap_files_var.set("; ".join(selected))

    def build_config_from_ui() -> CaptureConfig | None:
        try:
            baud = int(baud_var.get().strip())
            rotate_minutes = int(rotate_var.get().strip())
            range_g = int(range_g_var.get().strip())
            scale = float(scale_var.get().strip())
            start_delay_seconds = float(start_delay_var.get().strip())
            sample_rate_hz = float(sample_rate_var.get().strip())
            serial_startup_delay_seconds = float(serial_startup_delay_var.get().strip())
        except ValueError as exc:
            messagebox.showerror("Invalid option", f"Numeric option is invalid: {exc}")
            return None

        if rotate_minutes < 0:
            messagebox.showerror("Invalid option", "Rotation minutes must be 0 or greater")
            return None
        if range_g not in SUPPORTED_RANGE_G:
            allowed = ", ".join(str(value) for value in SUPPORTED_RANGE_G)
            messagebox.showerror("Invalid range", f"Range g must be one of: {allowed}")
            return None
        if start_delay_seconds <= 0 or sample_rate_hz <= 0 or serial_startup_delay_seconds < 0:
            messagebox.showerror(
                "Invalid option",
                "START_AT delay/rate must be positive and serial startup delay must be 0 or greater",
            )
            return None
        if not is_supported_sample_rate(sample_rate_hz):
            allowed = ", ".join(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ)
            messagebox.showerror("Invalid sample rate", f"Sample Hz must be one of: {allowed}")
            return None

        ports = parse_ports_text(port_var.get())
        if not ports:
            messagebox.showerror(
                "Missing port",
                "Please select or enter serial ports, for example COM6,COM7",
            )
            return None

        output_root = Path(output_var.get().strip() or "data")
        output_path = output_root if rotate_minutes > 0 else output_root / "rx.csv"

        try:
            base_time = parse_user_time(base_time_var.get()) if base_time_var.get().strip() else None
        except SystemExit as exc:
            messagebox.showerror("Invalid base time", str(exc))
            return None

        return CaptureConfig(
            mode="capture",
            ports=ports,
            baud=baud,
            output=output_path,
            rotate_minutes=rotate_minutes,
            range_g=range_g,
            scale=scale,
            base_time=base_time,
            append=append_var.get(),
            echo_data=echo_var.get(),
            write_header=header_var.get(),
            include_id=include_id_var.get(),
            legacy_device_id=device_id_var.get().strip() or DEFAULT_DEVICE_ID,
            send_start_at=start_at_var.get(),
            start_delay_seconds=start_delay_seconds,
            sample_rate_hz=sample_rate_hz,
            serial_startup_delay_seconds=serial_startup_delay_seconds,
            gap_csv_files=[],
            gap_factor=DEFAULT_GAP_FACTOR,
            gap_samples_per_packet=DEFAULT_GAP_SAMPLES_PER_PACKET,
            gap_top=DEFAULT_GAP_TOP,
        )

    def analyze_selected_gap_files() -> None:
        try:
            sample_rate_hz, samples_per_packet, top_gaps = parse_gap_analysis_options(
                gap_sample_rate_var.get(),
                gap_samples_per_packet_var.get(),
                gap_top_var.get(),
            )
        except ValueError as exc:
            messagebox.showerror("Invalid gap option", str(exc))
            return

        csv_paths = parse_gap_csv_paths_text(gap_files_var.get())
        if not csv_paths:
            messagebox.showerror("Missing CSV", "Please choose one or more CSV files to analyze")
            return

        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")
        append_log(
            f"[info] Analyzing {len(csv_paths)} CSV file(s), "
            f"sample_rate={sample_rate_hz:g}Hz, samples_per_packet={samples_per_packet}"
        )
        for csv_path in csv_paths:
            try:
                report = analyze_csv_file(
                    csv_path,
                    sample_rate_hz=sample_rate_hz,
                    gap_factor=DEFAULT_GAP_FACTOR,
                    samples_per_packet=samples_per_packet,
                )
            except (OSError, ValueError) as exc:
                append_log(f"[error] {csv_path}: {exc}")
                continue

            for line in format_gap_report_lines(report, top_gaps):
                append_log(line)
            append_log("")

    def start_capture() -> None:
        nonlocal worker_thread, stop_event
        if worker_thread is not None and worker_thread.is_alive():
            return

        config = build_config_from_ui()
        if config is None:
            return

        stop_event = threading.Event()
        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")
        set_running(True)

        def gui_log(message: str) -> None:
            log_queue.put(("log", message))

        def worker() -> None:
            assert stop_event is not None
            code = capture(config, stop_event=stop_event, log=gui_log)
            log_queue.put(("done", code))

        worker_thread = threading.Thread(target=worker, name="serial-capture", daemon=True)
        worker_thread.start()

    def stop_capture() -> None:
        if stop_event is not None:
            stop_event.set()
            append_log("[info] Stop requested")
        stop_button.configure(state="disabled")

    def drain_log_queue() -> None:
        nonlocal worker_thread
        try:
            while True:
                kind, payload = log_queue.get_nowait()
                if kind == "log":
                    append_log(str(payload))
                elif kind == "done":
                    append_log(f"[info] Worker exited with code {payload}")
                    worker_thread = None
                    set_running(False)
        except queue.Empty:
            pass
        root.after(100, drain_log_queue)

    def on_close() -> None:
        if worker_thread is not None and worker_thread.is_alive():
            if not messagebox.askokcancel("Stop capture", "Capture is running. Stop and close?"):
                return
            if stop_event is not None:
                stop_event.set()
        root.destroy()

    root.columnconfigure(0, weight=1)
    root.rowconfigure(2, weight=1)

    form = ttk.Frame(root, padding=12)
    form.grid(row=0, column=0, sticky="ew")
    form.columnconfigure(1, weight=1)
    form.columnconfigure(3, weight=1)

    ttk.Label(form, text="Serial Ports").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
    port_combo = ttk.Combobox(form, textvariable=port_var, width=18)
    port_combo.grid(row=0, column=1, sticky="ew", pady=4)
    refresh_button = ttk.Button(form, text="Refresh", command=refresh_ports)
    refresh_button.grid(row=0, column=2, sticky="ew", padx=8, pady=4)

    ttk.Label(form, text="Baud").grid(row=0, column=3, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=baud_var, width=12).grid(row=0, column=4, sticky="ew", pady=4)

    ttk.Label(form, text="Output Folder").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(form, textvariable=output_var).grid(row=1, column=1, columnspan=3, sticky="ew", pady=4)
    browse_button = ttk.Button(form, text="Browse", command=choose_output_dir)
    browse_button.grid(row=1, column=4, sticky="ew", padx=(8, 0), pady=4)

    ttk.Label(form, text="Rotate Minutes").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
    rotate_combo = ttk.Combobox(
        form,
        textvariable=rotate_var,
        values=("0", "5", "10", "15", "30", "60"),
        width=12,
    )
    rotate_combo.grid(row=2, column=1, sticky="ew", pady=4)

    ttk.Label(form, text="Range g").grid(row=2, column=2, sticky="w", padx=(8, 8), pady=4)
    ttk.Combobox(
        form,
        textvariable=range_g_var,
        values=tuple(str(value) for value in SUPPORTED_RANGE_G),
        width=8,
    ).grid(row=2, column=3, sticky="ew", pady=4)

    ttk.Label(form, text="Legacy ID").grid(row=2, column=4, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=device_id_var, width=10).grid(row=2, column=5, sticky="ew", pady=4)

    ttk.Label(form, text="Base Time").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(form, textvariable=base_time_var).grid(row=3, column=1, columnspan=3, sticky="ew", pady=4)
    ttk.Label(form, text="Scale").grid(row=3, column=4, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=scale_var, width=12).grid(row=3, column=5, sticky="ew", pady=4)

    ttk.Label(form, textvariable=range_scale_text_var, foreground="#444").grid(
        row=4,
        column=0,
        columnspan=6,
        sticky="w",
        pady=(4, 0),
    )

    ttk.Checkbutton(form, text="Send START_AT", variable=start_at_var).grid(row=5, column=0, sticky="w", pady=4)
    ttk.Label(form, text="Start Delay s").grid(row=5, column=1, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=start_delay_var, width=10).grid(row=5, column=2, sticky="ew", pady=4)
    ttk.Label(form, text="Sample Hz").grid(row=5, column=3, sticky="w", padx=(8, 8), pady=4)
    sample_rate_combo = ttk.Combobox(
        form,
        textvariable=sample_rate_var,
        values=tuple(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ),
        width=10,
    )
    sample_rate_combo.grid(row=5, column=4, sticky="ew", pady=4)
    ttk.Label(form, text="Boot Wait s").grid(row=6, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(form, textvariable=serial_startup_delay_var, width=8).grid(row=6, column=1, sticky="ew", pady=4)

    ttk.Label(form, text="CSV Gap Files").grid(row=7, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(form, textvariable=gap_files_var).grid(row=7, column=1, columnspan=3, sticky="ew", pady=4)
    choose_gap_files_button = ttk.Button(form, text="Choose CSVs", command=choose_gap_files)
    choose_gap_files_button.grid(row=7, column=4, sticky="ew", padx=(8, 0), pady=4)
    analyze_gaps_button = ttk.Button(form, text="Analyze Gaps", command=analyze_selected_gap_files)
    analyze_gaps_button.grid(row=7, column=5, sticky="ew", padx=(8, 0), pady=4)

    ttk.Label(form, text="Analyze Hz").grid(row=8, column=0, sticky="w", padx=(0, 8), pady=4)
    gap_sample_rate_combo = ttk.Combobox(
        form,
        textvariable=gap_sample_rate_var,
        values=tuple(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ),
        width=8,
    )
    gap_sample_rate_combo.grid(row=8, column=1, sticky="ew", pady=4)
    ttk.Label(form, text="Samples/Packet").grid(row=8, column=2, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=gap_samples_per_packet_var, width=8).grid(row=8, column=3, sticky="ew", pady=4)
    ttk.Label(form, text="Top Gaps").grid(row=8, column=4, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=gap_top_var, width=8).grid(row=8, column=5, sticky="ew", pady=4)

    options = ttk.Frame(root, padding=(12, 0, 12, 8))
    options.grid(row=1, column=0, sticky="ew")
    ttk.Checkbutton(options, text="Write header", variable=header_var).grid(row=0, column=0, sticky="w", padx=(0, 16))
    ttk.Checkbutton(options, text="Include device id column", variable=include_id_var).grid(row=0, column=1, sticky="w", padx=(0, 16))
    ttk.Checkbutton(options, text="Append existing file", variable=append_var).grid(row=0, column=2, sticky="w", padx=(0, 16))
    ttk.Checkbutton(options, text="Echo data rows", variable=echo_var).grid(row=0, column=3, sticky="w")

    log_text = scrolledtext.ScrolledText(root, state="disabled", height=18)
    log_text.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 8))

    footer = ttk.Frame(root, padding=(12, 0, 12, 12))
    footer.grid(row=3, column=0, sticky="ew")
    footer.columnconfigure(2, weight=1)
    start_button = ttk.Button(footer, text="Start Capture", command=start_capture)
    start_button.grid(row=0, column=0, padx=(0, 8))
    stop_button = ttk.Button(footer, text="Stop", command=stop_capture, state="disabled")
    stop_button.grid(row=0, column=1, padx=(0, 8))
    status_var = tk.StringVar(value="Stopped")
    ttk.Label(footer, textvariable=status_var).grid(row=0, column=2, sticky="e")

    refresh_ports()
    root.protocol("WM_DELETE_WINDOW", on_close)
    root.after(100, drain_log_queue)
    root.mainloop()
    return 0


def main() -> int:
    config = parse_args()
    if config is None:
        return run_gui()
    if config.mode == "analyze_gaps":
        return analyze_gaps(config)
    return capture(config)


if __name__ == "__main__":
    raise SystemExit(main())
