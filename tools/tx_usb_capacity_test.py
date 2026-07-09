#!/usr/bin/env python3
"""
Capture two TX USB serial streams and compare local sampling capacity.

Expected TX USB test firmware lines:
  TXH,dev_id,seq,t0_offset_us,fs_x10,count
  dev_id,epoch_us,ax_raw,ay_raw,az_raw
  TXG,dev_id,pps_epoch_us,pps_local_us,pps_interval_us,age_ms
  TIMEBASE,dev_id,healthy,pps_epoch_us,pps_local_us64,pps_interval_us,age_ms,pps_bad_count,local_timer_jump_count
  CLKSYNC,dev_id,pps_epoch_us,clk_hz,pps_interval_us,phase_error_us,first_int1_offset_us,healthy[,dpll_ppm,expected_cycles,stable]
  TXS,dev_id,packets,samples,int1_missing,int1_overflow,fifo_read_fail[,int1_edges,int1_pin]
"""

from __future__ import annotations

import argparse
import csv
import math
import subprocess
import re
import sys
import threading
import time
import queue
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, TextIO


def add_platformio_site_packages() -> bool:
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
            raise SystemExit("pyserial is required to read TX USB serial ports") from retry_exc
    else:
        raise SystemExit("pyserial is required to read TX USB serial ports") from exc


TX_HEADER_RE = re.compile(r"^TXH,(\d+),(\d+),(\d+),(\d+),(\d+)$")
TX_GPS_RE = re.compile(r"^TXG,(\d+),(\d+),(\d+),(\d+),(\d+)$")
TIMEBASE_RE = re.compile(r"^TIMEBASE,(\d+),([01]),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)$")
CLKSYNC_RE = re.compile(
    r"^CLKSYNC,(\d+),(\d+),(\d+),(\d+),(-?\d+),(-?\d+),([01])"
    r"(?:,(-?\d+),(\d+),([01]))?$"
)
GPS_SYNC_LOG_RE = re.compile(
    r"^GPS sync: dev=(\d+),pps_epoch_us=(\d+),pps_local_us=(\d+),"
    r"pps_interval_us=(\d+),age_ms=(\d+)(?:,.*)?$"
)
TX_STATS_RE = re.compile(r"^TXS,(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)(?:,(\d+),([01]))?$")
SAMPLE_RE = re.compile(r"^(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)$")

DEFAULT_BAUD = 460800
DEFAULT_SCALE_G = 0.0001220703125
DEFAULT_RANGE_G = 4
SUPPORTED_SAMPLE_RATES_HZ = (50.0, 100.0, 200.0, 400.0)
SUPPORTED_RANGE_G = (4, 8, 16)
CSV_TIMEBASE_CHOICES = ("auto", "raw", "aligned")
INDOOR_CAYD_OUTPUT = Path("data_mems_cayd_no_gps_16g")
INDOOR_CAYD_FIRMWARE_NOTE = (
    "Indoor MEMS-CAYD uses heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_test "
    "and heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_test; do not use "
    "usb_gps_sync_test firmware indoors. "
    "Defaults: COM6->1001, COM16->1002, Range g=16, GPS sync=off, "
    "CSV timebase=raw, Output=data_mems_cayd_no_gps_16g."
)


def is_platformio_python(executable: str | Path = sys.executable) -> bool:
    return ".platformio" in str(executable).lower()


def tkinter_probe_code() -> str:
    return "import tkinter"


def find_python_with_tkinter(
    candidates: list[str] | None = None,
    current_executable: str | Path = sys.executable,
    run_probe: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> str | None:
    if candidates is None:
        candidates = [
            "python",
            "py",
            str(Path.home() / "AppData" / "Local" / "Programs" / "Python" / "Python313" / "python.exe"),
            r"C:\Program Files\Python313\python.exe",
            r"C:\Program Files\Python312\python.exe",
            r"C:\Program Files\Python311\python.exe",
        ]

    current = str(current_executable).lower()
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        normalized = str(candidate).lower()
        if normalized == current or normalized in seen:
            continue
        seen.add(normalized)
        try:
            result = run_probe(
                [candidate, "-c", tkinter_probe_code()],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=5.0,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        if result.returncode == 0:
            return candidate
    return None


def format_tkinter_missing_message(current_executable: str | Path = sys.executable) -> str:
    base = (
        "tkinter is required for GUI mode, but the current Python does not include it.\n"
        f"Current Python: {current_executable}\n"
    )
    if is_platformio_python(current_executable):
        base += (
            "You are using PlatformIO Python, which commonly does not ship tkinter.\n"
            "Use the system Python for the GUI instead:\n"
            "  python tools\\tx_usb_capacity_test.py\n"
            "or keep using PlatformIO Python only for non-GUI CLI capture with --ports."
        )
    else:
        base += "Install or run a Python build that includes tkinter, then start the GUI again."
    return base


@dataclass
class TxUsbLine:
    kind: str
    device_id: str
    seq: int | None = None
    t0_offset_us: int | None = None
    fs_x10: int | None = None
    count: int | None = None
    epoch_us: int | None = None
    ax_raw: int | None = None
    ay_raw: int | None = None
    az_raw: int | None = None
    packets: int | None = None
    samples: int | None = None
    int1_missing: int | None = None
    int1_overflow: int | None = None
    fifo_read_fail: int | None = None
    int1_edges: int | None = None
    int1_pin: int | None = None
    pps_epoch_us: int | None = None
    pps_local_us: int | None = None
    pps_local_us64: int | None = None
    pps_interval_us: int | None = None
    gps_age_ms: int | None = None
    timebase_healthy: int | None = None
    pps_bad_count: int | None = None
    local_timer_jump_count: int | None = None
    clk_hz: int | None = None
    phase_error_us: int | None = None
    first_int1_offset_us: int | None = None
    clk_healthy: int | None = None
    dpll_ppm: int | None = None
    expected_cycles: int | None = None
    clk_stable: int | None = None


@dataclass
class PortStats:
    port: str
    expected_device_id: str
    sample_rate_hz: float
    rows: int = 0
    first_epoch_us: int | None = None
    last_epoch_us: int | None = None
    last_sample_epoch_us: int | None = None
    max_gap_us: int = 0
    gap_count: int = 0
    packets: int = 0
    last_seq: int | None = None
    seq_missing: int = 0
    seq_jumps: int = 0
    bad_device_lines: int = 0
    parse_errors: int = 0
    serial_errors: int = 0
    int1_missing: int = 0
    int1_overflow: int = 0
    fifo_read_fail: int = 0
    int1_edges: int = 0
    int1_pin: int | None = None
    gps_sync_seen: bool = False
    gps_sync_stale_count: int = 0
    pps_epoch_us: int | None = None
    pps_local_us: int | None = None
    pps_local_us64: int | None = None
    pps_interval_us: int | None = None
    gps_age_ms: int | None = None
    timebase_healthy: int | None = None
    pps_bad_count: int = 0
    local_timer_jump_count: int = 0
    pps_epoch_errors: int = 0
    last_pps_epoch_us: int | None = None
    pps_intervals: list[int] = field(default_factory=list)
    clksync_seen: bool = False
    clk_hz: int | None = None
    phase_error_us: int | None = None
    first_int1_offset_us: int | None = None
    clk_healthy: int | None = None
    dpll_ppm: int | None = None
    expected_cycles: int | None = None
    clk_stable: int | None = None

    def expected_interval_us(self) -> int:
        actual_rate_hz = self.sample_rate_hz * 1.024
        return int(round(1_000_000.0 / actual_rate_hz))

    def record_header(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        self.packets += 1
        if line.seq is not None:
            if self.last_seq is not None:
                delta = line.seq - self.last_seq
                if delta > 1:
                    self.seq_missing += delta - 1
                    self.seq_jumps += 1
            self.last_seq = line.seq

    def record_sample(self, line: TxUsbLine, csv_epoch_us: int | None = None) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        if line.epoch_us is None:
            self.parse_errors += 1
            return
        sample_epoch_us = line.epoch_us if csv_epoch_us is None else csv_epoch_us
        if self.first_epoch_us is None:
            self.first_epoch_us = sample_epoch_us
        if self.last_sample_epoch_us is not None:
            gap_us = sample_epoch_us - self.last_sample_epoch_us
            if gap_us > self.expected_interval_us() * 2:
                self.gap_count += 1
                self.max_gap_us = max(self.max_gap_us, gap_us)
        self.last_sample_epoch_us = sample_epoch_us
        self.last_epoch_us = sample_epoch_us
        self.rows += 1

    def record_stats(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        self.int1_missing = line.int1_missing or 0
        self.int1_overflow = line.int1_overflow or 0
        self.fifo_read_fail = line.fifo_read_fail or 0
        self.int1_edges = line.int1_edges or 0
        self.int1_pin = line.int1_pin

    def record_gps(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        self.gps_sync_seen = True
        if (
            self.last_pps_epoch_us is not None
            and line.pps_epoch_us is not None
            and line.pps_epoch_us <= self.last_pps_epoch_us
        ):
            self.pps_epoch_errors += 1
        if line.pps_epoch_us is not None:
            self.last_pps_epoch_us = line.pps_epoch_us
        self.pps_epoch_us = line.pps_epoch_us
        self.pps_local_us = line.pps_local_us
        self.pps_local_us64 = line.pps_local_us64
        self.pps_interval_us = line.pps_interval_us
        self.gps_age_ms = line.gps_age_ms
        self.timebase_healthy = line.timebase_healthy
        if line.pps_bad_count is not None:
            self.pps_bad_count = line.pps_bad_count
        if line.local_timer_jump_count is not None:
            self.local_timer_jump_count = line.local_timer_jump_count
        if line.pps_interval_us is not None:
            self.pps_intervals.append(line.pps_interval_us)
        if line.gps_age_ms is not None and line.gps_age_ms > 3000:
            self.gps_sync_stale_count += 1

    def record_clksync(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        self.clksync_seen = True
        self.gps_sync_seen = True
        self.pps_epoch_us = line.pps_epoch_us
        self.pps_interval_us = line.pps_interval_us
        if line.pps_interval_us is not None:
            self.pps_intervals.append(line.pps_interval_us)
        self.clk_hz = line.clk_hz
        self.phase_error_us = line.phase_error_us
        self.first_int1_offset_us = line.first_int1_offset_us
        self.clk_healthy = line.clk_healthy
        self.dpll_ppm = line.dpll_ppm
        self.expected_cycles = line.expected_cycles
        self.clk_stable = line.clk_stable

    def capture_ratio(self, duration_seconds: float) -> float:
        expected_rows = self.expected_rows(duration_seconds)
        if expected_rows <= 0:
            return 0.0
        return self.rows / expected_rows

    def expected_rows(self, duration_seconds: float) -> int:
        return int(round(self.sample_rate_hz * 1.024 * duration_seconds))

    def pps_interval_min_us(self) -> int | None:
        return min(self.pps_intervals) if self.pps_intervals else None

    def pps_interval_max_us(self) -> int | None:
        return max(self.pps_intervals) if self.pps_intervals else None

    def pps_interval_mean_us(self) -> float | None:
        if not self.pps_intervals:
            return None
        return sum(self.pps_intervals) / len(self.pps_intervals)

    def pps_interval_std_us(self) -> float | None:
        mean = self.pps_interval_mean_us()
        if mean is None:
            return None
        variance = sum((value - mean) ** 2 for value in self.pps_intervals) / len(self.pps_intervals)
        return math.sqrt(variance)


class StartCoordinator:
    def __init__(
        self,
        device_ids: list[str],
        start_delay_seconds: float,
        gps_sync: bool,
        gps_sync_wait_seconds: float,
        wall_clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.device_ids = set(device_ids)
        self.start_delay_seconds = start_delay_seconds
        self.gps_sync = gps_sync
        self.gps_sync_wait_seconds = gps_sync_wait_seconds
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self.ready_device_ids: set[str] = set()
        self.start_epoch_us: int | None = (
            None if gps_sync else int((self.wall_clock() + start_delay_seconds) * 1_000_000)
        )
        self.started_at_monotonic = self.monotonic_clock()
        self.condition = threading.Condition()

    def mark_gps_ready(self, device_id: str) -> None:
        if not self.gps_sync or device_id not in self.device_ids:
            return
        with self.condition:
            self.ready_device_ids.add(device_id)
            if self.start_epoch_us is None and self.ready_device_ids >= self.device_ids:
                requested_epoch_us = int(
                    (self.wall_clock() + self.start_delay_seconds) * 1_000_000
                )
                self.start_epoch_us = align_epoch_to_next_second_us(requested_epoch_us)
            self.condition.notify_all()

    def wait_for_start_epoch(self, stop_event: threading.Event) -> int | None:
        with self.condition:
            while self.start_epoch_us is None and not stop_event.is_set():
                remaining = self.gps_sync_wait_seconds - (
                    self.monotonic_clock() - self.started_at_monotonic
                )
                if remaining <= 0:
                    break
                self.condition.wait(timeout=min(0.2, remaining))
            return self.start_epoch_us


def parse_ports(text: str) -> list[str]:
    ports = [item.strip() for item in re.split(r"[,;\s]+", text) if item.strip()]
    result: list[str] = []
    seen: set[str] = set()
    for port in ports:
        key = port.upper()
        if key not in seen:
            seen.add(key)
            result.append(port)
    return result


def align_epoch_to_next_second_us(epoch_us: int) -> int:
    if epoch_us <= 0:
        return 0
    return ((epoch_us + 999_999) // 1_000_000) * 1_000_000


def parse_device_ids(text: str) -> list[str]:
    ids = [item.strip() for item in re.split(r"[,;\s]+", text) if item.strip()]
    if not ids or any(not item.isdigit() for item in ids):
        raise ValueError("device ids must be numeric, for example 1001,1002")
    return ids


def parse_tx_usb_line(line: str) -> TxUsbLine | None:
    line = line.strip()
    match = TX_HEADER_RE.match(line)
    if match:
        return TxUsbLine(
            kind="header",
            device_id=match.group(1),
            seq=int(match.group(2)),
            t0_offset_us=int(match.group(3)),
            fs_x10=int(match.group(4)),
            count=int(match.group(5)),
        )
    match = SAMPLE_RE.match(line)
    if match:
        return TxUsbLine(
            kind="sample",
            device_id=match.group(1),
            epoch_us=int(match.group(2)),
            ax_raw=int(match.group(3)),
            ay_raw=int(match.group(4)),
            az_raw=int(match.group(5)),
        )
    match = TX_GPS_RE.match(line)
    if match:
        return TxUsbLine(
            kind="gps",
            device_id=match.group(1),
            pps_epoch_us=int(match.group(2)),
            pps_local_us=int(match.group(3)),
            pps_interval_us=int(match.group(4)),
            gps_age_ms=int(match.group(5)),
        )
    match = TIMEBASE_RE.match(line)
    if match:
        return TxUsbLine(
            kind="gps",
            device_id=match.group(1),
            timebase_healthy=int(match.group(2)),
            pps_epoch_us=int(match.group(3)),
            pps_local_us64=int(match.group(4)),
            pps_interval_us=int(match.group(5)),
            gps_age_ms=int(match.group(6)),
            pps_bad_count=int(match.group(7)),
            local_timer_jump_count=int(match.group(8)),
        )
    match = CLKSYNC_RE.match(line)
    if match:
        return TxUsbLine(
            kind="clksync",
            device_id=match.group(1),
            pps_epoch_us=int(match.group(2)),
            clk_hz=int(match.group(3)),
            pps_interval_us=int(match.group(4)),
            phase_error_us=int(match.group(5)),
            first_int1_offset_us=int(match.group(6)),
            clk_healthy=int(match.group(7)),
            dpll_ppm=int(match.group(8)) if match.group(8) is not None else None,
            expected_cycles=int(match.group(9)) if match.group(9) is not None else None,
            clk_stable=int(match.group(10)) if match.group(10) is not None else None,
        )
    match = GPS_SYNC_LOG_RE.match(line)
    if match:
        return TxUsbLine(
            kind="gps",
            device_id=match.group(1),
            pps_epoch_us=int(match.group(2)),
            pps_local_us=int(match.group(3)),
            pps_interval_us=int(match.group(4)),
            gps_age_ms=int(match.group(5)),
        )
    match = TX_STATS_RE.match(line)
    if match:
        return TxUsbLine(
            kind="stats",
            device_id=match.group(1),
            packets=int(match.group(2)),
            samples=int(match.group(3)),
            int1_missing=int(match.group(4)),
            int1_overflow=int(match.group(5)),
            fifo_read_fail=int(match.group(6)),
            int1_edges=int(match.group(7)) if match.group(7) is not None else None,
            int1_pin=int(match.group(8)) if match.group(8) is not None else None,
        )
    return None


def epoch_us_to_datetime(epoch_us: int) -> datetime:
    return datetime.fromtimestamp(epoch_us / 1_000_000)


def format_datetime_4(dt: datetime) -> str:
    return f"{dt.year}/{dt.month}/{dt.day} {dt:%H:%M:%S}.{dt.microsecond // 100:04d}"


def actual_iim_rate_hz(configured_sample_rate_hz: float) -> float:
    return configured_sample_rate_hz * 1.024


def scale_for_range_g(range_g: int) -> float:
    return range_g / 32768.0


def format_range_scale_text(range_g: int) -> str:
    current = scale_for_range_g(range_g)
    table = "  ".join(
        f"+/-{value}g: {scale_for_range_g(value):.12f} g/count"
        for value in SUPPORTED_RANGE_G
    )
    return f"Current +/-{range_g}g = {current:.12f} g/count\n{table}"


def indoor_cayd_gui_defaults() -> argparse.Namespace:
    range_g = 16
    return argparse.Namespace(
        ports="COM6,COM16",
        device_ids="1001,1002",
        baud=DEFAULT_BAUD,
        sample_rate_hz=50.0,
        range_g=range_g,
        duration_seconds=60.0,
        start_delay_seconds=5.0,
        samples_per_packet=24,
        serial_startup_delay_seconds=2.0,
        gps_sync_wait_seconds=60.0,
        scale=scale_for_range_g(range_g),
        output=INDOOR_CAYD_OUTPUT,
        gps_sync=False,
        csv_timebase="raw",
        firmware_note=INDOOR_CAYD_FIRMWARE_NOTE,
    )


def apply_range_scale_defaults(args: argparse.Namespace) -> argparse.Namespace:
    if getattr(args, "scale", None) is None:
        args.scale = scale_for_range_g(int(getattr(args, "range_g", DEFAULT_RANGE_G)))
    return args


def aligned_grid_period_us(configured_sample_rate_hz: float) -> float:
    return 1_000_000.0 / actual_iim_rate_hz(configured_sample_rate_hz)


def csv_epoch_for_sample(start_epoch_us: int, sample_index: int, sample_rate_hz: float) -> int:
    period_us = aligned_grid_period_us(sample_rate_hz)
    return int(round(start_epoch_us + sample_index * period_us))


def resolve_csv_timebase(config: argparse.Namespace) -> str:
    csv_timebase = getattr(config, "csv_timebase", "auto")
    if csv_timebase == "auto":
        return "raw"
    return csv_timebase


def write_sample_csv(
    fp: TextIO,
    include_header: bool,
    device_id: str,
    epoch_us: int,
    ax_raw: int,
    ay_raw: int,
    az_raw: int,
    scale: float,
    csv_epoch_us: int | None = None,
) -> None:
    writer = csv.writer(fp)
    if include_header:
        writer.writerow(["datetime", "ax", "ay", "az"])
    output_epoch_us = epoch_us if csv_epoch_us is None else csv_epoch_us
    writer.writerow(
        [
            format_datetime_4(epoch_us_to_datetime(output_epoch_us)),
            f"{ax_raw * scale:.6f}",
            f"{ay_raw * scale:.6f}",
            f"{az_raw * scale:.6f}",
        ]
    )


def open_sensor_csv(output: Path, device_id: str) -> tuple[Path, TextIO]:
    date_dir = datetime.now().strftime("%Y%m%d")
    path = output / f"sensor{device_id}" / date_dir / "tx_usb_capacity.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path, path.open("w", newline="", encoding="utf-8")


def find_latest_sensor_csv(output: Path, device_id: str) -> Path:
    sensor_dir = output / f"sensor{device_id}"
    candidates = list(sensor_dir.glob("*/tx_usb_capacity.csv"))
    if not candidates:
        raise FileNotFoundError(f"No tx_usb_capacity.csv found under {sensor_dir}")
    return max(
        candidates,
        key=lambda path: (
            path.parent.name,
            path.stat().st_mtime,
        ),
    )


def format_gui_time_error_analysis(
    output: Path,
    device_ids: list[str],
    csv_timebase: str,
    window_seconds: float = 1.0,
) -> str:
    if len(device_ids) < 2:
        raise ValueError("At least two device IDs are required for time error analysis")
    if csv_timebase == "auto":
        csv_timebase = "raw"
    if csv_timebase not in ("raw", "aligned"):
        raise ValueError("csv_timebase must be raw, aligned, or auto")

    try:
        from tools.sensor_time_error_analysis import (
            analyze_sensor_time_error,
            format_time_error_report,
        )
    except ModuleNotFoundError:  # pragma: no cover - script execution from tools/
        from sensor_time_error_analysis import (
            analyze_sensor_time_error,
            format_time_error_report,
        )

    sensor_a = find_latest_sensor_csv(output, device_ids[0])
    sensor_b = find_latest_sensor_csv(output, device_ids[1])
    report = analyze_sensor_time_error(sensor_a, sensor_b, window_seconds=window_seconds)
    return format_time_error_report(report, csv_timebase=csv_timebase)


def serial_worker(
    port: str,
    device_id: str,
    config: argparse.Namespace,
    coordinator: StartCoordinator,
    stop_event: threading.Event,
    stats: PortStats,
    log: Callable[[str], None] | None = None,
) -> None:
    csv_path, csv_fp = open_sensor_csv(config.output, device_id)
    wrote_header = False
    command_sent = False
    csv_timebase = resolve_csv_timebase(config)
    current_packet_base_index: int | None = None
    current_packet_samples_seen = 0
    try:
        with serial.Serial(port, config.baud, timeout=0.2) as ser:
            time.sleep(config.serial_startup_delay_seconds)
            emit(f"[info] {port}: writing {csv_path}", log)

            while not stop_event.is_set():
                if not command_sent and (not config.gps_sync or stats.gps_sync_seen):
                    start_epoch_us = coordinator.wait_for_start_epoch(stop_event)
                    if start_epoch_us is not None:
                        command = (
                            f"START_AT {start_epoch_us} {config.sample_rate_hz:g} "
                            f"{int(config.start_delay_seconds * 1000)} {config.range_g}\n"
                        )
                        ser.write(command.encode("ascii"))
                        ser.flush()
                        command_sent = True
                        emit(f"[info] {port}: sent {command.strip()} to dev {device_id}", log)
                    elif stop_event.is_set():
                        break

                try:
                    raw = ser.readline()
                except SerialException as exc:
                    stats.serial_errors += 1
                    emit(f"[error] {port}: serial read failed: {exc}", log)
                    break
                if not raw:
                    continue
                line = raw.decode("utf-8", errors="replace").strip()
                parsed = parse_tx_usb_line(line)
                if parsed is None:
                    if line:
                        if "TX USB ignored START_AT waiting GPS/PPS" in line:
                            stats.gps_sync_stale_count += 1
                        emit(f"[{port} serial] {line}", log)
                    continue
                if parsed.kind == "header":
                    stats.record_header(parsed)
                    if parsed.device_id == device_id and parsed.seq is not None and parsed.count is not None:
                        current_packet_base_index = parsed.seq * parsed.count
                        current_packet_samples_seen = 0
                elif parsed.kind == "gps":
                    stats.record_gps(parsed)
                    coordinator.mark_gps_ready(parsed.device_id)
                    if line.startswith("GPS sync:"):
                        emit(f"[{port} serial] {line}", log)
                elif parsed.kind == "clksync":
                    stats.record_clksync(parsed)
                    coordinator.mark_gps_ready(parsed.device_id)
                elif parsed.kind == "stats":
                    stats.record_stats(parsed)
                elif parsed.kind == "sample":
                    csv_epoch_us: int | None = None
                    if (
                        csv_timebase == "aligned"
                        and parsed.device_id == device_id
                        and parsed.epoch_us is not None
                        and coordinator.start_epoch_us is not None
                    ):
                        if current_packet_base_index is None:
                            sample_index = stats.rows
                        else:
                            sample_index = current_packet_base_index + current_packet_samples_seen
                        csv_epoch_us = csv_epoch_for_sample(
                            coordinator.start_epoch_us,
                            sample_index,
                            config.sample_rate_hz,
                        )
                    if parsed.device_id == device_id:
                        current_packet_samples_seen += 1
                    stats.record_sample(parsed, csv_epoch_us=csv_epoch_us)
                    write_sample_csv(
                        csv_fp,
                        include_header=not wrote_header,
                        device_id=parsed.device_id,
                        epoch_us=parsed.epoch_us or 0,
                        ax_raw=parsed.ax_raw or 0,
                        ay_raw=parsed.ay_raw or 0,
                        az_raw=parsed.az_raw or 0,
                        scale=config.scale,
                        csv_epoch_us=csv_epoch_us,
                    )
                    wrote_header = True
    except SerialException as exc:
        stats.serial_errors += 1
        emit(f"[error] {port}: serial open failed: {exc}", log)
    finally:
        csv_fp.close()


def emit(message: str, log: Callable[[str], None] | None = None) -> None:
    if log is not None:
        log(message)
    else:
        print(message)


def epoch_delta_ms(first: int | None, second: int | None) -> float | None:
    if first is None or second is None:
        return None
    return (first - second) / 1000.0


def format_delta_ms(value: float | None) -> str:
    return "NA" if value is None else f"{value:.3f}"


def format_optional_int(value: int | None) -> str:
    return "NA" if value is None else str(value)


def format_optional_float(value: float | None) -> str:
    return "NA" if value is None else f"{value:.3f}"


def format_report_lines(
    stats_items: list[PortStats],
    duration_seconds: float,
    gps_sync: bool = False,
    csv_timebase: str = "raw",
    range_g: int = DEFAULT_RANGE_G,
    scale: float | None = None,
) -> list[str]:
    grid_rate_hz = actual_iim_rate_hz(stats_items[0].sample_rate_hz) if stats_items else 0.0
    grid_period_us = aligned_grid_period_us(stats_items[0].sample_rate_hz) if stats_items else 0.0
    if scale is None:
        scale = scale_for_range_g(range_g)
    meaning = (
        "common analysis grid"
        if csv_timebase == "aligned"
        else "GPS-calibrated INT1 edge time"
    )
    lines = [
        "[info] TX USB capacity report "
        f"csv_timebase={csv_timebase} meaning=\"{meaning}\" "
        f"grid_rate_hz={grid_rate_hz:.3f} "
        f"grid_period_us={grid_period_us:.3f} "
        f"range_g={range_g} scale={scale:.12f}"
    ]
    for stats in stats_items:
        ratio = stats.capture_ratio(duration_seconds) * 100.0
        line = (
            "PORT={port} DEV={dev} rows={rows} expected={expected} ratio={ratio:.2f}% "
            "packets={packets} seq_missing={seq_missing} seq_jumps={seq_jumps} "
            "gaps={gaps} max_gap_ms={max_gap_ms:.3f} bad_dev={bad_dev} "
            "parse_errors={parse_errors} serial_errors={serial_errors} "
            "int1_missing={int1_missing} int1_overflow={int1_overflow} "
            "fifo_read_fail={fifo_read_fail} int1_edges={int1_edges} "
            "int1_pin={int1_pin}".format(
                port=stats.port,
                dev=stats.expected_device_id,
                rows=stats.rows,
                expected=stats.expected_rows(duration_seconds),
                ratio=ratio,
                packets=stats.packets,
                seq_missing=stats.seq_missing,
                seq_jumps=stats.seq_jumps,
                gaps=stats.gap_count,
                max_gap_ms=stats.max_gap_us / 1000.0,
                bad_dev=stats.bad_device_lines,
                parse_errors=stats.parse_errors,
                serial_errors=stats.serial_errors,
                int1_missing=stats.int1_missing,
                int1_overflow=stats.int1_overflow,
                fifo_read_fail=stats.fifo_read_fail,
                int1_edges=stats.int1_edges,
                int1_pin="NA" if stats.int1_pin is None else stats.int1_pin,
            )
        )
        if gps_sync:
            line += (
                " gps_sync_seen={seen} gps_sync_stale_count={stale} "
                "pps_epoch_us={pps_epoch} gps_age_ms={age} "
                "pps_interval_min_us={pps_min} pps_interval_max_us={pps_max} "
                "pps_interval_mean_us={pps_mean} pps_interval_std_us={pps_std} "
                "pps_epoch_errors={pps_epoch_errors} timebase_healthy={healthy} "
                "pps_bad_count={pps_bad} local_timer_jump_count={jumps} "
                "clksync_seen={clksync_seen} clk_hz={clk_hz} "
                "phase_error_us={phase_error} first_int1_offset_us={first_int1} "
                "clk_healthy={clk_healthy} dpll_ppm={dpll_ppm} "
                "expected_cycles={expected_cycles} clk_stable={clk_stable}"
            ).format(
                seen=1 if stats.gps_sync_seen else 0,
                stale=stats.gps_sync_stale_count,
                pps_epoch="NA" if stats.pps_epoch_us is None else stats.pps_epoch_us,
                age="NA" if stats.gps_age_ms is None else stats.gps_age_ms,
                pps_min=format_optional_int(stats.pps_interval_min_us()),
                pps_max=format_optional_int(stats.pps_interval_max_us()),
                pps_mean=format_optional_float(stats.pps_interval_mean_us()),
                pps_std=format_optional_float(stats.pps_interval_std_us()),
                pps_epoch_errors=stats.pps_epoch_errors,
                healthy="NA" if stats.timebase_healthy is None else stats.timebase_healthy,
                pps_bad=stats.pps_bad_count,
                jumps=stats.local_timer_jump_count,
                clksync_seen=1 if stats.clksync_seen else 0,
                clk_hz=format_optional_int(stats.clk_hz),
                phase_error=format_optional_int(stats.phase_error_us),
                first_int1=format_optional_int(stats.first_int1_offset_us),
                clk_healthy="NA" if stats.clk_healthy is None else stats.clk_healthy,
                dpll_ppm=format_optional_int(stats.dpll_ppm),
                expected_cycles=format_optional_int(stats.expected_cycles),
                clk_stable="NA" if stats.clk_stable is None else stats.clk_stable,
            )
        lines.append(line)
    if gps_sync and len(stats_items) >= 2:
        first, second = stats_items[0], stats_items[1]
        lines.append(
            "GPS_SYNC_COMPARE DEV={dev_a}-{dev_b} "
            "first_epoch_delta_ms={first_delta} last_epoch_delta_ms={last_delta}".format(
                dev_a=first.expected_device_id,
                dev_b=second.expected_device_id,
                first_delta=format_delta_ms(
                    epoch_delta_ms(first.first_epoch_us, second.first_epoch_us)
                ),
                last_delta=format_delta_ms(
                    epoch_delta_ms(first.last_epoch_us, second.last_epoch_us)
                ),
            )
        )
    return lines


def print_report(
    stats_items: list[PortStats],
    duration_seconds: float,
    gps_sync: bool = False,
    csv_timebase: str = "raw",
    range_g: int = DEFAULT_RANGE_G,
    scale: float | None = None,
) -> None:
    for line in format_report_lines(
        stats_items,
        duration_seconds,
        gps_sync=gps_sync,
        csv_timebase=csv_timebase,
        range_g=range_g,
        scale=scale,
    ):
        print(line)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture two TX USB serial streams and compare sampling capacity."
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Open a local desktop GUI; this is the default when no arguments are given",
    )
    parser.set_defaults(gps_sync=True)
    gps_group = parser.add_mutually_exclusive_group()
    gps_group.add_argument(
        "--gps-sync",
        dest="gps_sync",
        action="store_true",
        help="Expect TXG GPS/PPS sync diagnostics; this is the default",
    )
    gps_group.add_argument(
        "--no-gps-sync",
        dest="gps_sync",
        action="store_false",
        help="Disable GPS/PPS sync handling for plain USB capacity tests",
    )
    parser.add_argument(
        "--csv-timebase",
        choices=CSV_TIMEBASE_CHOICES,
        default="raw",
        help="CSV datetime source: raw writes GPS/PPS-calibrated TX sample times; aligned writes a common analysis grid",
    )
    parser.add_argument("--ports", help="Comma-separated ports, e.g. COM7,COM8")
    parser.add_argument("--device-ids", help="Comma-separated ids, e.g. 1001,1002")
    parser.add_argument("--baud", type=int, default=DEFAULT_BAUD)
    parser.add_argument("--sample-rate-hz", type=float, default=50.0)
    parser.add_argument("--range-g", type=int, choices=SUPPORTED_RANGE_G, default=DEFAULT_RANGE_G)
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--start-delay-seconds", type=float, default=5.0)
    parser.add_argument("--samples-per-packet", type=int, default=24)
    parser.add_argument("--serial-startup-delay-seconds", type=float, default=2.0)
    parser.add_argument("--gps-sync-wait-seconds", type=float, default=60.0)
    parser.add_argument("--scale", type=float, default=None)
    parser.add_argument("--output", type=Path, default=Path("data_tx_usb"))
    return parser


def validate_capture_args(args: argparse.Namespace) -> argparse.Namespace:
    if not args.ports:
        raise SystemExit("--ports is required unless --gui is used")
    if not args.device_ids:
        raise SystemExit("--device-ids is required unless --gui is used")

    args.ports = parse_ports(args.ports)
    args.device_ids = parse_device_ids(args.device_ids)
    if len(args.ports) != len(args.device_ids):
        raise SystemExit("--ports and --device-ids must have the same count")
    if args.sample_rate_hz not in SUPPORTED_SAMPLE_RATES_HZ:
        allowed = ", ".join(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ)
        raise SystemExit(f"--sample-rate-hz must be one of: {allowed}")
    if args.range_g not in SUPPORTED_RANGE_G:
        allowed = ", ".join(str(range_g) for range_g in SUPPORTED_RANGE_G)
        raise SystemExit(f"--range-g must be one of: {allowed}")
    apply_range_scale_defaults(args)
    if args.scale <= 0:
        raise SystemExit("--scale must be positive")
    if args.duration_seconds <= 0 or args.start_delay_seconds <= 0:
        raise SystemExit("--duration-seconds and --start-delay-seconds must be positive")
    if args.serial_startup_delay_seconds < 0 or args.gps_sync_wait_seconds <= 0:
        raise SystemExit("--serial-startup-delay-seconds must be non-negative and --gps-sync-wait-seconds must be positive")
    if args.samples_per_packet <= 0:
        raise SystemExit("--samples-per-packet must be positive")
    if getattr(args, "csv_timebase", "auto") not in CSV_TIMEBASE_CHOICES:
        allowed = ", ".join(CSV_TIMEBASE_CHOICES)
        raise SystemExit(f"--csv-timebase must be one of: {allowed}")
    return args


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_arg_parser()
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)
    if args.gui or not argv:
        args.gui = True
        return apply_range_scale_defaults(args)
    return validate_capture_args(args)


def run_capture(
    config: argparse.Namespace,
    log: Callable[[str], None] | None = None,
    external_stop_event: threading.Event | None = None,
) -> tuple[int, list[PortStats]]:
    stop_event = external_stop_event or threading.Event()
    stats_items = [
        PortStats(port=port, expected_device_id=device_id, sample_rate_hz=config.sample_rate_hz)
        for port, device_id in zip(config.ports, config.device_ids)
    ]
    coordinator = StartCoordinator(
        device_ids=config.device_ids,
        start_delay_seconds=config.start_delay_seconds,
        gps_sync=config.gps_sync,
        gps_sync_wait_seconds=config.gps_sync_wait_seconds,
    )

    emit(
        f"[info] Starting TX USB capacity test ports={','.join(config.ports)} "
        f"sample_rate={config.sample_rate_hz:g}Hz duration={config.duration_seconds:g}s "
        f"samples_per_packet={config.samples_per_packet} gps_sync={1 if config.gps_sync else 0} "
        f"csv_timebase={resolve_csv_timebase(config)} range_g={config.range_g} "
        f"scale={config.scale:.12f}",
        log,
    )
    threads = [
        threading.Thread(
            target=serial_worker,
            args=(stats.port, stats.expected_device_id, config, coordinator, stop_event, stats, log),
            name=f"tx-usb-{stats.port}",
            daemon=True,
        )
        for stats in stats_items
    ]
    for thread in threads:
        thread.start()

    startup_budget = config.gps_sync_wait_seconds if config.gps_sync else 0.0
    stop_after = time.monotonic() + startup_budget + config.start_delay_seconds + config.duration_seconds
    try:
        while time.monotonic() < stop_after and not stop_event.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        emit("[info] Interrupted, stopping capture", log)
    finally:
        stop_event.set()

    for thread in threads:
        thread.join(timeout=3.0)
    csv_timebase = resolve_csv_timebase(config)
    for line in format_report_lines(
        stats_items,
        config.duration_seconds,
        gps_sync=config.gps_sync,
        csv_timebase=csv_timebase,
        range_g=config.range_g,
        scale=config.scale,
    ):
        emit(line, log)

    failed = any(
        stats.serial_errors > 0
        or stats.rows == 0
        or not math.isfinite(stats.capture_ratio(config.duration_seconds))
        or (config.gps_sync and not stats.gps_sync_seen)
        for stats in stats_items
    )
    return (1 if failed else 0), stats_items


def discover_serial_ports() -> list[str]:
    try:
        from serial.tools import list_ports
    except Exception:
        return []
    return [port.device for port in list_ports.comports()]


def run_gui(config: argparse.Namespace) -> int:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, scrolledtext, ttk
    except ImportError as exc:
        replacement_python = find_python_with_tkinter()
        if replacement_python:
            print(
                "[info] Current Python has no tkinter; restarting GUI with "
                f"{replacement_python}"
            )
            completed = subprocess.run([replacement_python, str(Path(__file__).resolve()), *sys.argv[1:]])
            return completed.returncode
        raise SystemExit(format_tkinter_missing_message()) from exc

    root = tk.Tk()
    root.title("TX USB Capacity Test")
    root.geometry("980x620")

    log_queue: queue.Queue[str] = queue.Queue()
    worker_thread: threading.Thread | None = None
    stop_event: threading.Event | None = None
    gui_defaults = indoor_cayd_gui_defaults()

    ports_var = tk.StringVar(value=gui_defaults.ports)
    device_ids_var = tk.StringVar(value=gui_defaults.device_ids)
    baud_var = tk.StringVar(value=str(gui_defaults.baud))
    sample_rate_var = tk.StringVar(value=str(gui_defaults.sample_rate_hz))
    range_g_var = tk.StringVar(value=str(gui_defaults.range_g))
    duration_var = tk.StringVar(value=str(gui_defaults.duration_seconds))
    start_delay_var = tk.StringVar(value=str(gui_defaults.start_delay_seconds))
    samples_per_packet_var = tk.StringVar(value=str(gui_defaults.samples_per_packet))
    serial_delay_var = tk.StringVar(value=str(gui_defaults.serial_startup_delay_seconds))
    gps_sync_wait_var = tk.StringVar(value=str(gui_defaults.gps_sync_wait_seconds))
    scale_var = tk.StringVar(value=f"{gui_defaults.scale:.12f}")
    output_var = tk.StringVar(value=str(gui_defaults.output))
    gps_sync_var = tk.BooleanVar(value=gui_defaults.gps_sync)
    csv_timebase_var = tk.StringVar(value=gui_defaults.csv_timebase)
    status_var = tk.StringVar(value="Idle")
    range_scale_text_var = tk.StringVar(value=format_range_scale_text(gui_defaults.range_g))

    def sync_scale_from_range(*_args: object) -> None:
        try:
            range_g = int(range_g_var.get())
            scale_var.set(f"{scale_for_range_g(range_g):.12f}")
            range_scale_text_var.set(format_range_scale_text(range_g))
        except ValueError:
            pass

    range_g_var.trace_add("write", sync_scale_from_range)

    def log(message: str) -> None:
        log_queue.put(message)

    def set_running(is_running: bool) -> None:
        start_button.configure(state="disabled" if is_running else "normal")
        stop_button.configure(state="normal" if is_running else "disabled")
        refresh_button.configure(state="disabled" if is_running else "normal")
        status_var.set("Running" if is_running else "Idle")

    def refresh_ports() -> None:
        ports = discover_serial_ports()
        if ports:
            ports_var.set(",".join(ports[:2]))
            log(f"[info] detected ports: {','.join(ports)}")
        else:
            log("[info] no serial ports detected")

    def browse_output() -> None:
        selected = filedialog.askdirectory(initialdir=output_var.get() or ".")
        if selected:
            output_var.set(selected)

    def build_config_from_form() -> argparse.Namespace:
        args = argparse.Namespace(
            gui=False,
            ports=ports_var.get(),
            device_ids=device_ids_var.get(),
            baud=int(baud_var.get().strip()),
            sample_rate_hz=float(sample_rate_var.get().strip()),
            range_g=int(range_g_var.get().strip()),
            duration_seconds=float(duration_var.get().strip()),
            start_delay_seconds=float(start_delay_var.get().strip()),
            samples_per_packet=int(samples_per_packet_var.get().strip()),
            serial_startup_delay_seconds=float(serial_delay_var.get().strip()),
            gps_sync_wait_seconds=float(gps_sync_wait_var.get().strip()),
            scale=float(scale_var.get().strip()),
            output=Path(output_var.get().strip() or "data_tx_usb"),
            gps_sync=bool(gps_sync_var.get()),
            csv_timebase=csv_timebase_var.get().strip() or "raw",
        )
        return validate_capture_args(args)

    def start_capture() -> None:
        nonlocal worker_thread, stop_event
        if worker_thread is not None and worker_thread.is_alive():
            return
        try:
            capture_config = build_config_from_form()
        except (SystemExit, ValueError) as exc:
            messagebox.showerror("Invalid options", str(exc))
            return

        stop_event = threading.Event()
        set_running(True)

        def worker() -> None:
            try:
                code, _stats_items = run_capture(capture_config, log=log, external_stop_event=stop_event)
                log(f"[info] capture finished with code {code}")
            except Exception as exc:  # pragma: no cover - GUI safety net
                log(f"[error] capture failed: {exc}")
            finally:
                root.after(0, lambda: set_running(False))

        worker_thread = threading.Thread(target=worker, name="tx-usb-gui", daemon=True)
        worker_thread.start()

    def stop_capture() -> None:
        if stop_event is not None:
            stop_event.set()
            log("[info] stop requested")

    def analyze_time_error() -> None:
        try:
            args = build_config_from_form()
            report = format_gui_time_error_analysis(
                output=args.output,
                device_ids=args.device_ids,
                csv_timebase=resolve_csv_timebase(args),
                window_seconds=1.0,
            )
        except (SystemExit, ValueError, FileNotFoundError, OSError) as exc:
            messagebox.showerror("Analyze time error failed", str(exc))
            return
        log(report)

    def poll_log_queue() -> None:
        try:
            while True:
                line = log_queue.get_nowait()
                log_text.configure(state="normal")
                log_text.insert("end", line + "\n")
                log_text.see("end")
                log_text.configure(state="disabled")
        except queue.Empty:
            pass
        root.after(100, poll_log_queue)

    root.columnconfigure(0, weight=1)
    root.rowconfigure(1, weight=1)

    form = ttk.Frame(root, padding=12)
    form.grid(row=0, column=0, sticky="ew")
    for col in range(8):
        form.columnconfigure(col, weight=1)

    ttk.Label(form, text="Ports").grid(row=0, column=0, sticky="w", pady=4)
    ttk.Entry(form, textvariable=ports_var, width=22).grid(row=0, column=1, columnspan=2, sticky="ew", pady=4)
    refresh_button = ttk.Button(form, text="Refresh", command=refresh_ports)
    refresh_button.grid(row=0, column=3, sticky="ew", padx=(8, 0), pady=4)

    ttk.Label(form, text="Device IDs").grid(row=0, column=4, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=device_ids_var, width=18).grid(row=0, column=5, sticky="ew", pady=4)
    ttk.Label(form, text="Baud").grid(row=0, column=6, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=baud_var, width=10).grid(row=0, column=7, sticky="ew", pady=4)

    ttk.Label(form, text="Sample Hz").grid(row=1, column=0, sticky="w", pady=4)
    ttk.Combobox(form, textvariable=sample_rate_var, values=["50", "100", "200", "400"], width=8).grid(row=1, column=1, sticky="ew", pady=4)
    ttk.Label(form, text="Range g").grid(row=1, column=2, sticky="w", padx=(16, 0), pady=4)
    ttk.Combobox(form, textvariable=range_g_var, values=["4", "8", "16"], width=8).grid(row=1, column=3, sticky="ew", pady=4)
    ttk.Label(form, text="Duration s").grid(row=1, column=4, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=duration_var, width=10).grid(row=1, column=5, sticky="ew", pady=4)
    ttk.Label(form, text="Start delay s").grid(row=1, column=6, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=start_delay_var, width=10).grid(row=1, column=7, sticky="ew", pady=4)

    ttk.Label(form, text="Serial delay s").grid(row=2, column=0, sticky="w", pady=4)
    ttk.Entry(form, textvariable=serial_delay_var, width=10).grid(row=2, column=1, sticky="ew", pady=4)
    ttk.Label(form, text="GPS wait s").grid(row=2, column=2, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=gps_sync_wait_var, width=10).grid(row=2, column=3, sticky="ew", pady=4)
    ttk.Label(form, text="Scale").grid(row=2, column=4, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=scale_var, width=14).grid(row=2, column=5, sticky="ew", pady=4)
    ttk.Label(form, text="Output").grid(row=2, column=6, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=output_var).grid(row=2, column=7, sticky="ew", pady=4)

    ttk.Label(form, textvariable=range_scale_text_var, foreground="#444").grid(
        row=3,
        column=0,
        columnspan=8,
        sticky="w",
        pady=(4, 0),
    )

    start_button = ttk.Button(form, text="Start", command=start_capture)
    start_button.grid(row=4, column=0, sticky="ew", pady=(10, 0))
    stop_button = ttk.Button(form, text="Stop", command=stop_capture, state="disabled")
    stop_button.grid(row=4, column=1, sticky="ew", padx=(8, 0), pady=(10, 0))
    ttk.Checkbutton(form, text="GPS sync", variable=gps_sync_var).grid(row=4, column=2, sticky="w", padx=(16, 0), pady=(10, 0))
    ttk.Label(form, text="CSV timebase raw/aligned").grid(row=4, column=3, sticky="w", padx=(16, 0), pady=(10, 0))
    ttk.Combobox(
        form,
        textvariable=csv_timebase_var,
        values=list(CSV_TIMEBASE_CHOICES),
        width=10,
        state="readonly",
    ).grid(row=4, column=4, sticky="ew", padx=(8, 0), pady=(10, 0))
    ttk.Button(form, text="Analyze time error", command=analyze_time_error).grid(row=4, column=5, sticky="ew", padx=(8, 0), pady=(10, 0))
    ttk.Label(form, textvariable=status_var).grid(row=4, column=6, sticky="w", padx=(16, 0), pady=(10, 0))
    ttk.Button(form, text="Browse", command=browse_output).grid(row=4, column=7, sticky="ew", padx=(8, 0), pady=(10, 0))

    log_text = scrolledtext.ScrolledText(root, state="disabled", wrap="word", height=24)
    log_text.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))

    log(gui_defaults.firmware_note)
    poll_log_queue()
    root.mainloop()
    return 0


def main() -> int:
    config = parse_args()
    if config.gui:
        return run_gui(config)
    code, _stats_items = run_capture(config)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
