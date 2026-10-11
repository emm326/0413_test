#!/usr/bin/env python3
"""
Capture multiple TX USB serial streams and compare local sampling capacity.

Expected TX USB test firmware lines:
  TXH,dev_id,seq,t0_offset_us,fs_x10,count
  dev_id,epoch_us,ax_raw,ay_raw,az_raw
  TXG,dev_id,pps_epoch_us,pps_local_us,pps_interval_us,age_ms
  TIMEBASE,dev_id,healthy,pps_epoch_us,pps_local_us64,pps_interval_us,age_ms,pps_bad_count,local_timer_jump_count
  CLKSYNC,dev_id,pps_epoch_us,clk_hz,pps_interval_us,phase_error_us,first_int1_offset_us,healthy[,dpll_ppm,expected_cycles,stable]
  TXS,dev_id,packets,samples,int1_missing,int1_overflow,fifo_read_fail[,int1_edges,int1_pin]
  SENSOR_READY,dev_id,who_am_i,clk_in,int1_drdy,int1_edges
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
from datetime import datetime, timedelta, timezone
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
SENSOR_READY_RE = re.compile(r"^SENSOR_READY,(\d+),0x([0-9A-Fa-f]{2}),([01]),([01]),(\d+)$")
SAMPLE_RE = re.compile(r"^(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)$")

DEFAULT_BAUD = 460800
DEFAULT_SCALE_G = 0.0001220703125
DEFAULT_RANGE_G = 4
SUPPORTED_SAMPLE_RATES_HZ = (50.0, 100.0, 200.0, 400.0)
SUPPORTED_RANGE_G = (4, 8, 16)
CSV_TIMEBASE_CHOICES = (
    "auto",
    "raw",
    "aligned",
    "raw+pps-aligned",
    "raw+pps-index-aligned",
)
GUI_PROFILE_CHOICES = ("usb-gps-sync", "indoor-cayd")
PPS_ALIGN_REORDER_WINDOW_SECONDS = 5
BEIJING_TIMEZONE = timezone(timedelta(hours=8), name="Asia/Shanghai")
USB_GPS_SYNC_OUTPUT = Path("data_usb_pps_sync")
USB_GPS_SYNC_FIRMWARE_NOTE = (
    "USB+GPS PPS calibration defaults: COM5->1001, COM6->1002. "
    "Use the ordinary usb_gps_sync_test firmware with DS3231 32K connected to "
    "IIM CLKIN; do not use the GPIO3 clk_sync firmware. Defaults: Range g=4, "
    "GPS sync=on, CSV timebase=raw+pps-aligned, Output=data_usb_pps_sync."
)
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
    sensor_who: int | None = None
    sensor_clk_in: int | None = None
    sensor_int1_drdy: int | None = None
    sensor_int1_edges: int | None = None


@dataclass(frozen=True)
class UsbEpochSample:
    device_id: str
    epoch_us: int
    ax_raw: int
    ay_raw: int
    az_raw: int


@dataclass
class PpsAlignedSecond:
    pps_epoch_us: int
    anchor_epoch_us: int | None
    paired: dict[str, list[tuple[int, UsbEpochSample]]]
    paired_count: int
    dropped_by_device: dict[str, int]
    missing_slots: int
    missing_device_ids: tuple[str, ...] = ()


@dataclass
class PpsIndexAlignedSecond:
    pps_epoch_us: int
    actual_rate_hz: float
    capacity: int
    paired: dict[str, list[tuple[int, UsbEpochSample]]]
    common_count: int
    count_by_device: dict[str, int]
    ignored_by_device: dict[str, int]
    missing_by_device: dict[str, int]
    missing_device_ids: tuple[str, ...] = ()
    unhealthy_device_ids: tuple[str, ...] = ()


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
    sensor_ready_seen: bool = False
    sensor_who: int | None = None
    sensor_clk_in: int | None = None
    sensor_int1_drdy: int | None = None
    sensor_int1_edges: int = 0

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

    def record_sensor_ready(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        self.sensor_who = line.sensor_who
        self.sensor_clk_in = line.sensor_clk_in
        self.sensor_int1_drdy = line.sensor_int1_drdy
        self.sensor_int1_edges = line.sensor_int1_edges or 0
        self.sensor_ready_seen = (
            self.sensor_who == 0x6D
            and self.sensor_clk_in == 1
            and self.sensor_int1_drdy == 1
            and self.sensor_int1_edges > 0
        )

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
        require_sensor_ready: bool = False,
        wall_clock: Callable[[], float] = time.time,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.device_ids = set(device_ids)
        self.start_delay_seconds = start_delay_seconds
        self.gps_sync = gps_sync
        self.gps_sync_wait_seconds = gps_sync_wait_seconds
        self.require_sensor_ready = require_sensor_ready
        self.wall_clock = wall_clock
        self.monotonic_clock = monotonic_clock
        self.ready_device_ids: set[str] = set()
        self.latest_pps_epoch_us: dict[str, int] = {}
        self.sensor_ready_device_ids: set[str] = set()
        self.start_epoch_us: int | None = (
            None
            if gps_sync or require_sensor_ready
            else int((self.wall_clock() + start_delay_seconds) * 1_000_000)
        )
        self.started_at_monotonic = self.monotonic_clock()
        self.condition = threading.Condition()

    def mark_gps_ready(self, device_id: str, pps_epoch_us: int | None = None) -> None:
        if not self.gps_sync or device_id not in self.device_ids:
            return
        with self.condition:
            self.ready_device_ids.add(device_id)
            if pps_epoch_us is not None and pps_epoch_us > 0:
                self.latest_pps_epoch_us[device_id] = max(
                    pps_epoch_us, self.latest_pps_epoch_us.get(device_id, pps_epoch_us)
                )
            self._set_start_if_ready()
            self.condition.notify_all()

    def mark_sensor_ready(self, device_id: str) -> None:
        if device_id not in self.device_ids:
            return
        with self.condition:
            self.sensor_ready_device_ids.add(device_id)
            self._set_start_if_ready()
            self.condition.notify_all()

    def _set_start_if_ready(self) -> None:
        gps_ready = not self.gps_sync or self.ready_device_ids >= self.device_ids
        sensor_ready = (
            not self.require_sensor_ready
            or self.sensor_ready_device_ids >= self.device_ids
        )
        if self.start_epoch_us is None and gps_ready and sensor_ready:
            if self.gps_sync and self.latest_pps_epoch_us.keys() >= self.device_ids:
                requested_epoch_us = max(self.latest_pps_epoch_us.values()) + int(
                    self.start_delay_seconds * 1_000_000
                )
            else:
                requested_epoch_us = int(
                    (self.wall_clock() + self.start_delay_seconds) * 1_000_000
                )
            self.start_epoch_us = align_epoch_to_next_second_us(requested_epoch_us)

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

    def current_start_epoch(self) -> int | None:
        with self.condition:
            return self.start_epoch_us


class UsbPpsAligner:
    """Align two raw GPS/PPS timestamp streams one UTC second at a time."""

    def __init__(self, device_ids: tuple[str, str], sample_rate_hz: float) -> None:
        if len(device_ids) != 2 or device_ids[0] == device_ids[1]:
            raise ValueError("raw+pps-aligned requires exactly two distinct device ids")
        self.device_ids = device_ids
        self.period_us = aligned_grid_period_us(sample_rate_hz)
        self._buckets: dict[int, dict[str, list[UsbEpochSample]]] = {}
        self._latest_second: dict[str, int] = {}
        self._finalized_through: int | None = None
        self.late_samples = 0

    def add(self, sample: UsbEpochSample) -> list[PpsAlignedSecond]:
        if sample.device_id not in self.device_ids:
            return []
        second = sample.epoch_us // 1_000_000
        if self._finalized_through is not None and second <= self._finalized_through:
            self.late_samples += 1
            return []
        self._buckets.setdefault(second, {}).setdefault(sample.device_id, []).append(sample)
        self._latest_second[sample.device_id] = max(
            second, self._latest_second.get(sample.device_id, second)
        )
        latest_seconds = tuple(self._latest_second.values())
        normal_cutoff = (
            min(latest_seconds) if len(latest_seconds) == len(self.device_ids) else 0
        )
        # If one port disappears, do not retain every following second forever.
        # Five seconds still permits normal cross-thread arrival/reordering; older
        # seconds are finalized as missing and late packets for them are rejected.
        expiry_cutoff = max(latest_seconds) - PPS_ALIGN_REORDER_WINDOW_SECONDS
        return self._finalize_before(max(normal_cutoff, expiry_cutoff))

    def flush(self) -> list[PpsAlignedSecond]:
        results = [
            self._build_second(second, self._buckets[second])
            for second in sorted(self._buckets)
        ]
        if self._buckets:
            self._finalized_through = max(self._buckets)
        self._buckets.clear()
        self._latest_second.clear()
        return results

    def _finalize_before(self, cutoff_second: int) -> list[PpsAlignedSecond]:
        results: list[PpsAlignedSecond] = []
        for second in sorted(value for value in self._buckets if value < cutoff_second):
            results.append(self._build_second(second, self._buckets.pop(second)))
            self._finalized_through = second
        return results

    def _build_second(
        self, second: int, bucket: dict[str, list[UsbEpochSample]]
    ) -> PpsAlignedSecond:
        per_device = {
            device_id: sorted(bucket.get(device_id, []), key=lambda item: item.epoch_us)
            for device_id in self.device_ids
        }
        missing = tuple(device_id for device_id, values in per_device.items() if not values)
        dropped = {device_id: len(values) for device_id, values in per_device.items()}
        if missing:
            return PpsAlignedSecond(
                pps_epoch_us=second * 1_000_000,
                anchor_epoch_us=None,
                paired={device_id: [] for device_id in self.device_ids},
                paired_count=0,
                dropped_by_device=dropped,
                missing_slots=0,
                missing_device_ids=missing,
            )

        anchor_epoch_us = max(values[0].epoch_us for values in per_device.values())
        next_pps_epoch_us = (second + 1) * 1_000_000
        grid_slots = max(
            0,
            int(math.ceil((next_pps_epoch_us - anchor_epoch_us) / self.period_us - 1e-12)),
        )
        slots_by_device: dict[str, dict[int, UsbEpochSample]] = {}
        for device_id, values in per_device.items():
            selected: dict[int, UsbEpochSample] = {}
            for sample in values:
                if sample.epoch_us < anchor_epoch_us:
                    continue
                slot = int(round((sample.epoch_us - anchor_epoch_us) / self.period_us))
                if slot < 0 or slot >= grid_slots:
                    continue
                target_epoch_us = int(round(anchor_epoch_us + slot * self.period_us))
                previous = selected.get(slot)
                if previous is None or abs(sample.epoch_us - target_epoch_us) < abs(
                    previous.epoch_us - target_epoch_us
                ):
                    selected[slot] = sample
            slots_by_device[device_id] = selected

        common_slots = sorted(set.intersection(*(set(values) for values in slots_by_device.values())))
        paired = {
            device_id: [
                (int(round(anchor_epoch_us + slot * self.period_us)), slots_by_device[device_id][slot])
                for slot in common_slots
            ]
            for device_id in self.device_ids
        }
        paired_count = len(common_slots)
        return PpsAlignedSecond(
            pps_epoch_us=second * 1_000_000,
            anchor_epoch_us=anchor_epoch_us,
            paired=paired,
            paired_count=paired_count,
            dropped_by_device={
                device_id: len(per_device[device_id]) - paired_count
                for device_id in self.device_ids
            },
            missing_slots=max(0, grid_slots - paired_count),
        )


class UsbPpsIndexAligner:
    """Pair multiple nodes by their received sample order after each UTC PPS."""

    def __init__(
        self,
        device_ids: tuple[str, ...],
        sample_rate_hz: float,
        *,
        require_pps_health: bool = False,
    ) -> None:
        if len(device_ids) < 2 or len(set(device_ids)) != len(device_ids):
            raise ValueError(
                "raw+pps-index-aligned requires at least two distinct device ids"
            )
        self.device_ids = device_ids
        self.actual_rate_hz = actual_iim_rate_hz(sample_rate_hz)
        self.capacity = math.floor(self.actual_rate_hz)
        if self.capacity <= 0:
            raise ValueError("actual IIM sample rate must provide at least one sample per second")
        self.require_pps_health = require_pps_health
        self.start_second: int | None = None
        self._buckets: dict[int, dict[str, list[UsbEpochSample]]] = {}
        self._pps_health: dict[int, dict[str, bool]] = {}
        self._latest_second: dict[str, int] = {}
        self._finalized_through: int | None = None
        self.late_samples = 0
        self.pre_start_samples = 0

    def set_start_epoch(self, start_epoch_us: int) -> None:
        if start_epoch_us <= 0 or start_epoch_us % 1_000_000 != 0:
            raise ValueError("PPS index start epoch must be a positive UTC whole second")
        start_second = start_epoch_us // 1_000_000
        if self.start_second is not None and self.start_second != start_second:
            raise ValueError("PPS index start epoch cannot change during a run")
        self.start_second = start_second

    def add(self, sample: UsbEpochSample) -> list[PpsIndexAlignedSecond]:
        if sample.device_id not in self.device_ids:
            return []
        second = sample.epoch_us // 1_000_000
        if self.start_second is not None and second < self.start_second:
            self.pre_start_samples += 1
            return []
        if self._finalized_through is not None and second <= self._finalized_through:
            self.late_samples += 1
            return []
        self._buckets.setdefault(second, {}).setdefault(sample.device_id, []).append(sample)
        self._note_latest(sample.device_id, second)
        return self._finalize_ready()

    def mark_pps_health(
        self, device_id: str, pps_epoch_us: int, healthy: bool
    ) -> list[PpsIndexAlignedSecond]:
        if device_id not in self.device_ids or pps_epoch_us <= 0:
            return []
        second = pps_epoch_us // 1_000_000
        if self.start_second is not None and second < self.start_second:
            return []
        if self._finalized_through is not None and second <= self._finalized_through:
            return []
        health = self._pps_health.setdefault(second, {})
        previous = health.get(device_id)
        health[device_id] = healthy if previous is None else previous and healthy
        self._buckets.setdefault(second, {})
        self._note_latest(device_id, second)
        return self._finalize_ready()

    def flush(self) -> list[PpsIndexAlignedSecond]:
        """Finalize all buffered seconds; intended for deterministic unit tests."""
        results = [
            self._build_second(second, self._buckets[second])
            for second in sorted(self._buckets)
        ]
        if self._buckets:
            self._finalized_through = max(self._buckets)
        self._clear_buffers()
        return results

    def flush_complete(self) -> list[PpsIndexAlignedSecond]:
        """Finalize only proven-complete seconds and discard the trailing partial second."""
        cutoff_second = max(self._latest_second.values(), default=0)
        results = self._finalize_before(cutoff_second)
        self._clear_buffers()
        return results

    def _note_latest(self, device_id: str, second: int) -> None:
        self._latest_second[device_id] = max(
            second, self._latest_second.get(device_id, second)
        )

    def _finalize_ready(self) -> list[PpsIndexAlignedSecond]:
        if not self._latest_second:
            return []
        latest_seconds = tuple(self._latest_second.values())
        normal_cutoff = (
            min(latest_seconds) if len(latest_seconds) == len(self.device_ids) else 0
        )
        expiry_cutoff = max(latest_seconds) - PPS_ALIGN_REORDER_WINDOW_SECONDS
        return self._finalize_before(max(normal_cutoff, expiry_cutoff))

    def _finalize_before(self, cutoff_second: int) -> list[PpsIndexAlignedSecond]:
        results: list[PpsIndexAlignedSecond] = []
        for second in sorted(value for value in self._buckets if value < cutoff_second):
            results.append(self._build_second(second, self._buckets.pop(second)))
            self._pps_health.pop(second, None)
            self._finalized_through = second
        return results

    def _build_second(
        self, second: int, bucket: dict[str, list[UsbEpochSample]]
    ) -> PpsIndexAlignedSecond:
        pps_epoch_us = second * 1_000_000
        next_pps_epoch_us = pps_epoch_us + 1_000_000
        per_device: dict[str, list[UsbEpochSample]] = {}
        count_by_device: dict[str, int] = {}
        ignored_by_device: dict[str, int] = {}
        missing_by_device: dict[str, int] = {}
        indices_by_device: dict[str, dict[int, UsbEpochSample]] = {}

        for device_id in self.device_ids:
            ordered = sorted(
                (
                    sample
                    for sample in bucket.get(device_id, [])
                    if pps_epoch_us < sample.epoch_us < next_pps_epoch_us
                ),
                key=lambda item: item.epoch_us,
            )
            selected = ordered[: self.capacity]
            per_device[device_id] = selected
            count_by_device[device_id] = len(selected)
            ignored_by_device[device_id] = max(0, len(ordered) - self.capacity)
            missing_by_device[device_id] = self.capacity - len(selected)
            indices_by_device[device_id] = {
                index: sample for index, sample in enumerate(selected, start=1)
            }

        health = self._pps_health.get(second, {})
        unhealthy = (
            tuple(
                device_id
                for device_id in self.device_ids
                if health.get(device_id) is not True
            )
            if self.require_pps_health
            else ()
        )
        missing = tuple(
            device_id for device_id in self.device_ids if count_by_device[device_id] == 0
        )
        if unhealthy or missing:
            common_indices: list[int] = []
        else:
            common_indices = sorted(
                set.intersection(
                    *(set(values.keys()) for values in indices_by_device.values())
                )
            )

        paired = {
            device_id: [
                (
                    int(
                        round(
                            pps_epoch_us
                            + index * 1_000_000.0 / self.actual_rate_hz
                        )
                    ),
                    indices_by_device[device_id][index],
                )
                for index in common_indices
            ]
            for device_id in self.device_ids
        }
        return PpsIndexAlignedSecond(
            pps_epoch_us=pps_epoch_us,
            actual_rate_hz=self.actual_rate_hz,
            capacity=self.capacity,
            paired=paired,
            common_count=len(common_indices),
            count_by_device=count_by_device,
            ignored_by_device=ignored_by_device,
            missing_by_device=missing_by_device,
            missing_device_ids=missing,
            unhealthy_device_ids=unhealthy,
        )

    def _clear_buffers(self) -> None:
        self._buckets.clear()
        self._pps_health.clear()
        self._latest_second.clear()


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
        healthy_match = re.search(r"(?:^|,)healthy=([01])(?:,|$)", line)
        return TxUsbLine(
            kind="gps",
            device_id=match.group(1),
            pps_epoch_us=int(match.group(2)),
            pps_local_us=int(match.group(3)),
            pps_interval_us=int(match.group(4)),
            gps_age_ms=int(match.group(5)),
            timebase_healthy=(
                int(healthy_match.group(1)) if healthy_match is not None else None
            ),
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
    match = SENSOR_READY_RE.match(line)
    if match:
        return TxUsbLine(
            kind="sensor_ready",
            device_id=match.group(1),
            sensor_who=int(match.group(2), 16),
            sensor_clk_in=int(match.group(3)),
            sensor_int1_drdy=int(match.group(4)),
            sensor_int1_edges=int(match.group(5)),
        )
    return None


def epoch_us_to_datetime(epoch_us: int) -> datetime:
    """Convert a GPS-calibrated Unix epoch to a naive Beijing-time datetime."""
    return datetime.fromtimestamp(
        epoch_us / 1_000_000, tz=BEIJING_TIMEZONE
    ).replace(tzinfo=None)


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


def usb_gps_sync_gui_defaults() -> argparse.Namespace:
    range_g = DEFAULT_RANGE_G
    return argparse.Namespace(
        ports="COM5,COM6",
        device_ids="1001,1002",
        baud=DEFAULT_BAUD,
        sample_rate_hz=50.0,
        range_g=range_g,
        duration_seconds=600.0,
        start_delay_seconds=5.0,
        samples_per_packet=24,
        serial_startup_delay_seconds=2.0,
        gps_sync_wait_seconds=60.0,
        scale=scale_for_range_g(range_g),
        output=USB_GPS_SYNC_OUTPUT,
        gps_sync=True,
        csv_timebase="raw+pps-aligned",
        firmware_note=USB_GPS_SYNC_FIRMWARE_NOTE,
    )


def gui_defaults_for_profile(profile: str | None) -> argparse.Namespace:
    if profile == "indoor-cayd":
        return indoor_cayd_gui_defaults()
    return usb_gps_sync_gui_defaults()


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
    date_dir = datetime.now(BEIJING_TIMEZONE).strftime("%Y%m%d")
    path = output / f"sensor{device_id}" / date_dir / "tx_usb_capacity.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path, path.open("w", newline="", encoding="utf-8")


class LazyCsvWriters:
    """Create sensor CSV files only when the first valid sample is written."""

    def __init__(self, output: Path, scale: float) -> None:
        self.output = output
        self.scale = scale
        self._files: dict[str, TextIO] = {}
        self.paths: dict[str, Path] = {}

    def write(self, sample: UsbEpochSample, output_epoch_us: int | None = None) -> Path:
        fp = self._files.get(sample.device_id)
        include_header = fp is None
        if fp is None:
            path, fp = open_sensor_csv(self.output, sample.device_id)
            self.paths[sample.device_id] = path
            self._files[sample.device_id] = fp
        write_sample_csv(
            fp,
            include_header=include_header,
            device_id=sample.device_id,
            epoch_us=sample.epoch_us,
            ax_raw=sample.ax_raw,
            ay_raw=sample.ay_raw,
            az_raw=sample.az_raw,
            scale=self.scale,
            csv_epoch_us=output_epoch_us,
        )
        fp.flush()
        return self.paths[sample.device_id]

    def close(self) -> None:
        for fp in self._files.values():
            fp.close()
        self._files.clear()


class RawAndPpsAlignedOutput:
    def __init__(
        self,
        output: Path,
        device_ids: tuple[str, str],
        sample_rate_hz: float,
        scale: float,
        log: Callable[[str], None] | None,
    ) -> None:
        self.device_ids = device_ids
        self.raw = LazyCsvWriters(output / "raw", scale)
        self.aligned = LazyCsvWriters(output / "pps_aligned", scale)
        self.aligner = UsbPpsAligner(device_ids, sample_rate_hz)
        self.log = log
        self.lock = threading.Lock()
        self.report_lines: list[str] = []
        self.closed = False

    def add(self, sample: UsbEpochSample) -> None:
        with self.lock:
            if self.closed:
                raise RuntimeError("raw+pps-aligned output is already closed")
            self.raw.write(sample)
            self._write_aligned(self.aligner.add(sample))

    def _write_aligned(self, results: list[PpsAlignedSecond]) -> None:
        for result in results:
            line = (
                "PPS_ALIGN,pps_epoch_us={pps},anchor_epoch_us={anchor},paired={paired},"
                "dropped_{a}={dropped_a},dropped_{b}={dropped_b},missing_slots={missing}"
            ).format(
                pps=result.pps_epoch_us,
                anchor="NA" if result.anchor_epoch_us is None else result.anchor_epoch_us,
                paired=result.paired_count,
                a=self.device_ids[0],
                b=self.device_ids[1],
                dropped_a=result.dropped_by_device.get(self.device_ids[0], 0),
                dropped_b=result.dropped_by_device.get(self.device_ids[1], 0),
                missing=result.missing_slots,
            )
            self.report_lines.append(line)
            emit(line, self.log)
            if result.paired_count == 0:
                continue
            for index in range(result.paired_count):
                for device_id in self.device_ids:
                    logical_epoch_us, sample = result.paired[device_id][index]
                    self.aligned.write(sample, output_epoch_us=logical_epoch_us)

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self._write_aligned(self.aligner.flush())
            self.raw.close()
            self.aligned.close()
            self.closed = True


class RawAndPpsIndexAlignedOutput:
    def __init__(
        self,
        output: Path,
        device_ids: tuple[str, ...],
        sample_rate_hz: float,
        scale: float,
        log: Callable[[str], None] | None,
        *,
        require_pps_health: bool = True,
    ) -> None:
        self.device_ids = device_ids
        self.raw = LazyCsvWriters(output / "raw", scale)
        self.aligned = LazyCsvWriters(output / "pps_index_aligned", scale)
        self.aligner = UsbPpsIndexAligner(
            device_ids,
            sample_rate_hz,
            require_pps_health=require_pps_health,
        )
        self.log = log
        self.lock = threading.Lock()
        self.report_lines: list[str] = []
        self.start_epoch_us: int | None = None
        self.closed = False

    def set_start_epoch(self, start_epoch_us: int) -> None:
        with self.lock:
            if self.closed:
                raise RuntimeError("raw+pps-index-aligned output is already closed")
            self.aligner.set_start_epoch(start_epoch_us)
            self.start_epoch_us = start_epoch_us

    def mark_pps_health(
        self, device_id: str, pps_epoch_us: int, healthy: bool
    ) -> None:
        with self.lock:
            if self.closed or self.start_epoch_us is None:
                return
            self._write_aligned(
                self.aligner.mark_pps_health(device_id, pps_epoch_us, healthy)
            )

    def add(self, sample: UsbEpochSample) -> None:
        with self.lock:
            if self.closed:
                raise RuntimeError("raw+pps-index-aligned output is already closed")
            self.raw.write(sample)
            if self.start_epoch_us is not None:
                self._write_aligned(self.aligner.add(sample))

    def _write_aligned(self, results: list[PpsIndexAlignedSecond]) -> None:
        for result in results:
            fields = [
                "PPS_INDEX_ALIGN",
                f"pps_epoch_us={result.pps_epoch_us}",
                f"actual_rate_hz={result.actual_rate_hz:.3f}",
                f"capacity={result.capacity}",
                f"common={result.common_count}",
            ]
            for device_id in self.device_ids:
                fields.extend(
                    [
                        f"count_{device_id}={result.count_by_device[device_id]}",
                        f"ignored_{device_id}={result.ignored_by_device[device_id]}",
                        f"missing_{device_id}={result.missing_by_device[device_id]}",
                    ]
                )
            fields.append(
                "unhealthy_nodes="
                + ("|".join(result.unhealthy_device_ids) or "none")
            )
            line = ",".join(fields)
            self.report_lines.append(line)
            emit(line, self.log)
            for index in range(result.common_count):
                for device_id in self.device_ids:
                    logical_epoch_us, sample = result.paired[device_id][index]
                    self.aligned.write(sample, output_epoch_us=logical_epoch_us)

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self._write_aligned(self.aligner.flush_complete())
            self.raw.close()
            self.aligned.close()
            self.closed = True


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
    combined_output: RawAndPpsAlignedOutput | RawAndPpsIndexAlignedOutput | None = None,
) -> None:
    csv_path: Path | None = None
    csv_fp: TextIO | None = None
    wrote_header = False
    command_sent = False
    csv_timebase = resolve_csv_timebase(config)
    current_packet_base_index: int | None = None
    current_packet_samples_seen = 0
    try:
        with serial.Serial(port, config.baud, timeout=0.2) as ser:
            time.sleep(config.serial_startup_delay_seconds)
            emit(f"[info] {port}: waiting for SENSOR_READY and GPS/PPS", log)

            while not stop_event.is_set():
                if not command_sent and (
                    not config.gps_sync
                    or (stats.gps_sync_seen and stats.sensor_ready_seen)
                ):
                    start_epoch_us = coordinator.wait_for_start_epoch(stop_event)
                    if start_epoch_us is not None:
                        if isinstance(combined_output, RawAndPpsIndexAlignedOutput):
                            combined_output.set_start_epoch(start_epoch_us)
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
                    if (
                        isinstance(combined_output, RawAndPpsIndexAlignedOutput)
                        and parsed.pps_epoch_us is not None
                        and parsed.timebase_healthy is not None
                    ):
                        combined_output.mark_pps_health(
                            parsed.device_id,
                            parsed.pps_epoch_us,
                            parsed.timebase_healthy == 1,
                        )
                    if (
                        parsed.pps_epoch_us is not None
                        and parsed.timebase_healthy != 0
                        and (parsed.gps_age_ms is None or parsed.gps_age_ms <= 3000)
                    ):
                        coordinator.mark_gps_ready(parsed.device_id, parsed.pps_epoch_us)
                    if line.startswith("GPS sync:"):
                        emit(f"[{port} serial] {line}", log)
                elif parsed.kind == "clksync":
                    stats.record_clksync(parsed)
                    if parsed.pps_epoch_us is not None and parsed.clk_healthy == 1:
                        coordinator.mark_gps_ready(parsed.device_id, parsed.pps_epoch_us)
                elif parsed.kind == "sensor_ready":
                    stats.record_sensor_ready(parsed)
                    if stats.sensor_ready_seen:
                        coordinator.mark_sensor_ready(parsed.device_id)
                        emit(f"[{port} serial] {line}", log)
                elif parsed.kind == "stats":
                    stats.record_stats(parsed)
                elif parsed.kind == "sample":
                    if parsed.device_id != device_id or parsed.epoch_us is None:
                        stats.record_sample(parsed)
                        continue
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
                    sample = UsbEpochSample(
                        device_id=parsed.device_id,
                        epoch_us=parsed.epoch_us,
                        ax_raw=parsed.ax_raw or 0,
                        ay_raw=parsed.ay_raw or 0,
                        az_raw=parsed.az_raw or 0,
                    )
                    if combined_output is not None:
                        combined_output.add(sample)
                    else:
                        if csv_fp is None:
                            csv_path, csv_fp = open_sensor_csv(config.output, device_id)
                            emit(f"[info] {port}: writing {csv_path}", log)
                        write_sample_csv(
                            csv_fp,
                            include_header=not wrote_header,
                            device_id=parsed.device_id,
                            epoch_us=parsed.epoch_us,
                            ax_raw=sample.ax_raw,
                            ay_raw=sample.ay_raw,
                            az_raw=sample.az_raw,
                            scale=config.scale,
                            csv_epoch_us=csv_epoch_us,
                        )
                        csv_fp.flush()
                        wrote_header = True
    except SerialException as exc:
        stats.serial_errors += 1
        emit(f"[error] {port}: serial open failed: {exc}", log)
    finally:
        if csv_fp is not None:
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
        "raw GPS timestamps plus per-PPS common grid"
        if csv_timebase == "raw+pps-aligned"
        else (
            "raw GPS timestamps plus per-PPS sample-index intersection"
            if csv_timebase == "raw+pps-index-aligned"
            else (
                "common analysis grid"
                if csv_timebase == "aligned"
                else "GPS-calibrated INT1 edge time"
            )
        )
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
            "int1_pin={int1_pin} sensor_ready={sensor_ready} "
            "sensor_who={sensor_who} sensor_clk_in={sensor_clk_in} "
            "sensor_int1_drdy={sensor_int1_drdy}".format(
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
                sensor_ready=1 if stats.sensor_ready_seen else 0,
                sensor_who=(
                    "NA" if stats.sensor_who is None else f"0x{stats.sensor_who:02X}"
                ),
                sensor_clk_in=(
                    "NA" if stats.sensor_clk_in is None else stats.sensor_clk_in
                ),
                sensor_int1_drdy=(
                    "NA" if stats.sensor_int1_drdy is None else stats.sensor_int1_drdy
                ),
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
        description="Capture multiple TX USB serial streams and compare sampling capacity."
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Open a local desktop GUI; this is the default when no arguments are given",
    )
    parser.add_argument(
        "--gui-profile",
        choices=GUI_PROFILE_CHOICES,
        default="usb-gps-sync",
        help="GUI defaults profile: usb-gps-sync uses COM5/COM6 GPS test; indoor-cayd keeps the no-GPS indoor profile",
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
        help=(
            "CSV output mode: raw writes GPS/PPS timestamps; aligned writes the legacy "
            "run grid; raw+pps-aligned keeps raw and writes a common nearest-slot grid; "
            "raw+pps-index-aligned keeps raw and intersects PPS-local sample indices"
        ),
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
    if args.csv_timebase == "raw+pps-aligned":
        if not args.gps_sync:
            raise SystemExit("--csv-timebase raw+pps-aligned requires --gps-sync")
        if len(args.device_ids) != 2 or args.device_ids[0] == args.device_ids[1]:
            raise SystemExit("--csv-timebase raw+pps-aligned requires two distinct device ids")
    if args.csv_timebase == "raw+pps-index-aligned":
        if not args.gps_sync:
            raise SystemExit(
                "--csv-timebase raw+pps-index-aligned requires --gps-sync"
            )
        if len(args.device_ids) < 2 or len(set(args.device_ids)) != len(args.device_ids):
            raise SystemExit(
                "--csv-timebase raw+pps-index-aligned requires at least two distinct device ids"
            )
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
        require_sensor_ready=config.gps_sync,
    )
    combined_output: RawAndPpsAlignedOutput | RawAndPpsIndexAlignedOutput | None = None
    csv_timebase = resolve_csv_timebase(config)
    if csv_timebase == "raw+pps-aligned":
        combined_output = RawAndPpsAlignedOutput(
            output=config.output,
            device_ids=(config.device_ids[0], config.device_ids[1]),
            sample_rate_hz=config.sample_rate_hz,
            scale=config.scale,
            log=log,
        )
    elif csv_timebase == "raw+pps-index-aligned":
        combined_output = RawAndPpsIndexAlignedOutput(
            output=config.output,
            device_ids=tuple(config.device_ids),
            sample_rate_hz=config.sample_rate_hz,
            scale=config.scale,
            log=log,
            require_pps_health=True,
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
            args=(
                stats.port,
                stats.expected_device_id,
                config,
                coordinator,
                stop_event,
                stats,
                log,
                combined_output,
            ),
            name=f"tx-usb-{stats.port}",
            daemon=True,
        )
        for stats in stats_items
    ]
    for thread in threads:
        thread.start()

    startup_deadline = (
        coordinator.started_at_monotonic + config.gps_sync_wait_seconds
        if config.gps_sync
        else None
    )
    capture_deadline: float | None = None
    try:
        while not stop_event.is_set():
            now_monotonic = time.monotonic()
            start_epoch_us = coordinator.current_start_epoch()
            if start_epoch_us is not None and capture_deadline is None:
                # START_AT is derived from device GPS/PPS, so the PC wall clock
                # may be wrong.  The requested delay is a safe upper bound here.
                seconds_until_start = config.start_delay_seconds + 1.0
                capture_deadline = now_monotonic + seconds_until_start + config.duration_seconds
            if capture_deadline is not None and now_monotonic >= capture_deadline:
                break
            if (
                capture_deadline is None
                and startup_deadline is not None
                and now_monotonic >= startup_deadline
            ):
                emit("[error] Timed out waiting for both SENSOR_READY and GPS/PPS", log)
                break
            time.sleep(0.2)
    except KeyboardInterrupt:
        emit("[info] Interrupted, stopping capture", log)
    finally:
        stop_event.set()

    for thread in threads:
        thread.join()
    if combined_output is not None:
        combined_output.close()
    report_lines = format_report_lines(
        stats_items,
        config.duration_seconds,
        gps_sync=config.gps_sync,
        csv_timebase=csv_timebase,
        range_g=config.range_g,
        scale=config.scale,
    )
    for line in report_lines:
        emit(line, log)

    if combined_output is not None and all(stats.rows > 0 for stats in stats_items):
        config.output.mkdir(parents=True, exist_ok=True)
        report_path = config.output / "capture_report.txt"
        report_path.write_text(
            "\n".join([*combined_output.report_lines, *report_lines]) + "\n",
            encoding="utf-8",
        )
        emit(f"[info] wrote {report_path}", log)

    failed = any(
        stats.serial_errors > 0
        or stats.rows == 0
        or not math.isfinite(stats.capture_ratio(config.duration_seconds))
        or (config.gps_sync and not stats.gps_sync_seen)
        or (config.gps_sync and not stats.sensor_ready_seen)
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

    log_queue: queue.Queue[str] = queue.Queue()
    worker_thread: threading.Thread | None = None
    stop_event: threading.Event | None = None
    gui_profile = getattr(config, "gui_profile", "usb-gps-sync")
    gui_defaults = gui_defaults_for_profile(gui_profile)

    root = tk.Tk()
    root.title(f"TX USB Capacity Test - {gui_profile}")
    root.geometry("980x620")

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
        range_g = int(range_g_var.get().strip())
        args = argparse.Namespace(
            gui=False,
            ports=ports_var.get(),
            device_ids=device_ids_var.get(),
            baud=int(baud_var.get().strip()),
            sample_rate_hz=float(sample_rate_var.get().strip()),
            range_g=range_g,
            duration_seconds=float(duration_var.get().strip()),
            start_delay_seconds=float(start_delay_var.get().strip()),
            samples_per_packet=int(samples_per_packet_var.get().strip()),
            serial_startup_delay_seconds=float(serial_delay_var.get().strip()),
            gps_sync_wait_seconds=float(gps_sync_wait_var.get().strip()),
            # Derive CSV conversion from the same range sent to the firmware.
            # An editable GUI scale could silently make START_AT and CSV units disagree.
            scale=scale_for_range_g(range_g),
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
            analysis_output = (
                args.output / "raw"
                if resolve_csv_timebase(args)
                in ("raw+pps-aligned", "raw+pps-index-aligned")
                else args.output
            )
            report = format_gui_time_error_analysis(
                output=analysis_output,
                device_ids=args.device_ids,
                csv_timebase="raw",
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
    ttk.Entry(form, textvariable=scale_var, width=14, state="readonly").grid(
        row=2, column=5, sticky="ew", pady=4
    )
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
    ttk.Label(form, text="CSV output mode").grid(row=4, column=3, sticky="w", padx=(16, 0), pady=(10, 0))
    ttk.Combobox(
        form,
        textvariable=csv_timebase_var,
        values=list(CSV_TIMEBASE_CHOICES),
        width=18,
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
