#!/usr/bin/env python3
"""Analyze one-second timestamp error between two sensor CSV files."""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class TimeErrorWindow:
    start_epoch_us: int
    end_epoch_us: int
    sensor_a_count: int
    sensor_b_count: int
    nearest_mean_ms: float
    nearest_max_ms: float
    nearest_rms_ms: float
    nearest_p95_ms: float
    same_index_mean_ms: float
    same_index_max_ms: float


@dataclass
class TimeErrorReport:
    sensor_a: Path
    sensor_b: Path
    window_seconds: float
    windows: list[TimeErrorWindow]


def parse_csv_datetime(text: str) -> int:
    text = text.strip()
    date_text, time_text = text.split(" ", 1)
    year_s, month_s, day_s = date_text.split("/")
    hms_text, frac_text = time_text.split(".", 1)
    hour_s, minute_s, second_s = hms_text.split(":")
    frac_text = (frac_text + "000000")[:6]
    dt = datetime(
        int(year_s),
        int(month_s),
        int(day_s),
        int(hour_s),
        int(minute_s),
        int(second_s),
        int(frac_text),
    )
    return int(round(dt.timestamp() * 1_000_000))


def read_csv_epochs(path: Path) -> list[int]:
    epochs: list[int] = []
    with path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        first = True
        for row in reader:
            if not row:
                continue
            if first and row[0].strip().lower() == "datetime":
                first = False
                continue
            first = False
            epochs.append(parse_csv_datetime(row[0]))
    return epochs


def nearest_abs_deltas_us(source: list[int], target: list[int]) -> list[int]:
    if not source or not target:
        return []
    result: list[int] = []
    j = 0
    for value in source:
        while j + 1 < len(target) and abs(target[j + 1] - value) <= abs(target[j] - value):
            j += 1
        result.append(abs(target[j] - value))
    return result


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percent / 100.0
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def rms(values: list[float]) -> float:
    return math.sqrt(sum(value * value for value in values) / len(values)) if values else 0.0


def analyze_sensor_time_error(
    sensor_a: Path,
    sensor_b: Path,
    window_seconds: float = 1.0,
) -> TimeErrorReport:
    epochs_a = read_csv_epochs(sensor_a)
    epochs_b = read_csv_epochs(sensor_b)
    if not epochs_a or not epochs_b:
        return TimeErrorReport(sensor_a=sensor_a, sensor_b=sensor_b, window_seconds=window_seconds, windows=[])

    window_us = int(round(window_seconds * 1_000_000))
    start_us = min(epochs_a[0], epochs_b[0])
    end_us = max(epochs_a[-1], epochs_b[-1])
    windows: list[TimeErrorWindow] = []
    cursor = start_us - (start_us % window_us)
    index_a = 0
    index_b = 0
    while cursor <= end_us:
        next_cursor = cursor + window_us
        slice_a: list[int] = []
        slice_b: list[int] = []
        while index_a < len(epochs_a) and epochs_a[index_a] < next_cursor:
            if epochs_a[index_a] >= cursor:
                slice_a.append(epochs_a[index_a])
            index_a += 1
        while index_b < len(epochs_b) and epochs_b[index_b] < next_cursor:
            if epochs_b[index_b] >= cursor:
                slice_b.append(epochs_b[index_b])
            index_b += 1

        nearest_ms = [value / 1000.0 for value in nearest_abs_deltas_us(slice_a, slice_b)]
        same_count = min(len(slice_a), len(slice_b))
        same_index_ms = [
            abs(slice_a[i] - slice_b[i]) / 1000.0
            for i in range(same_count)
        ]
        windows.append(
            TimeErrorWindow(
                start_epoch_us=cursor,
                end_epoch_us=next_cursor,
                sensor_a_count=len(slice_a),
                sensor_b_count=len(slice_b),
                nearest_mean_ms=mean(nearest_ms),
                nearest_max_ms=max(nearest_ms) if nearest_ms else 0.0,
                nearest_rms_ms=rms(nearest_ms),
                nearest_p95_ms=percentile(nearest_ms, 95.0),
                same_index_mean_ms=mean(same_index_ms),
                same_index_max_ms=max(same_index_ms) if same_index_ms else 0.0,
            )
        )
        cursor = next_cursor
    return TimeErrorReport(sensor_a=sensor_a, sensor_b=sensor_b, window_seconds=window_seconds, windows=windows)


def format_epoch_us(epoch_us: int) -> str:
    return datetime.fromtimestamp(epoch_us / 1_000_000).strftime("%Y/%m/%d %H:%M:%S")


def format_time_error_report(report: TimeErrorReport, csv_timebase: str) -> str:
    label = (
        "aligned analysis grid difference"
        if csv_timebase == "aligned"
        else "raw GPS-calibrated INT1 edge time error"
    )
    lines = [
        "[info] sensor time error report "
        f"csv_timebase={csv_timebase} meaning=\"{label}\" "
        f"window_seconds={report.window_seconds:g}",
        f"sensor_a={report.sensor_a}",
        f"sensor_b={report.sensor_b}",
    ]
    if csv_timebase == "aligned":
        lines.append(
            "[info] aligned note: 0 ms means CSV grid aligned, not physical edge synchronization"
        )
    for window in report.windows:
        lines.append(
            "WINDOW {start} count_a={count_a} count_b={count_b} "
            "nearest_mean_ms={nearest_mean:.3f} nearest_max_ms={nearest_max:.3f} "
            "nearest_rms_ms={nearest_rms:.3f} nearest_p95_ms={nearest_p95:.3f} "
            "same_index_mean_ms={same_mean:.3f} same_index_max_ms={same_max:.3f}".format(
                start=format_epoch_us(window.start_epoch_us),
                count_a=window.sensor_a_count,
                count_b=window.sensor_b_count,
                nearest_mean=window.nearest_mean_ms,
                nearest_max=window.nearest_max_ms,
                nearest_rms=window.nearest_rms_ms,
                nearest_p95=window.nearest_p95_ms,
                same_mean=window.same_index_mean_ms,
                same_max=window.same_index_max_ms,
            )
        )
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze one-second timestamp error between two sensor CSV files.")
    parser.add_argument("--sensor-a", type=Path, required=True)
    parser.add_argument("--sensor-b", type=Path, required=True)
    parser.add_argument("--window-seconds", type=float, default=1.0)
    parser.add_argument("--csv-timebase", choices=["raw", "aligned"], default="raw")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    report = analyze_sensor_time_error(args.sensor_a, args.sensor_b, args.window_seconds)
    print(format_time_error_report(report, args.csv_timebase))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
