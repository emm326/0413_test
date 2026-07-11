#!/usr/bin/env python3
"""Analyze and linearly align two timestamped acceleration CSV files."""

from __future__ import annotations

import argparse
import csv
import math
import os
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class SensorSample:
    epoch_us: int
    ax: float
    ay: float
    az: float


@dataclass
class InputQuality:
    rows: int
    duplicate_timestamps: int
    non_monotonic_timestamps: int
    gap_count: int
    max_gap_ms: float


@dataclass
class TimeErrorWindow:
    start_epoch_us: int
    end_epoch_us: int
    matched_count: int
    mean_abs_ms: float
    rms_abs_ms: float
    p95_abs_ms: float
    max_abs_ms: float


@dataclass
class TimeErrorReport:
    sensor_a: Path
    sensor_b: Path
    actual_rate_hz: float
    sample_period_ms: float
    max_error_ms: float
    quality_a: InputQuality
    quality_b: InputQuality
    overlap_start_epoch_us: int | None
    overlap_end_epoch_us: int | None
    matched_count: int
    unmatched_a: int
    unmatched_b: int
    signed_mean_ms: float
    mean_abs_ms: float
    median_abs_ms: float
    rms_abs_ms: float
    p95_abs_ms: float
    max_abs_ms: float
    first_signed_ms: float
    last_signed_ms: float
    drift_ppm: float
    passed: bool
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


def read_csv_samples(path: Path) -> list[SensorSample]:
    samples: list[SensorSample] = []
    with path.open("r", newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        required = {"datetime", "ax", "ay", "az"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(f"{path} must contain datetime,ax,ay,az columns")
        for row_number, row in enumerate(reader, start=2):
            try:
                samples.append(
                    SensorSample(
                        epoch_us=parse_csv_datetime(row["datetime"]),
                        ax=float(row["ax"]),
                        ay=float(row["ay"]),
                        az=float(row["az"]),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{path}:{row_number}: invalid sample row") from exc
    return samples


def read_csv_epochs(path: Path) -> list[int]:
    """Backward-compatible helper used by existing callers and tests."""
    return [sample.epoch_us for sample in read_csv_samples(path)]


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


def median(values: list[float]) -> float:
    return percentile(values, 50.0)


def _sanitize_samples(samples: list[SensorSample]) -> list[SensorSample]:
    by_epoch: dict[int, SensorSample] = {}
    for sample in samples:
        by_epoch.setdefault(sample.epoch_us, sample)
    return [by_epoch[epoch] for epoch in sorted(by_epoch)]


def _input_quality(samples: list[SensorSample], sample_period_us: float) -> InputQuality:
    duplicate_count = 0
    non_monotonic_count = 0
    gaps: list[int] = []
    for previous, current in zip(samples, samples[1:]):
        delta = current.epoch_us - previous.epoch_us
        if delta == 0:
            duplicate_count += 1
        elif delta < 0:
            non_monotonic_count += 1
        elif delta > sample_period_us * 1.5:
            gaps.append(delta)
    return InputQuality(
        rows=len(samples),
        duplicate_timestamps=duplicate_count,
        non_monotonic_timestamps=non_monotonic_count,
        gap_count=len(gaps),
        max_gap_ms=(max(gaps) / 1000.0) if gaps else 0.0,
    )


def _ordered_matches(
    samples_a: list[SensorSample],
    samples_b: list[SensorSample],
    overlap_start_us: int,
    overlap_end_us: int,
    max_error_us: float,
) -> list[tuple[int, int, int, int]]:
    """Return monotonic one-to-one matches as (time, delta, index_a, index_b)."""
    i = 0
    j = 0

    matches: list[tuple[int, int, int, int]] = []
    while i < len(samples_a) and j < len(samples_b):
        time_a = samples_a[i].epoch_us
        time_b = samples_b[j].epoch_us
        if time_a > overlap_end_us and time_b > overlap_end_us:
            break
        delta_us = time_b - time_a
        if abs(delta_us) <= max_error_us:
            pair_time_us = (time_a + time_b) // 2
            matches.append((pair_time_us, delta_us, i, j))
            i += 1
            j += 1
        elif time_a < time_b:
            i += 1
        else:
            j += 1
    return matches


def _linear_drift_ppm(matches: list[tuple[int, int, int, int]]) -> float:
    if len(matches) < 2:
        return 0.0
    origin_us = matches[0][0]
    x_seconds = [(item[0] - origin_us) / 1_000_000.0 for item in matches]
    y_us = [float(item[1]) for item in matches]
    mean_x = mean(x_seconds)
    mean_y = mean(y_us)
    denominator = sum((value - mean_x) ** 2 for value in x_seconds)
    if denominator == 0:
        return 0.0
    # A drift of 1 microsecond per second is 1 ppm.
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(x_seconds, y_us)) / denominator


def _build_windows(
    matches: list[tuple[int, int, int, int]],
    window_seconds: float,
) -> list[TimeErrorWindow]:
    if not matches or window_seconds <= 0:
        return []
    window_us = max(1, int(round(window_seconds * 1_000_000)))
    first_time = matches[0][0]
    last_time = matches[-1][0]
    cursor = first_time - (first_time % window_us)
    index = 0
    windows: list[TimeErrorWindow] = []
    while cursor <= last_time:
        end = cursor + window_us
        deltas_ms: list[float] = []
        while index < len(matches) and matches[index][0] < end:
            if matches[index][0] >= cursor:
                deltas_ms.append(abs(matches[index][1]) / 1000.0)
            index += 1
        if deltas_ms:
            windows.append(
                TimeErrorWindow(
                    start_epoch_us=cursor,
                    end_epoch_us=end,
                    matched_count=len(deltas_ms),
                    mean_abs_ms=mean(deltas_ms),
                    rms_abs_ms=rms(deltas_ms),
                    p95_abs_ms=percentile(deltas_ms, 95.0),
                    max_abs_ms=max(deltas_ms),
                )
            )
        cursor = end
    return windows


def analyze_sensor_time_error(
    sensor_a: Path,
    sensor_b: Path,
    window_seconds: float = 1.0,
    actual_rate_hz: float = 51.2,
    max_error_ms: float | None = None,
) -> TimeErrorReport:
    if actual_rate_hz <= 0:
        raise ValueError("actual_rate_hz must be positive")
    sample_period_us = 1_000_000.0 / actual_rate_hz
    sample_period_ms = sample_period_us / 1000.0
    resolved_max_error_ms = sample_period_ms if max_error_ms is None else max_error_ms
    if resolved_max_error_ms <= 0:
        raise ValueError("max_error_ms must be positive")

    raw_a = read_csv_samples(sensor_a)
    raw_b = read_csv_samples(sensor_b)
    quality_a = _input_quality(raw_a, sample_period_us)
    quality_b = _input_quality(raw_b, sample_period_us)
    samples_a = _sanitize_samples(raw_a)
    samples_b = _sanitize_samples(raw_b)

    empty_report = dict(
        sensor_a=sensor_a,
        sensor_b=sensor_b,
        actual_rate_hz=actual_rate_hz,
        sample_period_ms=sample_period_ms,
        max_error_ms=resolved_max_error_ms,
        quality_a=quality_a,
        quality_b=quality_b,
        overlap_start_epoch_us=None,
        overlap_end_epoch_us=None,
        matched_count=0,
        unmatched_a=len(samples_a),
        unmatched_b=len(samples_b),
        signed_mean_ms=0.0,
        mean_abs_ms=0.0,
        median_abs_ms=0.0,
        rms_abs_ms=0.0,
        p95_abs_ms=0.0,
        max_abs_ms=0.0,
        first_signed_ms=0.0,
        last_signed_ms=0.0,
        drift_ppm=0.0,
        passed=False,
        windows=[],
    )
    if not samples_a or not samples_b:
        return TimeErrorReport(**empty_report)

    overlap_start_us = max(samples_a[0].epoch_us, samples_b[0].epoch_us)
    overlap_end_us = min(samples_a[-1].epoch_us, samples_b[-1].epoch_us)
    if overlap_start_us > overlap_end_us:
        return TimeErrorReport(**empty_report)

    matches = _ordered_matches(
        samples_a,
        samples_b,
        overlap_start_us,
        overlap_end_us,
        resolved_max_error_ms * 1000.0,
    )
    signed_ms = [item[1] / 1000.0 for item in matches]
    absolute_ms = [abs(value) for value in signed_ms]
    max_abs_ms = max(absolute_ms) if absolute_ms else 0.0
    return TimeErrorReport(
        sensor_a=sensor_a,
        sensor_b=sensor_b,
        actual_rate_hz=actual_rate_hz,
        sample_period_ms=sample_period_ms,
        max_error_ms=resolved_max_error_ms,
        quality_a=quality_a,
        quality_b=quality_b,
        overlap_start_epoch_us=overlap_start_us,
        overlap_end_epoch_us=overlap_end_us,
        matched_count=len(matches),
        unmatched_a=len(samples_a) - len(matches),
        unmatched_b=len(samples_b) - len(matches),
        signed_mean_ms=mean(signed_ms),
        mean_abs_ms=mean(absolute_ms),
        median_abs_ms=median(absolute_ms),
        rms_abs_ms=rms(absolute_ms),
        p95_abs_ms=percentile(absolute_ms, 95.0),
        max_abs_ms=max_abs_ms,
        first_signed_ms=signed_ms[0] if signed_ms else 0.0,
        last_signed_ms=signed_ms[-1] if signed_ms else 0.0,
        drift_ppm=_linear_drift_ppm(matches),
        passed=bool(matches) and max_abs_ms < resolved_max_error_ms,
        windows=_build_windows(matches, window_seconds),
    )


def format_epoch_us(epoch_us: int) -> str:
    return datetime.fromtimestamp(epoch_us / 1_000_000).strftime("%Y/%m/%d %H:%M:%S")


def format_datetime_4(epoch_us: float) -> str:
    dt = datetime.fromtimestamp(epoch_us / 1_000_000.0)
    return dt.strftime("%Y/%m/%d %H:%M:%S.") + f"{dt.microsecond // 100:04d}"


def format_time_error_report(report: TimeErrorReport, csv_timebase: str = "raw") -> str:
    meaning = (
        "aligned analysis grid difference"
        if csv_timebase == "aligned"
        else "raw GPS-calibrated INT1 edge time error"
    )
    lines = [
        "[info] sensor synchronization report "
        f"csv_timebase={csv_timebase} actual_rate_hz={report.actual_rate_hz:g} "
        f"sample_period_ms={report.sample_period_ms:.5f} threshold_ms={report.max_error_ms:.5f} "
        f'meaning="{meaning}"',
        f"sensor_a={report.sensor_a}",
        f"sensor_b={report.sensor_b}",
        "QUALITY_A rows={rows} duplicate={duplicate} non_monotonic={non_monotonic} "
        "gaps={gaps} max_gap_ms={max_gap:.3f}".format(
            rows=report.quality_a.rows,
            duplicate=report.quality_a.duplicate_timestamps,
            non_monotonic=report.quality_a.non_monotonic_timestamps,
            gaps=report.quality_a.gap_count,
            max_gap=report.quality_a.max_gap_ms,
        ),
        "QUALITY_B rows={rows} duplicate={duplicate} non_monotonic={non_monotonic} "
        "gaps={gaps} max_gap_ms={max_gap:.3f}".format(
            rows=report.quality_b.rows,
            duplicate=report.quality_b.duplicate_timestamps,
            non_monotonic=report.quality_b.non_monotonic_timestamps,
            gaps=report.quality_b.gap_count,
            max_gap=report.quality_b.max_gap_ms,
        ),
        "SUMMARY matched={matched} unmatched_a={unmatched_a} unmatched_b={unmatched_b} "
        "signed_mean_ms={signed_mean:.3f} mean_abs_ms={mean_abs:.3f} "
        "median_abs_ms={median_abs:.3f} rms_abs_ms={rms_abs:.3f} "
        "p95_abs_ms={p95_abs:.3f} max_abs_ms={max_abs:.3f} "
        "first_signed_ms={first:.3f} last_signed_ms={last:.3f} "
        "drift_ppm={drift:.3f} result={result}".format(
            matched=report.matched_count,
            unmatched_a=report.unmatched_a,
            unmatched_b=report.unmatched_b,
            signed_mean=report.signed_mean_ms,
            mean_abs=report.mean_abs_ms,
            median_abs=report.median_abs_ms,
            rms_abs=report.rms_abs_ms,
            p95_abs=report.p95_abs_ms,
            max_abs=report.max_abs_ms,
            first=report.first_signed_ms,
            last=report.last_signed_ms,
            drift=report.drift_ppm,
            result="PASS" if report.passed else "FAIL",
        ),
        "[info] PPS is treated as the accurate UTC reference; reported errors compare CSV sample grids.",
        "[info] A common aligned CSV grid does not prove physical INT1 edge synchronization.",
    ]
    for window in report.windows:
        lines.append(
            "WINDOW {start} matched={matched} mean_abs_ms={mean_abs:.3f} "
            "rms_abs_ms={rms_abs:.3f} p95_abs_ms={p95_abs:.3f} max_abs_ms={max_abs:.3f}".format(
                start=format_epoch_us(window.start_epoch_us),
                matched=window.matched_count,
                mean_abs=window.mean_abs_ms,
                rms_abs=window.rms_abs_ms,
                p95_abs=window.p95_abs_ms,
                max_abs=window.max_abs_ms,
            )
        )
    return "\n".join(lines)


def _interpolate_sample(
    samples: list[SensorSample],
    epochs: list[int],
    target_epoch_us: float,
    max_gap_us: float,
) -> tuple[float, float, float] | None:
    right = bisect_right(epochs, target_epoch_us)
    if right > 0 and epochs[right - 1] == target_epoch_us:
        sample = samples[right - 1]
        return sample.ax, sample.ay, sample.az
    if right == 0 or right >= len(samples):
        return None
    left_sample = samples[right - 1]
    right_sample = samples[right]
    gap_us = right_sample.epoch_us - left_sample.epoch_us
    if gap_us <= 0 or gap_us > max_gap_us:
        return None
    fraction = (target_epoch_us - left_sample.epoch_us) / gap_us
    return (
        left_sample.ax + (right_sample.ax - left_sample.ax) * fraction,
        left_sample.ay + (right_sample.ay - left_sample.ay) * fraction,
        left_sample.az + (right_sample.az - left_sample.az) * fraction,
    )


def write_aligned_common_grid(
    sensor_a: Path,
    sensor_b: Path,
    output_path: Path,
    actual_rate_hz: float = 51.2,
) -> int:
    if actual_rate_hz <= 0:
        raise ValueError("actual_rate_hz must be positive")
    samples_a = _sanitize_samples(read_csv_samples(sensor_a))
    samples_b = _sanitize_samples(read_csv_samples(sensor_b))
    if not samples_a or not samples_b:
        raise ValueError("both sensor CSV files must contain samples")

    overlap_start_us = max(samples_a[0].epoch_us, samples_b[0].epoch_us)
    overlap_end_us = min(samples_a[-1].epoch_us, samples_b[-1].epoch_us)
    if overlap_start_us > overlap_end_us:
        raise ValueError("sensor CSV files do not overlap in time")

    period_us = 1_000_000.0 / actual_rate_hz
    second_origin_us = math.floor(overlap_start_us / 1_000_000.0) * 1_000_000.0
    first_index = math.ceil((overlap_start_us - second_origin_us) / period_us - 1e-12)
    target_epoch_us = second_origin_us + first_index * period_us
    max_gap_us = period_us * 1.5
    epochs_a = [sample.epoch_us for sample in samples_a]
    epochs_b = [sample.epoch_us for sample in samples_b]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    with output_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "datetime",
                "sensor1001_ax",
                "sensor1001_ay",
                "sensor1001_az",
                "sensor1002_ax",
                "sensor1002_ay",
                "sensor1002_az",
            ]
        )
        while target_epoch_us <= overlap_end_us + 1e-6:
            value_a = _interpolate_sample(samples_a, epochs_a, target_epoch_us, max_gap_us)
            value_b = _interpolate_sample(samples_b, epochs_b, target_epoch_us, max_gap_us)
            if value_a is not None and value_b is not None:
                writer.writerow(
                    [
                        format_datetime_4(target_epoch_us),
                        *(f"{value:.6f}" for value in value_a),
                        *(f"{value:.6f}" for value in value_b),
                    ]
                )
                rows_written += 1
            target_epoch_us += period_us
    return rows_written


def default_output_dir(sensor_a: Path, sensor_b: Path) -> Path:
    common = Path(os.path.commonpath([sensor_a.resolve(), sensor_b.resolve()]))
    return common if common.is_dir() else common.parent


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze two timestamped sensor CSV files and optionally create a common UTC grid."
    )
    parser.add_argument("--sensor-a", type=Path, required=True)
    parser.add_argument("--sensor-b", type=Path, required=True)
    parser.add_argument("--window-seconds", type=float, default=1.0)
    parser.add_argument("--actual-rate-hz", type=float, default=51.2)
    parser.add_argument("--max-error-ms", type=float)
    parser.add_argument("--csv-timebase", choices=["raw", "aligned"], default="raw")
    parser.add_argument("--write-aligned", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    report = analyze_sensor_time_error(
        args.sensor_a,
        args.sensor_b,
        window_seconds=args.window_seconds,
        actual_rate_hz=args.actual_rate_hz,
        max_error_ms=args.max_error_ms,
    )
    print(format_time_error_report(report, csv_timebase=args.csv_timebase))
    if args.write_aligned:
        output_dir = args.output_dir or default_output_dir(args.sensor_a, args.sensor_b)
        output_path = output_dir / "aligned_common_grid.csv"
        rows = write_aligned_common_grid(
            args.sensor_a,
            args.sensor_b,
            output_path,
            actual_rate_hz=args.actual_rate_hz,
        )
        print(f"[info] aligned_csv={output_path} rows={rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
