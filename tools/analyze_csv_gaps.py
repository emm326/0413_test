#!/usr/bin/env python3
"""Compatibility wrapper for CSV timestamp gap analysis."""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from .serial_rx_to_file import (
        DEFAULT_GAP_FACTOR,
        DEFAULT_GAP_SAMPLES_PER_PACKET,
        DEFAULT_GAP_TOP,
        DEFAULT_SAMPLE_RATE_HZ,
        SUPPORTED_SAMPLE_RATES_HZ,
        analyze_csv_file,
        is_supported_sample_rate,
        print_gap_report,
    )
except ImportError:  # pragma: no cover - used when run as a script from tools/
    from serial_rx_to_file import (
        DEFAULT_GAP_FACTOR,
        DEFAULT_GAP_SAMPLES_PER_PACKET,
        DEFAULT_GAP_TOP,
        DEFAULT_SAMPLE_RATE_HZ,
        SUPPORTED_SAMPLE_RATES_HZ,
        analyze_csv_file,
        is_supported_sample_rate,
        print_gap_report,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report timestamp gaps in sensor CSV files."
    )
    parser.add_argument("csv_files", nargs="+", type=Path, help="CSV files to analyze")
    parser.add_argument(
        "--sample-rate-hz",
        type=float,
        default=DEFAULT_SAMPLE_RATE_HZ,
        help="Configured sample rate. IIM actual rate is 1.024x. Default: 100",
    )
    parser.add_argument(
        "--gap-factor",
        type=float,
        default=DEFAULT_GAP_FACTOR,
        help="Flag gaps larger than expected period times this factor. Default: 2",
    )
    parser.add_argument(
        "--samples-per-packet",
        type=int,
        default=DEFAULT_GAP_SAMPLES_PER_PACKET,
        help="Samples in each TX packet for packet-loss estimates. Default: 37",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_GAP_TOP,
        help="Number of largest gaps to print for each file. Default: 5",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.sample_rate_hz <= 0:
        raise SystemExit("--sample-rate-hz must be greater than 0")
    if not is_supported_sample_rate(args.sample_rate_hz):
        allowed = ", ".join(f"{rate:g}" for rate in SUPPORTED_SAMPLE_RATES_HZ)
        raise SystemExit(f"--sample-rate-hz must be one of: {allowed}")
    if args.gap_factor <= 0:
        raise SystemExit("--gap-factor must be greater than 0")
    if args.samples_per_packet <= 0:
        raise SystemExit("--samples-per-packet must be greater than 0")
    if args.top < 0:
        raise SystemExit("--top must be 0 or greater")

    for csv_file in args.csv_files:
        report = analyze_csv_file(
            csv_file,
            sample_rate_hz=args.sample_rate_hz,
            gap_factor=args.gap_factor,
            samples_per_packet=args.samples_per_packet,
        )
        print_gap_report(report, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
