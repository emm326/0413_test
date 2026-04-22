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
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

try:
    import serial
    from serial import SerialException
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "pyserial is required.\n"
        r"Try: C:\Users\26223\.platformio\penv\Scripts\python.exe "
        r"tools\serial_rx_to_file.py --port COM6 --output rx.csv"
    ) from exc


MULTI_DEVICE_RE = re.compile(r"^\s*(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
LEGACY_RE = re.compile(r"^\s*(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
DEFAULT_SCALE_G = 0.0039
DEFAULT_DEVICE_ID = "1001"


@dataclass
class CaptureConfig:
    port: str
    baud: int
    output: Path
    scale: float
    base_time: datetime | None
    append: bool
    echo_data: bool
    write_header: bool
    include_id: bool
    legacy_device_id: str


@dataclass
class ParsedSample:
    device_id: str
    sensor_ms: int
    ax_raw: int
    ay_raw: int
    az_raw: int
    from_legacy_line: bool


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


def parse_args() -> CaptureConfig:
    parser = argparse.ArgumentParser(
        description=(
            "Read RX serial samples, restore wall-clock time and save CSV/TXT rows."
        )
    )
    parser.add_argument("--port", required=True, help="Serial port, for example COM6")
    parser.add_argument(
        "--baud", type=int, default=115200, help="Serial baud rate, default 115200"
    )
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument(
        "--scale",
        type=float,
        default=DEFAULT_SCALE_G,
        help="Raw to g scale factor. Default: 0.0039",
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
    args = parser.parse_args()

    base_time = parse_user_time(args.base_time) if args.base_time else None
    return CaptureConfig(
        port=args.port,
        baud=args.baud,
        output=Path(args.output),
        scale=args.scale,
        base_time=base_time,
        append=args.append,
        echo_data=args.echo_data,
        write_header=args.with_header,
        include_id=args.include_id,
        legacy_device_id=str(args.device_id),
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


def convert_value(raw: int, scale: float) -> float:
    return raw * scale


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
        return ParsedSample(
            device_id=match.group(1),
            sensor_ms=int(match.group(2)),
            ax_raw=int(match.group(3)),
            ay_raw=int(match.group(4)),
            az_raw=int(match.group(5)),
            from_legacy_line=False,
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
        )

    return None


def build_header(include_id: bool) -> list[str]:
    if include_id:
        return ["id", "datetime", "ax", "ay", "az"]
    return ["datetime", "ax", "ay", "az"]


def capture(config: CaptureConfig) -> int:
    anchors: dict[str, TimeAnchor] = {}
    omitted_id_warned = False

    try:
        ser = serial.Serial(config.port, config.baud, timeout=1)
    except SerialException as exc:
        print(f"[error] Serial open failed: {exc}", file=sys.stderr)
        return 1

    with ser, open_output(config.output, config.append) as fp:
        writer = csv.writer(fp)

        if config.write_header and (not config.append or config.output.stat().st_size == 0):
            writer.writerow(build_header(config.include_id))
            fp.flush()

        print(f"[info] Listening on serial {config.port} @ {config.baud}")
        print(f"[info] Output file: {config.output}")
        print(f"[info] Scale: {config.scale}")
        print(f"[info] Include id column: {config.include_id}")
        if config.base_time is None:
            print("[info] The first sample from each device will anchor to current PC time")
        else:
            print(
                "[info] The first sample from each device will anchor to: "
                f"{format_datetime_4(config.base_time)}"
            )

        try:
            while True:
                raw_line = ser.readline()
                if not raw_line:
                    continue

                line = raw_line.decode("utf-8", errors="ignore").strip()
                sample = parse_serial_sample(line, config.legacy_device_id)
                if sample is None:
                    if line:
                        print(f"[serial] {line}")
                    continue

                anchor = anchors.setdefault(
                    sample.device_id, TimeAnchor(base_time=config.base_time)
                )
                sample_time, reanchored = anchor.to_datetime(sample.sensor_ms)
                if reanchored:
                    print(
                        f"[info] Time anchor set for device {sample.device_id}: "
                        f"{format_datetime_4(sample_time)}"
                    )

                if not config.include_id and not sample.from_legacy_line and not omitted_id_warned:
                    print(
                        "[warn] Device id is present on serial input but omitted from file output. "
                        "Use --include-id if multiple transmitters share one gateway."
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

                writer.writerow(row)
                fp.flush()

                if config.echo_data:
                    print(",".join(row))

        except KeyboardInterrupt:
            print("\n[info] Capture stopped by user")
            return 0


def main() -> int:
    config = parse_args()
    return capture(config)


if __name__ == "__main__":
    raise SystemExit(main())
