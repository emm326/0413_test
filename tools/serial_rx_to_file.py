#!/usr/bin/env python3
"""
将接收端串口中的:
timestamp_ms,ax_raw,ay_raw,az_raw

转换并保存为:
2026/3/13 1:27:28.0320,0.000000,-0.000033,0.000004

说明:
1. 不修改单片机固件, 只在电脑端做串口转存。
2. 默认把原始值按 ADXL345 full-resolution 的 3.9 mg/LSB 转成 g。
3. 发送端时间基准是 millis(), 所以绝对时间需要用“电脑当前时间”或用户指定的起始时间做锚定。
4. 由于输入时间只有毫秒级, 输出里的第 4 位小数会固定为 0。
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
        "未找到 pyserial。建议直接使用 PlatformIO 自带 Python 运行本脚本:\n"
        r"C:\Users\26223\.platformio\penv\Scripts\python.exe tools\serial_rx_to_file.py --port COM6 --output rx.csv"
    ) from exc


DATA_LINE_RE = re.compile(r"^\s*(\d+),(-?\d+),(-?\d+),(-?\d+)\s*$")
DEFAULT_SCALE_G = 0.0039


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


def parse_args() -> CaptureConfig:
    parser = argparse.ArgumentParser(
        description="读取接收端串口数据并转换为绝对时间 + 浮点值后写入 CSV/TXT。"
    )
    parser.add_argument("--port", required=True, help="串口号，例如 COM6")
    parser.add_argument("--baud", type=int, default=115200, help="串口波特率，默认 115200")
    parser.add_argument(
        "--output",
        required=True,
        help="输出文件路径，例如 rx.csv 或 rx.txt",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=DEFAULT_SCALE_G,
        help="原始值乘法缩放系数，默认 0.0039（转换为 g）",
    )
    parser.add_argument(
        "--base-time",
        default="",
        help=(
            "可选，指定第一条样本对应的绝对时间。"
            "格式示例: 2026/3/13 1:27:28.0320"
        ),
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="追加写入文件，而不是覆盖",
    )
    parser.add_argument(
        "--echo-data",
        action="store_true",
        help="在控制台同步打印转换后的数据行",
    )
    parser.add_argument(
        "--with-header",
        action="store_true",
        help="在文件首行写入表头 datetime,ax,ay,az",
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
    )


def parse_user_time(text: str) -> datetime:
    text = text.strip()
    for fmt in ("%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise SystemExit(
        "base-time 格式错误。示例: 2026/3/13 1:27:28.0320"
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


def capture(config: CaptureConfig) -> int:
    first_sample_ms: int | None = None
    first_sample_time: datetime | None = config.base_time

    try:
        ser = serial.Serial(config.port, config.baud, timeout=1)
    except SerialException as exc:
        print(f"[error] 串口打开失败: {exc}", file=sys.stderr)
        return 1

    with ser, open_output(config.output, config.append) as fp:
        writer = csv.writer(fp)

        if config.write_header and (not config.append or config.output.stat().st_size == 0):
            writer.writerow(["datetime", "ax", "ay", "az"])
            fp.flush()

        print(f"[info] 正在监听串口 {config.port} @ {config.baud}")
        print(f"[info] 输出文件: {config.output}")
        print(f"[info] 缩放系数: {config.scale}")
        if config.base_time is None:
            print("[info] 第一条数据到达时，将使用电脑当前时间作为起始时间")
        else:
            print(f"[info] 第一条数据的绝对时间固定为: {format_datetime_4(config.base_time)}")

        try:
            while True:
                raw_line = ser.readline()
                if not raw_line:
                    continue

                try:
                    line = raw_line.decode("utf-8", errors="ignore").strip()
                except UnicodeDecodeError:
                    continue

                match = DATA_LINE_RE.match(line)
                if not match:
                    # 非数据行只做透传提示，便于观察 RX 状态
                    print(f"[serial] {line}")
                    continue

                sample_ms = int(match.group(1))
                ax_raw = int(match.group(2))
                ay_raw = int(match.group(3))
                az_raw = int(match.group(4))

                if first_sample_ms is None:
                    first_sample_ms = sample_ms
                    if first_sample_time is None:
                        first_sample_time = datetime.now()

                assert first_sample_time is not None
                delta_ms = sample_ms - first_sample_ms
                sample_time = first_sample_time + timedelta(milliseconds=delta_ms)

                row = [
                    format_datetime_4(sample_time),
                    f"{convert_value(ax_raw, config.scale):.6f}",
                    f"{convert_value(ay_raw, config.scale):.6f}",
                    f"{convert_value(az_raw, config.scale):.6f}",
                ]
                writer.writerow(row)
                fp.flush()

                if config.echo_data:
                    print(",".join(row))

        except KeyboardInterrupt:
            print("\n[info] 用户终止采集")
            return 0


def main() -> int:
    config = parse_args()
    return capture(config)


if __name__ == "__main__":
    raise SystemExit(main())
