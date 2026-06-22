#!/usr/bin/env python3
"""
Capture two TX USB serial streams and compare local sampling capacity.

Expected TX USB test firmware lines:
  TXH,dev_id,seq,t0_offset_us,fs_x10,count
  dev_id,epoch_us,ax_raw,ay_raw,az_raw
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
from dataclasses import dataclass
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
TX_STATS_RE = re.compile(r"^TXS,(\d+),(\d+),(\d+),(\d+),(\d+),(\d+)(?:,(\d+),([01]))?$")
SAMPLE_RE = re.compile(r"^(\d+),(\d+),(-?\d+),(-?\d+),(-?\d+)$")

DEFAULT_BAUD = 460800
DEFAULT_SCALE_G = 0.0001220703125
SUPPORTED_SAMPLE_RATES_HZ = (50.0, 100.0, 200.0, 400.0)


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

    def record_sample(self, line: TxUsbLine) -> None:
        if line.device_id != self.expected_device_id:
            self.bad_device_lines += 1
            return
        if line.epoch_us is None:
            self.parse_errors += 1
            return
        if self.first_epoch_us is None:
            self.first_epoch_us = line.epoch_us
        if self.last_sample_epoch_us is not None:
            gap_us = line.epoch_us - self.last_sample_epoch_us
            if gap_us > self.expected_interval_us() * 2:
                self.gap_count += 1
                self.max_gap_us = max(self.max_gap_us, gap_us)
        self.last_sample_epoch_us = line.epoch_us
        self.last_epoch_us = line.epoch_us
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

    def capture_ratio(self, duration_seconds: float) -> float:
        expected_rows = self.expected_rows(duration_seconds)
        if expected_rows <= 0:
            return 0.0
        return self.rows / expected_rows

    def expected_rows(self, duration_seconds: float) -> int:
        return int(round(self.sample_rate_hz * 1.024 * duration_seconds))


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


def write_sample_csv(
    fp: TextIO,
    include_header: bool,
    device_id: str,
    epoch_us: int,
    ax_raw: int,
    ay_raw: int,
    az_raw: int,
    scale: float,
) -> None:
    writer = csv.writer(fp)
    if include_header:
        writer.writerow(["datetime", "ax", "ay", "az"])
    writer.writerow(
        [
            format_datetime_4(epoch_us_to_datetime(epoch_us)),
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


def serial_worker(
    port: str,
    device_id: str,
    config: argparse.Namespace,
    start_epoch_us: int,
    stop_event: threading.Event,
    stats: PortStats,
    log: Callable[[str], None] | None = None,
) -> None:
    csv_path, csv_fp = open_sensor_csv(config.output, device_id)
    wrote_header = False
    try:
        with serial.Serial(port, config.baud, timeout=0.2) as ser:
            time.sleep(config.serial_startup_delay_seconds)
            command = (
                f"START_AT {start_epoch_us} {config.sample_rate_hz:g} "
                f"{int(config.start_delay_seconds * 1000)}\n"
            )
            ser.write(command.encode("ascii"))
            ser.flush()
            emit(f"[info] {port}: sent {command.strip()} to dev {device_id}", log)
            emit(f"[info] {port}: writing {csv_path}", log)

            while not stop_event.is_set():
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
                        emit(f"[{port} serial] {line}", log)
                    continue
                if parsed.kind == "header":
                    stats.record_header(parsed)
                elif parsed.kind == "stats":
                    stats.record_stats(parsed)
                elif parsed.kind == "sample":
                    stats.record_sample(parsed)
                    write_sample_csv(
                        csv_fp,
                        include_header=not wrote_header,
                        device_id=parsed.device_id,
                        epoch_us=parsed.epoch_us or 0,
                        ax_raw=parsed.ax_raw or 0,
                        ay_raw=parsed.ay_raw or 0,
                        az_raw=parsed.az_raw or 0,
                        scale=config.scale,
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


def format_report_lines(stats_items: list[PortStats], duration_seconds: float) -> list[str]:
    lines = ["[info] TX USB capacity report"]
    for stats in stats_items:
        ratio = stats.capture_ratio(duration_seconds) * 100.0
        lines.append(
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
    return lines


def print_report(stats_items: list[PortStats], duration_seconds: float) -> None:
    for line in format_report_lines(stats_items, duration_seconds):
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
    parser.add_argument("--ports", help="Comma-separated ports, e.g. COM7,COM8")
    parser.add_argument("--device-ids", help="Comma-separated ids, e.g. 1001,1002")
    parser.add_argument("--baud", type=int, default=DEFAULT_BAUD)
    parser.add_argument("--sample-rate-hz", type=float, default=50.0)
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--start-delay-seconds", type=float, default=5.0)
    parser.add_argument("--samples-per-packet", type=int, default=24)
    parser.add_argument("--serial-startup-delay-seconds", type=float, default=2.0)
    parser.add_argument("--scale", type=float, default=DEFAULT_SCALE_G)
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
    if args.duration_seconds <= 0 or args.start_delay_seconds <= 0:
        raise SystemExit("--duration-seconds and --start-delay-seconds must be positive")
    if args.samples_per_packet <= 0:
        raise SystemExit("--samples-per-packet must be positive")
    return args


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_arg_parser()
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)
    if args.gui or not argv:
        args.gui = True
        return args
    return validate_capture_args(args)


def run_capture(
    config: argparse.Namespace,
    log: Callable[[str], None] | None = None,
    external_stop_event: threading.Event | None = None,
) -> tuple[int, list[PortStats]]:
    start_epoch_us = int((time.time() + config.start_delay_seconds) * 1_000_000)
    stop_event = external_stop_event or threading.Event()
    stats_items = [
        PortStats(port=port, expected_device_id=device_id, sample_rate_hz=config.sample_rate_hz)
        for port, device_id in zip(config.ports, config.device_ids)
    ]

    emit(
        f"[info] Starting TX USB capacity test ports={','.join(config.ports)} "
        f"sample_rate={config.sample_rate_hz:g}Hz duration={config.duration_seconds:g}s "
        f"samples_per_packet={config.samples_per_packet}",
        log,
    )
    threads = [
        threading.Thread(
            target=serial_worker,
            args=(stats.port, stats.expected_device_id, config, start_epoch_us, stop_event, stats, log),
            name=f"tx-usb-{stats.port}",
            daemon=True,
        )
        for stats in stats_items
    ]
    for thread in threads:
        thread.start()

    stop_after = time.monotonic() + config.start_delay_seconds + config.duration_seconds
    try:
        while time.monotonic() < stop_after and not stop_event.is_set():
            time.sleep(0.2)
    except KeyboardInterrupt:
        emit("[info] Interrupted, stopping capture", log)
    finally:
        stop_event.set()

    for thread in threads:
        thread.join(timeout=3.0)
    for line in format_report_lines(stats_items, config.duration_seconds):
        emit(line, log)

    failed = any(
        stats.serial_errors > 0 or stats.rows == 0 or not math.isfinite(stats.capture_ratio(config.duration_seconds))
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

    ports_var = tk.StringVar(value="COM7,COM8")
    device_ids_var = tk.StringVar(value="1001,1002")
    baud_var = tk.StringVar(value=str(config.baud))
    sample_rate_var = tk.StringVar(value=str(config.sample_rate_hz))
    duration_var = tk.StringVar(value=str(config.duration_seconds))
    start_delay_var = tk.StringVar(value=str(config.start_delay_seconds))
    samples_per_packet_var = tk.StringVar(value=str(config.samples_per_packet))
    serial_delay_var = tk.StringVar(value=str(config.serial_startup_delay_seconds))
    scale_var = tk.StringVar(value=str(config.scale))
    output_var = tk.StringVar(value=str(config.output))
    status_var = tk.StringVar(value="Idle")

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
            duration_seconds=float(duration_var.get().strip()),
            start_delay_seconds=float(start_delay_var.get().strip()),
            samples_per_packet=int(samples_per_packet_var.get().strip()),
            serial_startup_delay_seconds=float(serial_delay_var.get().strip()),
            scale=float(scale_var.get().strip()),
            output=Path(output_var.get().strip() or "data_tx_usb"),
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
    ttk.Label(form, text="Duration s").grid(row=1, column=2, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=duration_var, width=10).grid(row=1, column=3, sticky="ew", pady=4)
    ttk.Label(form, text="Start delay s").grid(row=1, column=4, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=start_delay_var, width=10).grid(row=1, column=5, sticky="ew", pady=4)
    ttk.Label(form, text="Packet samples").grid(row=1, column=6, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=samples_per_packet_var, width=10).grid(row=1, column=7, sticky="ew", pady=4)

    ttk.Label(form, text="Serial delay s").grid(row=2, column=0, sticky="w", pady=4)
    ttk.Entry(form, textvariable=serial_delay_var, width=10).grid(row=2, column=1, sticky="ew", pady=4)
    ttk.Label(form, text="Scale").grid(row=2, column=2, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=scale_var, width=14).grid(row=2, column=3, sticky="ew", pady=4)
    ttk.Label(form, text="Output").grid(row=2, column=4, sticky="w", padx=(16, 0), pady=4)
    ttk.Entry(form, textvariable=output_var).grid(row=2, column=5, columnspan=2, sticky="ew", pady=4)
    ttk.Button(form, text="Browse", command=browse_output).grid(row=2, column=7, sticky="ew", padx=(8, 0), pady=4)

    start_button = ttk.Button(form, text="Start", command=start_capture)
    start_button.grid(row=3, column=0, sticky="ew", pady=(10, 0))
    stop_button = ttk.Button(form, text="Stop", command=stop_capture, state="disabled")
    stop_button.grid(row=3, column=1, sticky="ew", padx=(8, 0), pady=(10, 0))
    ttk.Label(form, textvariable=status_var).grid(row=3, column=2, columnspan=2, sticky="w", padx=(16, 0), pady=(10, 0))

    log_text = scrolledtext.ScrolledText(root, state="disabled", wrap="word", height=24)
    log_text.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))

    poll_log_queue()
    refresh_ports()
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
