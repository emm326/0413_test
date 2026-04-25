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
import os
import re
import subprocess
import sys
import threading
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
DEFAULT_SCALE_G = 0.0039
DEFAULT_DEVICE_ID = "1001"
DEFAULT_ROTATE_MINUTES = 0
TK_RELAUNCH_ENV = "SERIAL_RX_TO_FILE_TK_RELAUNCHED"


@dataclass
class CaptureConfig:
    ports: list[str]
    baud: int
    output: Path
    rotate_minutes: int
    scale: float
    base_time: datetime | None
    append: bool
    echo_data: bool
    write_header: bool
    include_id: bool
    legacy_device_id: str


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
    parser.add_argument("--port", help="Single serial port, for example COM6")
    parser.add_argument(
        "--ports",
        help="Multiple serial ports, for example COM6,COM7,COM8",
    )
    parser.add_argument(
        "--baud", type=int, default=115200, help="Serial baud rate, default 115200"
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

    if args.gui or len(sys.argv) == 1:
        return None

    port_text = args.ports or args.port or ""
    ports = parse_ports_text(port_text)
    if not ports:
        parser.error("--port or --ports is required unless --gui is used")
    if not args.output:
        parser.error("--output is required unless --gui is used")

    base_time = parse_user_time(args.base_time) if args.base_time else None
    if args.rotate_minutes < 0:
        raise SystemExit("--rotate-minutes must be 0 or greater")

    return CaptureConfig(
        ports=ports,
        baud=args.baud,
        output=Path(args.output),
        rotate_minutes=args.rotate_minutes,
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
    root.geometry("780x560")
    root.minsize(720, 520)

    log_queue: "queue.Queue[object]"
    import queue

    log_queue = queue.Queue()
    worker_thread: threading.Thread | None = None
    stop_event: threading.Event | None = None

    port_var = tk.StringVar()
    baud_var = tk.StringVar(value="115200")
    output_var = tk.StringVar(value=str(Path.cwd() / "data"))
    rotate_var = tk.StringVar(value="30")
    scale_var = tk.StringVar(value=str(DEFAULT_SCALE_G))
    base_time_var = tk.StringVar()
    device_id_var = tk.StringVar(value=DEFAULT_DEVICE_ID)
    header_var = tk.BooleanVar(value=True)
    include_id_var = tk.BooleanVar(value=False)
    append_var = tk.BooleanVar(value=False)
    echo_var = tk.BooleanVar(value=False)

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

    def build_config_from_ui() -> CaptureConfig | None:
        try:
            baud = int(baud_var.get().strip())
            rotate_minutes = int(rotate_var.get().strip())
            scale = float(scale_var.get().strip())
        except ValueError as exc:
            messagebox.showerror("Invalid option", f"Numeric option is invalid: {exc}")
            return None

        if rotate_minutes < 0:
            messagebox.showerror("Invalid option", "Rotation minutes must be 0 or greater")
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
            ports=ports,
            baud=baud,
            output=output_path,
            rotate_minutes=rotate_minutes,
            scale=scale,
            base_time=base_time,
            append=append_var.get(),
            echo_data=echo_var.get(),
            write_header=header_var.get(),
            include_id=include_id_var.get(),
            legacy_device_id=device_id_var.get().strip() or DEFAULT_DEVICE_ID,
        )

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

    ttk.Label(form, text="Scale").grid(row=2, column=2, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=scale_var, width=12).grid(row=2, column=3, sticky="ew", pady=4)

    ttk.Label(form, text="Legacy ID").grid(row=2, column=4, sticky="w", padx=(8, 8), pady=4)
    ttk.Entry(form, textvariable=device_id_var, width=10).grid(row=2, column=5, sticky="ew", pady=4)

    ttk.Label(form, text="Base Time").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
    ttk.Entry(form, textvariable=base_time_var).grid(row=3, column=1, columnspan=5, sticky="ew", pady=4)

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
    return capture(config)


if __name__ == "__main__":
    raise SystemExit(main())
