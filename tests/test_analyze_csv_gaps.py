import csv
import tempfile
import unittest
from pathlib import Path

from tools.serial_rx_to_file import (
    DEFAULT_GAP_SAMPLES_PER_PACKET,
    DEFAULT_SERIAL_BAUD,
    analyze_csv_file,
    calc_p2p_packet_size_bytes,
    format_gap_report_lines,
    format_range_scale_text,
    invalid_epoch_sample_reason,
    parse_args,
    parse_gap_analysis_options,
    parse_gap_csv_paths_text,
    scale_for_range_g,
    send_start_at_command,
)


class AnalyzeCsvGapsTest(unittest.TestCase):
    def test_reports_gap_counts_and_missing_samples_for_iim_100hz(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sensor1001.csv"
            with path.open("w", newline="", encoding="utf-8") as fp:
                writer = csv.writer(fp)
                writer.writerow(["datetime", "ax", "ay", "az"])
                writer.writerow(["2026/6/20 19:47:45.6955", "0", "0", "0"])
                writer.writerow(["2026/6/20 19:47:45.7053", "0", "0", "0"])
                writer.writerow(["2026/6/20 19:47:48.4495", "0", "0", "0"])

            result = analyze_csv_file(path, sample_rate_hz=100.0, gap_factor=2.0)

        self.assertEqual(result.rows, 3)
        self.assertEqual(result.gap_count, 1)
        self.assertAlmostEqual(result.max_gap_ms, 2744.2, places=1)
        self.assertEqual(result.max_gap_missing_samples, 280)
        self.assertEqual(result.max_gap_missing_packets, 12)
        self.assertEqual(result.max_gap_from_line, 3)
        self.assertEqual(result.max_gap_to_line, 4)

    def test_gap_default_matches_platformio_p2p_packet_config(self):
        project_root = Path(__file__).resolve().parents[1]
        platformio_text = (project_root / "platformio.ini").read_text(encoding="utf-8")
        p2p_start = platformio_text.index("p2p_common_build_flags =")
        p2p_end = platformio_text.index("p2p_tx_build_flags =", p2p_start)
        p2p_flags = platformio_text[p2p_start:p2p_end]

        self.assertEqual(DEFAULT_GAP_SAMPLES_PER_PACKET, 24)
        self.assertIn(
            f"-D APP_MAX_SAMPLES_PER_PACKET_VALUE={DEFAULT_GAP_SAMPLES_PER_PACKET}",
            p2p_flags,
        )
        self.assertEqual(calc_p2p_packet_size_bytes(DEFAULT_GAP_SAMPLES_PER_PACKET), 162)

    def test_serial_rx_cli_accepts_analyze_gap_mode_without_serial_port(self):
        original_argv = list(__import__("sys").argv)
        try:
            __import__("sys").argv = [
                "serial_rx_to_file.py",
                "--analyze-gaps",
                "sensor1001.csv",
                "sensor1002.csv",
                "--sample-rate-hz",
                "50",
                "--samples-per-packet",
                "37",
                "--top-gaps",
                "3",
            ]

            config = parse_args()
        finally:
            __import__("sys").argv = original_argv

        self.assertEqual(config.mode, "analyze_gaps")
        self.assertEqual(
            [str(path) for path in config.gap_csv_files],
            ["sensor1001.csv", "sensor1002.csv"],
        )
        self.assertEqual(config.sample_rate_hz, 50.0)
        self.assertEqual(config.gap_samples_per_packet, 37)
        self.assertEqual(config.gap_top, 3)

    def test_serial_rx_cli_defaults_to_high_speed_baud(self):
        original_argv = list(__import__("sys").argv)
        try:
            __import__("sys").argv = [
                "serial_rx_to_file.py",
                "--port",
                "COM6",
                "--output",
                "data",
            ]

            config = parse_args()
        finally:
            __import__("sys").argv = original_argv

        self.assertEqual(DEFAULT_SERIAL_BAUD, 460800)
        self.assertEqual(config.baud, DEFAULT_SERIAL_BAUD)

    def test_serial_rx_range_g_sets_default_scale(self):
        original_argv = list(__import__("sys").argv)
        try:
            __import__("sys").argv = [
                "serial_rx_to_file.py",
                "--port",
                "COM6",
                "--output",
                "data",
                "--range-g",
                "8",
            ]

            config = parse_args()
        finally:
            __import__("sys").argv = original_argv

        self.assertEqual(config.range_g, 8)
        self.assertAlmostEqual(config.scale, 8 / 32768.0)
        self.assertAlmostEqual(scale_for_range_g(16), 16 / 32768.0)

    def test_range_scale_text_lists_gui_conversion_table(self):
        text = format_range_scale_text(16)

        self.assertIn("Current +/-16g = 0.000488281250 g/count", text)
        self.assertIn("+/-4g: 0.000122070312 g/count", text)
        self.assertIn("+/-8g: 0.000244140625 g/count", text)

    def test_serial_rx_start_at_command_includes_range_g(self):
        class FakeSerial:
            def __init__(self):
                self.payload = b""

            def write(self, payload):
                self.payload += payload

            def flush(self):
                pass

        config = __import__("types").SimpleNamespace(
            serial_startup_delay_seconds=0.0,
            start_delay_seconds=5.0,
            sample_rate_hz=50.0,
            range_g=16,
            start_epoch_us=None,
        )
        ser = FakeSerial()

        send_start_at_command(ser, "COM6", config, log=lambda _message: None)

        tokens = ser.payload.decode("ascii").strip().split()
        self.assertEqual(tokens[0], "START_AT")
        self.assertEqual(tokens[2:], ["50", "5000", "16"])

    def test_epoch_guard_rejects_first_packet_timestamp_underflow(self):
        start_epoch_us = 1_782_189_195_984_817
        now_epoch_us = start_epoch_us + 1_000_000
        underflowed_first_packet_us = start_epoch_us + 4_294_858_966

        reason = invalid_epoch_sample_reason(
            underflowed_first_packet_us,
            start_epoch_us=start_epoch_us,
            now_epoch_us=now_epoch_us,
        )

        self.assertIn("future", reason)

    def test_epoch_guard_accepts_long_capture_when_pc_time_has_advanced(self):
        start_epoch_us = 1_782_189_195_984_817
        ten_minutes_later_us = start_epoch_us + 600_000_000

        reason = invalid_epoch_sample_reason(
            ten_minutes_later_us,
            start_epoch_us=start_epoch_us,
            now_epoch_us=ten_minutes_later_us + 1_000_000,
        )

        self.assertIsNone(reason)

    def test_gap_analysis_options_parse_independent_sample_rate(self):
        sample_rate_hz, samples_per_packet, top_gaps = parse_gap_analysis_options(
            "50",
            "37",
            "10",
        )

        self.assertEqual(sample_rate_hz, 50.0)
        self.assertEqual(samples_per_packet, 37)
        self.assertEqual(top_gaps, 10)

    def test_formats_gap_report_lines_for_gui_log(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sensor1001.csv"
            with path.open("w", newline="", encoding="utf-8") as fp:
                writer = csv.writer(fp)
                writer.writerow(["datetime", "ax", "ay", "az"])
                writer.writerow(["2026/6/20 19:47:45.6955", "0", "0", "0"])
                writer.writerow(["2026/6/20 19:47:48.4495", "0", "0", "0"])

            report = analyze_csv_file(path, sample_rate_hz=100.0, gap_factor=2.0)
            lines = format_gap_report_lines(report, top=1)

        self.assertEqual(lines[0], f"FILE={path}")
        self.assertIn("rows=2", lines[1])
        self.assertIn("gaps=1", lines[2])
        self.assertEqual(lines[3], "top_1_gaps:")
        self.assertIn("missing_packets=12", lines[4])

    def test_gap_csv_path_parser_preserves_spaces_in_windows_paths(self):
        paths = parse_gap_csv_paths_text(
            r"C:\Data Folder\sensor1001.csv; C:\Data Folder\sensor1002.csv"
        )

        self.assertEqual(
            [str(path) for path in paths],
            [r"C:\Data Folder\sensor1001.csv", r"C:\Data Folder\sensor1002.csv"],
        )


if __name__ == "__main__":
    unittest.main()
