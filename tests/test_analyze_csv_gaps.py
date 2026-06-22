import csv
import tempfile
import unittest
from pathlib import Path

from tools.serial_rx_to_file import (
    DEFAULT_SERIAL_BAUD,
    analyze_csv_file,
    format_gap_report_lines,
    parse_args,
    parse_gap_analysis_options,
    parse_gap_csv_paths_text,
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
        self.assertEqual(result.max_gap_missing_packets, 8)
        self.assertEqual(result.max_gap_from_line, 3)
        self.assertEqual(result.max_gap_to_line, 4)

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
        self.assertIn("missing_packets=8", lines[4])

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
