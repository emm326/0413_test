import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.tx_usb_capacity_test import (
    DEFAULT_BAUD,
    PortStats,
    build_arg_parser,
    find_python_with_tkinter,
    format_tkinter_missing_message,
    parse_args,
    parse_device_ids,
    parse_ports,
    parse_tx_usb_line,
    validate_capture_args,
    write_sample_csv,
)


class TxUsbCapacityTest(unittest.TestCase):
    def test_parse_ports_and_device_ids(self):
        self.assertEqual(parse_ports("COM7, COM8;COM7"), ["COM7", "COM8"])
        self.assertEqual(parse_device_ids("1001,1002"), ["1001", "1002"])

    def test_parse_tx_header_sample_and_stats_lines(self):
        header = parse_tx_usb_line("TXH,1001,42,123456,500,24")
        sample = parse_tx_usb_line("1001,1782036104526132,-1,2,-3")
        stats = parse_tx_usb_line("TXS,1001,10,240,1,2,3")

        self.assertEqual(header.kind, "header")
        self.assertEqual(header.device_id, "1001")
        self.assertEqual(header.seq, 42)
        self.assertEqual(header.count, 24)
        self.assertEqual(sample.kind, "sample")
        self.assertEqual(sample.epoch_us, 1782036104526132)
        self.assertEqual(sample.ax_raw, -1)
        self.assertEqual(stats.kind, "stats")
        self.assertEqual(stats.samples, 240)
        self.assertEqual(stats.int1_overflow, 2)

    def test_parse_extended_tx_stats_line(self):
        stats = parse_tx_usb_line("TXS,1001,10,240,1,2,3,456,1")

        self.assertEqual(stats.kind, "stats")
        self.assertEqual(stats.int1_missing, 1)
        self.assertEqual(stats.fifo_read_fail, 3)
        self.assertEqual(stats.int1_edges, 456)
        self.assertEqual(stats.int1_pin, 1)

    def test_port_stats_detects_seq_gap_and_sample_gap(self):
        stats = PortStats(port="COM7", expected_device_id="1001", sample_rate_hz=50.0)
        stats.record_header(parse_tx_usb_line("TXH,1001,0,0,500,24"))
        stats.record_header(parse_tx_usb_line("TXH,1001,2,468750,500,24"))
        stats.record_sample(parse_tx_usb_line("1001,1782036104000000,0,0,0"))
        stats.record_sample(parse_tx_usb_line("1001,1782036104019531,0,0,0"))
        stats.record_sample(parse_tx_usb_line("1001,1782036104200000,0,0,0"))

        self.assertEqual(stats.seq_missing, 1)
        self.assertEqual(stats.max_gap_us, 180469)
        self.assertEqual(stats.rows, 3)
        self.assertGreater(stats.capture_ratio(3 / 51.2), 0.99)

    def test_write_sample_csv_converts_epoch_to_datetime(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sensor1001.csv"
            with path.open("w", newline="", encoding="utf-8") as fp:
                write_sample_csv(
                    fp,
                    include_header=True,
                    device_id="1001",
                    epoch_us=1782036104526132,
                    ax_raw=100,
                    ay_raw=-200,
                    az_raw=300,
                    scale=0.0001220703125,
                )

            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(lines[0], "datetime,ax,ay,az")
        self.assertIn("2026/", lines[1])
        self.assertIn("0.012207", lines[1])

    def test_gui_mode_does_not_require_ports_on_cli(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--gui"])

        self.assertTrue(args.gui)
        self.assertEqual(args.baud, DEFAULT_BAUD)

    def test_no_args_defaults_to_gui_mode(self):
        args = parse_args([])

        self.assertTrue(args.gui)
        self.assertEqual(args.baud, DEFAULT_BAUD)

    def test_capture_mode_requires_matching_ports_and_device_ids(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--ports", "COM7,COM8", "--device-ids", "1001"])

        with self.assertRaises(SystemExit):
            validate_capture_args(args)

    def test_report_includes_extended_int1_diagnostics(self):
        stats = PortStats(port="COM7", expected_device_id="1001", sample_rate_hz=100.0)
        stats.record_stats(parse_tx_usb_line("TXS,1001,10,240,1,2,3,456,1"))

        report = "\n".join(stats_line for stats_line in __import__(
            "tools.tx_usb_capacity_test", fromlist=["format_report_lines"]
        ).format_report_lines([stats], 1.0))

        self.assertIn("int1_edges=456", report)
        self.assertIn("int1_pin=1", report)

    def test_find_python_with_tkinter_skips_current_and_picks_working_candidate(self):
        calls = []

        def run_probe(command, **_kwargs):
            calls.append(command[0])
            return SimpleNamespace(returncode=0 if command[0] == "python-ok" else 1)

        candidate = find_python_with_tkinter(
            candidates=[r"C:\current\python.exe", "python-bad", "python-ok"],
            current_executable=r"C:\current\python.exe",
            run_probe=run_probe,
        )

        self.assertEqual(candidate, "python-ok")
        self.assertEqual(calls, ["python-bad", "python-ok"])

    def test_tkinter_missing_message_points_to_system_python(self):
        message = format_tkinter_missing_message(r"C:\Users\me\.platformio\penv\Scripts\python.exe")

        self.assertIn("PlatformIO Python", message)
        self.assertIn("python tools\\tx_usb_capacity_test.py", message)


if __name__ == "__main__":
    unittest.main()
