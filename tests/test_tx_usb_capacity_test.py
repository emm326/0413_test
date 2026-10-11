import tempfile
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.tx_usb_capacity_test import (
    DEFAULT_BAUD,
    PPS_ALIGN_REORDER_WINDOW_SECONDS,
    PortStats,
    RawAndPpsAlignedOutput,
    StartCoordinator,
    UsbEpochSample,
    UsbPpsAligner,
    aligned_grid_period_us,
    csv_epoch_for_sample,
    format_gui_time_error_analysis,
    find_latest_sensor_csv,
    align_epoch_to_next_second_us,
    resolve_csv_timebase,
    build_arg_parser,
    find_python_with_tkinter,
    format_range_scale_text,
    format_tkinter_missing_message,
    gui_defaults_for_profile,
    indoor_cayd_gui_defaults,
    parse_args,
    parse_device_ids,
    parse_ports,
    parse_tx_usb_line,
    format_report_lines,
    scale_for_range_g,
    usb_gps_sync_gui_defaults,
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

    def test_parse_tx_gps_sync_line(self):
        gps = parse_tx_usb_line("TXG,1001,1782723038000000,123456789,1000000,0")

        self.assertEqual(gps.kind, "gps")
        self.assertEqual(gps.device_id, "1001")
        self.assertEqual(gps.pps_epoch_us, 1782723038000000)
        self.assertEqual(gps.pps_local_us, 123456789)
        self.assertEqual(gps.pps_interval_us, 1000000)
        self.assertEqual(gps.gps_age_ms, 0)

    def test_parse_clksync_diagnostic_line(self):
        diag = parse_tx_usb_line(
            "CLKSYNC,1001,1782723038000000,32768,1000000,-3,412,1"
        )

        self.assertEqual(diag.kind, "clksync")
        self.assertEqual(diag.device_id, "1001")
        self.assertEqual(diag.pps_epoch_us, 1782723038000000)
        self.assertEqual(diag.clk_hz, 32768)
        self.assertEqual(diag.pps_interval_us, 1000000)
        self.assertEqual(diag.phase_error_us, -3)
        self.assertEqual(diag.first_int1_offset_us, 412)
        self.assertEqual(diag.clk_healthy, 1)

    def test_parse_sensor_ready_line(self):
        ready = parse_tx_usb_line("SENSOR_READY,1001,0x6D,1,1,123")

        self.assertEqual(ready.kind, "sensor_ready")
        self.assertEqual(ready.device_id, "1001")
        self.assertEqual(ready.sensor_who, 0x6D)
        self.assertEqual(ready.sensor_clk_in, 1)
        self.assertEqual(ready.sensor_int1_drdy, 1)
        self.assertEqual(ready.sensor_int1_edges, 123)

    def test_parse_clksync_dpll_diagnostic_line(self):
        diag = parse_tx_usb_line(
            "CLKSYNC,1001,1782723038000000,32767,1000004,-8,412,1,-31,32767,1"
        )

        self.assertEqual(diag.kind, "clksync")
        self.assertEqual(diag.device_id, "1001")
        self.assertEqual(diag.clk_hz, 32767)
        self.assertEqual(diag.pps_interval_us, 1000004)
        self.assertEqual(diag.phase_error_us, -8)
        self.assertEqual(diag.first_int1_offset_us, 412)
        self.assertEqual(diag.clk_healthy, 1)
        self.assertEqual(diag.dpll_ppm, -31)
        self.assertEqual(diag.expected_cycles, 32767)
        self.assertEqual(diag.clk_stable, 1)

    def test_parse_human_gps_sync_log_line(self):
        gps = parse_tx_usb_line(
            "GPS sync: dev=1002,pps_epoch_us=1782723039000000,"
            "pps_local_us=22334455,pps_interval_us=999994,age_ms=0"
        )

        self.assertEqual(gps.kind, "gps")
        self.assertEqual(gps.device_id, "1002")
        self.assertEqual(gps.pps_epoch_us, 1782723039000000)
        self.assertEqual(gps.pps_local_us, 22334455)
        self.assertEqual(gps.pps_interval_us, 999994)
        self.assertEqual(gps.gps_age_ms, 0)

    def test_gps_sync_start_coordinator_waits_for_all_devices_before_epoch(self):
        wall_times = iter([100.2])
        monotonic_times = iter([10.0, 10.0, 10.0])
        coordinator = StartCoordinator(
            device_ids=["1001", "1002"],
            start_delay_seconds=20.0,
            gps_sync=True,
            gps_sync_wait_seconds=30.0,
            wall_clock=lambda: next(wall_times),
            monotonic_clock=lambda: next(monotonic_times),
        )

        coordinator.mark_gps_ready("1001")
        self.assertIsNone(coordinator.start_epoch_us)

        coordinator.mark_gps_ready("1002")

        self.assertEqual(coordinator.start_epoch_us, 121_000_000)

    def test_gps_sync_start_epoch_aligns_to_future_integer_second(self):
        self.assertEqual(align_epoch_to_next_second_us(120_000_001), 121_000_000)
        self.assertEqual(align_epoch_to_next_second_us(120_000_000), 120_000_000)

    def test_start_coordinator_requires_gps_and_sensor_ready(self):
        coordinator = StartCoordinator(
            device_ids=["1001", "1002"],
            start_delay_seconds=5.0,
            gps_sync=True,
            gps_sync_wait_seconds=30.0,
            require_sensor_ready=True,
            wall_clock=lambda: 100.2,
            monotonic_clock=lambda: 10.0,
        )
        for device_id in ("1001", "1002"):
            coordinator.mark_gps_ready(device_id)
        self.assertIsNone(coordinator.start_epoch_us)
        coordinator.mark_sensor_ready("1001")
        self.assertIsNone(coordinator.start_epoch_us)
        coordinator.mark_sensor_ready("1002")
        self.assertEqual(coordinator.start_epoch_us, 106_000_000)

    def test_start_coordinator_uses_device_gps_pps_instead_of_pc_clock(self):
        coordinator = StartCoordinator(
            device_ids=["1001", "1002"],
            start_delay_seconds=5.0,
            gps_sync=True,
            gps_sync_wait_seconds=30.0,
            require_sensor_ready=True,
            wall_clock=lambda: 1.0,
            monotonic_clock=lambda: 10.0,
        )
        coordinator.mark_gps_ready("1001", 1_786_000_000_000_000)
        coordinator.mark_gps_ready("1002", 1_786_000_001_000_000)
        coordinator.mark_sensor_ready("1001")
        coordinator.mark_sensor_ready("1002")

        self.assertEqual(coordinator.start_epoch_us, 1_786_000_006_000_000)

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
        self.assertTrue(lines[1].startswith("2026/6/21 18:01:44.5261,"))
        self.assertIn("0.012207", lines[1])

    def test_csv_epoch_for_sample_uses_actual_iim_rate_grid(self):
        start_epoch_us = 1782730636781396

        self.assertEqual(csv_epoch_for_sample(start_epoch_us, 0, 50.0), start_epoch_us)
        self.assertEqual(csv_epoch_for_sample(start_epoch_us, 1, 50.0), start_epoch_us + 19531)
        self.assertEqual(csv_epoch_for_sample(start_epoch_us, 2, 50.0), start_epoch_us + 39062)

    def test_aligned_grid_period_changes_with_sample_rate(self):
        self.assertAlmostEqual(aligned_grid_period_us(50.0), 19531.25)
        self.assertAlmostEqual(aligned_grid_period_us(100.0), 9765.625)

    def test_gps_sync_defaults_csv_timebase_to_raw(self):
        parser = build_arg_parser()
        args = parser.parse_args(
            ["--gps-sync", "--ports", "COM14,COM6", "--device-ids", "1001,1002"]
        )
        args = validate_capture_args(args)

        self.assertEqual(args.csv_timebase, "raw")
        self.assertEqual(resolve_csv_timebase(args), "raw")
        self.assertEqual(args.range_g, 4)
        self.assertAlmostEqual(args.scale, 4 / 32768.0)

    def test_range_g_sets_default_scale_when_scale_not_explicit(self):
        args = parse_args(
            [
                "--gps-sync",
                "--ports",
                "COM6,COM12",
                "--device-ids",
                "1001,1002",
                "--range-g",
                "8",
            ]
        )

        self.assertEqual(args.range_g, 8)
        self.assertAlmostEqual(args.scale, 8 / 32768.0)
        self.assertAlmostEqual(scale_for_range_g(16), 16 / 32768.0)

    def test_range_scale_text_lists_gui_conversion_table(self):
        text = format_range_scale_text(8)

        self.assertIn("Current +/-8g = 0.000244140625 g/count", text)
        self.assertIn("+/-4g: 0.000122070312 g/count", text)
        self.assertIn("+/-16g: 0.000488281250 g/count", text)

    def test_gui_source_makes_scale_read_only_and_derives_it_from_range(self):
        source = (PROJECT_ROOT / "tools" / "tx_usb_capacity_test.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("scale=scale_for_range_g(range_g)", source)
        self.assertIn('textvariable=scale_var, width=14, state="readonly"', source)

    def test_explicit_scale_overrides_range_g_scale(self):
        args = parse_args(
            [
                "--gps-sync",
                "--ports",
                "COM6,COM12",
                "--device-ids",
                "1001,1002",
                "--range-g",
                "16",
                "--scale",
                "0.123",
            ]
        )

        self.assertEqual(args.range_g, 16)
        self.assertAlmostEqual(args.scale, 0.123)

    def test_gps_sync_can_write_raw_csv_timebase(self):
        args = parse_args(
            [
                "--gps-sync",
                "--csv-timebase",
                "raw",
                "--ports",
                "COM6,COM12",
                "--device-ids",
                "1001,1002",
            ]
        )

        self.assertEqual(resolve_csv_timebase(args), "raw")

    def test_write_sample_csv_can_use_aligned_epoch_independent_of_raw_epoch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "sensor1001.csv"
            aligned_epoch_us = 1782730636799700
            raw_epoch_us = aligned_epoch_us + 5100
            with path.open("w", newline="", encoding="utf-8") as fp:
                write_sample_csv(
                    fp,
                    include_header=True,
                    device_id="1001",
                    epoch_us=raw_epoch_us,
                    ax_raw=0,
                    ay_raw=0,
                    az_raw=0,
                    scale=0.0001220703125,
                    csv_epoch_us=aligned_epoch_us,
                )

            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertIn("18:57:16.7997", lines[1])

    def test_gui_mode_does_not_require_ports_on_cli(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--gui"])

        self.assertTrue(args.gui)
        self.assertEqual(args.baud, DEFAULT_BAUD)
        self.assertEqual(args.range_g, 4)

    def test_indoor_cayd_gui_defaults_use_no_gps_16g_com_ports(self):
        defaults = indoor_cayd_gui_defaults()

        self.assertEqual(defaults.ports, "COM6,COM16")
        self.assertEqual(defaults.device_ids, "1001,1002")
        self.assertEqual(defaults.baud, DEFAULT_BAUD)
        self.assertEqual(defaults.sample_rate_hz, 50.0)
        self.assertEqual(defaults.range_g, 16)
        self.assertAlmostEqual(defaults.scale, 16 / 32768.0)
        self.assertEqual(defaults.duration_seconds, 60.0)
        self.assertEqual(defaults.start_delay_seconds, 5.0)
        self.assertEqual(defaults.serial_startup_delay_seconds, 2.0)
        self.assertFalse(defaults.gps_sync)
        self.assertEqual(defaults.csv_timebase, "raw")
        self.assertEqual(defaults.output, Path("data_mems_cayd_no_gps_16g"))
        self.assertIn("usb_test", defaults.firmware_note)
        self.assertIn("usb_gps_sync_test", defaults.firmware_note)
        self.assertIn("COM6->1001", defaults.firmware_note)
        self.assertIn("COM16->1002", defaults.firmware_note)
        self.assertIn("Range g=16", defaults.firmware_note)
        self.assertIn("GPS sync=off", defaults.firmware_note)
        self.assertIn("CSV timebase=raw", defaults.firmware_note)
        self.assertIn("data_mems_cayd_no_gps_16g", defaults.firmware_note)

    def test_usb_gps_sync_gui_defaults_use_current_com_ports(self):
        defaults = usb_gps_sync_gui_defaults()

        self.assertEqual(defaults.ports, "COM5,COM6")
        self.assertEqual(defaults.device_ids, "1001,1002")
        self.assertEqual(defaults.baud, DEFAULT_BAUD)
        self.assertEqual(defaults.sample_rate_hz, 50.0)
        self.assertEqual(defaults.range_g, 4)
        self.assertAlmostEqual(defaults.scale, 4 / 32768.0)
        self.assertEqual(defaults.duration_seconds, 600.0)
        self.assertEqual(defaults.start_delay_seconds, 5.0)
        self.assertEqual(defaults.serial_startup_delay_seconds, 2.0)
        self.assertTrue(defaults.gps_sync)
        self.assertEqual(defaults.csv_timebase, "raw+pps-aligned")
        self.assertEqual(defaults.output, Path("data_usb_pps_sync"))
        self.assertIn("usb_gps_sync_test", defaults.firmware_note)
        self.assertIn("COM5->1001", defaults.firmware_note)
        self.assertIn("COM6->1002", defaults.firmware_note)
        self.assertIn("GPS sync=on", defaults.firmware_note)

    def test_gui_defaults_profile_selects_usb_gps_by_default(self):
        self.assertEqual(gui_defaults_for_profile(None).ports, "COM5,COM6")
        self.assertEqual(gui_defaults_for_profile("usb-gps-sync").ports, "COM5,COM6")
        self.assertEqual(gui_defaults_for_profile("indoor-cayd").ports, "COM6,COM16")

    def test_no_args_defaults_to_gui_mode(self):
        args = parse_args([])

        self.assertTrue(args.gui)
        self.assertEqual(args.gui_profile, "usb-gps-sync")
        self.assertEqual(args.baud, DEFAULT_BAUD)
        self.assertTrue(args.gps_sync)
        self.assertEqual(resolve_csv_timebase(args), "raw")

    def test_no_gps_sync_option_makes_auto_csv_timebase_raw(self):
        args = parse_args(
            [
                "--no-gps-sync",
                "--ports",
                "COM6,COM12",
                "--device-ids",
                "1001,1002",
            ]
        )

        self.assertFalse(args.gps_sync)
        self.assertEqual(resolve_csv_timebase(args), "raw")

    def test_capture_mode_requires_matching_ports_and_device_ids(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--ports", "COM7,COM8", "--device-ids", "1001"])

        with self.assertRaises(SystemExit):
            validate_capture_args(args)

    def test_report_includes_extended_int1_diagnostics(self):
        stats = PortStats(port="COM7", expected_device_id="1001", sample_rate_hz=100.0)
        stats.record_stats(parse_tx_usb_line("TXS,1001,10,240,1,2,3,456,1"))

        report = "\n".join(format_report_lines([stats], 1.0, range_g=8))

        self.assertIn("int1_edges=456", report)
        self.assertIn("int1_pin=1", report)
        self.assertIn("range_g=8", report)
        self.assertIn("scale=0.000244140625", report)

    def test_report_includes_gps_sync_and_cross_device_epoch_delta(self):
        first = PortStats(port="COM14", expected_device_id="1001", sample_rate_hz=50.0)
        second = PortStats(port="COM6", expected_device_id="1002", sample_rate_hz=50.0)
        first.record_sample(parse_tx_usb_line("1001,1782723037586400,0,0,0"))
        first.record_sample(parse_tx_usb_line("1001,1782723047586400,0,0,0"))
        second.record_sample(parse_tx_usb_line("1002,1782723037584100,0,0,0"))
        second.record_sample(parse_tx_usb_line("1002,1782723047581400,0,0,0"))
        first.record_gps(parse_tx_usb_line("TXG,1001,1782723038000000,123456789,1000000,0"))

        report = "\n".join(format_report_lines([first, second], 10.0, gps_sync=True))

        self.assertIn("gps_sync_seen=1", report)
        self.assertIn("gps_sync_seen=0", report)
        self.assertIn("first_epoch_delta_ms=2.300", report)
        self.assertIn("last_epoch_delta_ms=5.000", report)

    def test_report_includes_grid_and_pps_interval_statistics(self):
        stats = PortStats(port="COM14", expected_device_id="1001", sample_rate_hz=100.0)
        stats.record_gps(parse_tx_usb_line("TXG,1001,1782723038000000,123456789,999998,0"))
        stats.record_gps(parse_tx_usb_line("TXG,1001,1782723039000000,124456791,1000002,1"))

        report = "\n".join(format_report_lines([stats], 2.0, gps_sync=True, csv_timebase="aligned"))

        self.assertIn("grid_rate_hz=102.400", report)
        self.assertIn("grid_period_us=9765.625", report)
        self.assertIn("pps_interval_min_us=999998", report)
        self.assertIn("pps_interval_max_us=1000002", report)
        self.assertIn("pps_interval_mean_us=1000000.000", report)
        self.assertIn("pps_epoch_errors=0", report)

    def test_report_includes_clksync_diagnostics(self):
        stats = PortStats(port="COM14", expected_device_id="1001", sample_rate_hz=50.0)
        stats.record_clksync(
            parse_tx_usb_line("CLKSYNC,1001,1782723038000000,32768,1000001,4,390,1")
        )

        report = "\n".join(format_report_lines([stats], 2.0, gps_sync=True))

        self.assertIn("clksync_seen=1", report)
        self.assertIn("clk_hz=32768", report)
        self.assertIn("phase_error_us=4", report)
        self.assertIn("first_int1_offset_us=390", report)
        self.assertIn("clk_healthy=1", report)

    def test_report_includes_clksync_dpll_diagnostics(self):
        stats = PortStats(port="COM14", expected_device_id="1001", sample_rate_hz=50.0)
        stats.record_clksync(
            parse_tx_usb_line(
                "CLKSYNC,1001,1782723038000000,32767,1000004,-8,412,1,-31,32767,1"
            )
        )

        report = "\n".join(format_report_lines([stats], 2.0, gps_sync=True))

        self.assertIn("clk_hz=32767", report)
        self.assertIn("phase_error_us=-8", report)
        self.assertIn("dpll_ppm=-31", report)
        self.assertIn("expected_cycles=32767", report)
        self.assertIn("clk_stable=1", report)

    def test_report_flags_repeated_or_regressed_pps_epoch(self):
        stats = PortStats(port="COM14", expected_device_id="1001", sample_rate_hz=50.0)
        stats.record_gps(parse_tx_usb_line("TXG,1001,1782723038000000,123456789,1000000,0"))
        stats.record_gps(parse_tx_usb_line("TXG,1001,1782723038000000,124456789,1000000,0"))
        stats.record_gps(parse_tx_usb_line("TXG,1001,1782723037000000,125456789,1000000,0"))

        report = "\n".join(format_report_lines([stats], 2.0, gps_sync=True))

        self.assertIn("pps_epoch_errors=2", report)

    def test_gps_sync_cli_flag_parses(self):
        parser = build_arg_parser()
        args = parser.parse_args(
            ["--gps-sync", "--ports", "COM14,COM6", "--device-ids", "1001,1002"]
        )
        args = validate_capture_args(args)

        self.assertTrue(args.gps_sync)
        self.assertEqual(args.gps_sync_wait_seconds, 60.0)

    def test_user_gps_sync_command_without_csv_timebase_resolves_to_raw(self):
        args = parse_args(
            [
                "--gps-sync",
                "--ports",
                "COM14,COM6",
                "--device-ids",
                "1001,1002",
                "--baud",
                "460800",
                "--sample-rate-hz",
                "50",
                "--duration-seconds",
                "300",
                "--start-delay-seconds",
                "20",
                "--serial-startup-delay-seconds",
                "2",
                "--gps-sync-wait-seconds",
                "90",
                "--samples-per-packet",
                "24",
                "--output",
                "data_tx_usb_gps_align",
            ]
        )

        self.assertEqual(args.ports, ["COM14", "COM6"])
        self.assertEqual(args.device_ids, ["1001", "1002"])
        self.assertEqual(resolve_csv_timebase(args), "raw")

    def test_aligned_mode_stats_use_written_grid_epoch_for_gaps(self):
        stats = PortStats(port="COM6", expected_device_id="1001", sample_rate_hz=50.0)
        raw_first = parse_tx_usb_line("1001,1782036104000000,0,0,0")
        raw_second = parse_tx_usb_line("1001,1782036104000100,0,0,0")
        raw_third = parse_tx_usb_line("1001,1782036105000000,0,0,0")

        stats.record_sample(raw_first, csv_epoch_us=1782036104000000)
        stats.record_sample(raw_second, csv_epoch_us=1782036104019531)
        stats.record_sample(raw_third, csv_epoch_us=1782036104039062)

        self.assertEqual(stats.rows, 3)
        self.assertEqual(stats.gap_count, 0)
        self.assertEqual(stats.max_gap_us, 0)
        self.assertEqual(stats.first_epoch_us, 1782036104000000)
        self.assertEqual(stats.last_epoch_us, 1782036104039062)

    def test_pps_aligner_drops_samples_before_common_anchor_and_pairs_slots(self):
        aligner = UsbPpsAligner(("1001", "1002"), 50.0)
        values = [
            UsbEpochSample("1001", 1_010_000, 11, 12, 13),
            UsbEpochSample("1001", 1_030_000, 14, 15, 16),
            UsbEpochSample("1002", 1_015_000, 21, 22, 23),
            UsbEpochSample("1002", 1_034_000, 24, 25, 26),
        ]
        for value in values:
            aligner.add(value)
        results = []
        results.extend(aligner.add(UsbEpochSample("1001", 2_010_000, 0, 0, 0)))
        results.extend(aligner.add(UsbEpochSample("1002", 2_010_000, 0, 0, 0)))

        result = results[0]
        self.assertEqual(result.anchor_epoch_us, 1_015_000)
        self.assertEqual(result.paired_count, 1)
        self.assertEqual(result.paired["1001"][0][1].ax_raw, 14)
        self.assertEqual(result.paired["1002"][0][1].ax_raw, 24)
        self.assertEqual(result.paired["1001"][0][0], result.paired["1002"][0][0])

    def test_pps_aligner_skips_slot_missing_on_one_device(self):
        aligner = UsbPpsAligner(("1001", "1002"), 50.0)
        for item in (
            UsbEpochSample("1001", 1_010_000, 1, 0, 0),
            UsbEpochSample("1001", 1_029_531, 2, 0, 0),
            UsbEpochSample("1002", 1_010_000, 3, 0, 0),
        ):
            aligner.add(item)
        result = aligner.flush()[0]

        self.assertEqual(result.paired_count, 1)
        self.assertGreater(result.missing_slots, 0)
        self.assertEqual(len(result.paired["1001"]), len(result.paired["1002"]))

    def test_pps_aligner_handles_out_of_order_and_duplicate_samples(self):
        aligner = UsbPpsAligner(("1001", "1002"), 50.0)
        for item in (
            UsbEpochSample("1001", 1_049_531, 12, 0, 0),
            UsbEpochSample("1001", 1_010_000, 10, 0, 0),
            UsbEpochSample("1001", 1_030_500, 99, 0, 0),
            UsbEpochSample("1001", 1_029_531, 11, 0, 0),
            UsbEpochSample("1002", 1_010_000, 20, 0, 0),
            UsbEpochSample("1002", 1_029_531, 21, 0, 0),
            UsbEpochSample("1002", 1_049_531, 22, 0, 0),
        ):
            aligner.add(item)

        result = aligner.flush()[0]

        self.assertEqual(result.paired_count, 3)
        self.assertEqual(
            [sample.ax_raw for _, sample in result.paired["1001"]],
            [10, 11, 12],
        )
        self.assertEqual(result.dropped_by_device["1001"], 1)

    def test_pps_aligner_expires_old_seconds_when_one_device_disappears(self):
        aligner = UsbPpsAligner(("1001", "1002"), 50.0)
        aligner.add(UsbEpochSample("1001", 1_010_000, 1, 0, 0))
        aligner.add(UsbEpochSample("1002", 1_010_000, 2, 0, 0))

        results = []
        for second in range(2, PPS_ALIGN_REORDER_WINDOW_SECONDS + 4):
            results.extend(
                aligner.add(UsbEpochSample("1001", second * 1_000_000 + 10_000, 1, 0, 0))
            )

        self.assertTrue(any(result.pps_epoch_us == 1_000_000 for result in results))
        missing = next(result for result in results if result.pps_epoch_us == 2_000_000)
        self.assertEqual(missing.missing_device_ids, ("1002",))

    def test_pps_aligner_discards_late_sample_after_second_is_finalized(self):
        aligner = UsbPpsAligner(("1001", "1002"), 50.0)
        for device_id in ("1001", "1002"):
            aligner.add(UsbEpochSample(device_id, 1_010_000, 1, 0, 0))
            aligner.add(UsbEpochSample(device_id, 2_010_000, 1, 0, 0))

        self.assertEqual(aligner.add(UsbEpochSample("1002", 1_020_000, 3, 0, 0)), [])
        self.assertEqual(aligner.late_samples, 1)

    def test_raw_and_pps_aligned_output_preserves_values_and_shared_times(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output = RawAndPpsAlignedOutput(
                Path(tmp_dir), ("1001", "1002"), 50.0, 1.0, log=lambda _line: None
            )
            for item in (
                UsbEpochSample("1001", 1_010_000, 11, 12, 13),
                UsbEpochSample("1002", 1_010_000, 21, 22, 23),
                UsbEpochSample("1001", 2_010_000, 0, 0, 0),
                UsbEpochSample("1002", 2_010_000, 0, 0, 0),
            ):
                output.add(item)
            output.close()

            raw_1001 = next((Path(tmp_dir) / "raw" / "sensor1001").rglob("*.csv"))
            aligned_1001 = next((Path(tmp_dir) / "pps_aligned" / "sensor1001").rglob("*.csv"))
            aligned_1002 = next((Path(tmp_dir) / "pps_aligned" / "sensor1002").rglob("*.csv"))
            raw_text = raw_1001.read_text(encoding="utf-8")
            rows_1001 = aligned_1001.read_text(encoding="utf-8").splitlines()
            rows_1002 = aligned_1002.read_text(encoding="utf-8").splitlines()

        self.assertIn("11.000000,12.000000,13.000000", raw_text)
        self.assertEqual([row.split(",", 1)[0] for row in rows_1001],
                         [row.split(",", 1)[0] for row in rows_1002])

    def test_raw_and_pps_aligned_output_close_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output = RawAndPpsAlignedOutput(
                Path(tmp_dir), ("1001", "1002"), 50.0, 1.0, log=lambda _line: None
            )
            output.close()
            output.close()

        self.assertTrue(output.closed)

    def test_raw_and_pps_aligned_cli_requires_two_gps_devices(self):
        with self.assertRaises(SystemExit):
            parse_args([
                "--ports", "COM5", "--device-ids", "1001",
                "--csv-timebase", "raw+pps-aligned",
            ])

    def test_find_latest_sensor_csv_uses_newest_date_folder(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output = Path(tmp_dir)
            older = output / "sensor1001" / "20260629" / "tx_usb_capacity.csv"
            newer = output / "sensor1001" / "20260630" / "tx_usb_capacity.csv"
            older.parent.mkdir(parents=True)
            newer.parent.mkdir(parents=True)
            older.write_text("datetime,ax,ay,az\n2026/6/29 10:00:00.0000,0,0,1\n", encoding="utf-8")
            newer.write_text("datetime,ax,ay,az\n2026/6/30 10:00:00.0000,0,0,1\n", encoding="utf-8")

            self.assertEqual(find_latest_sensor_csv(output, "1001"), newer)

    def test_gui_time_error_analysis_formats_raw_report_for_two_devices(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output = Path(tmp_dir)
            first = output / "sensor1001" / "20260630" / "tx_usb_capacity.csv"
            second = output / "sensor1002" / "20260630" / "tx_usb_capacity.csv"
            first.parent.mkdir(parents=True)
            second.parent.mkdir(parents=True)
            first.write_text(
                "datetime,ax,ay,az\n"
                "2026/6/30 10:00:00.0000,0,0,1\n"
                "2026/6/30 10:00:00.0200,0,0,1\n",
                encoding="utf-8",
            )
            second.write_text(
                "datetime,ax,ay,az\n"
                "2026/6/30 10:00:00.0050,0,0,1\n"
                "2026/6/30 10:00:00.0250,0,0,1\n",
                encoding="utf-8",
            )

            report = format_gui_time_error_analysis(
                output=output,
                device_ids=["1001", "1002"],
                csv_timebase="raw",
                window_seconds=1.0,
            )

        self.assertIn("csv_timebase=raw", report)
        self.assertIn('meaning="raw GPS-calibrated INT1 edge time error"', report)
        self.assertIn("max_abs_ms=5.000", report)

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
