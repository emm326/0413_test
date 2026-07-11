import csv
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from tools.sensor_time_error_analysis import (
    analyze_sensor_time_error,
    format_time_error_report,
    write_aligned_common_grid,
)


def datetime_text(epoch_us: int) -> str:
    dt = datetime.fromtimestamp(epoch_us / 1_000_000)
    return dt.strftime("%Y/%m/%d %H:%M:%S.%f")


def write_samples(path: Path, epochs_us: list[int], values: list[float] | None = None) -> None:
    values = values or [float(index) for index in range(len(epochs_us))]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["datetime", "ax", "ay", "az"])
        for epoch_us, value in zip(epochs_us, values):
            writer.writerow([datetime_text(epoch_us), value, value * 2, value * 3])


class SensorTimeErrorAnalysisTest(unittest.TestCase):
    def test_reports_constant_offset_with_one_to_one_matches(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            start = 1_783_000_000_000_000
            epochs_a = [start + index * 20_000 for index in range(100)]
            epochs_b = [epoch + 5_000 for epoch in epochs_a]
            write_samples(sensor_a, epochs_a)
            write_samples(sensor_b, epochs_b)

            report = analyze_sensor_time_error(
                sensor_a, sensor_b, actual_rate_hz=50.0, max_error_ms=20.0
            )

        self.assertEqual(report.matched_count, 100)
        self.assertEqual(report.unmatched_a, 0)
        self.assertEqual(report.unmatched_b, 0)
        self.assertAlmostEqual(report.signed_mean_ms, 5.0)
        self.assertAlmostEqual(report.mean_abs_ms, 5.0)
        self.assertAlmostEqual(report.p95_abs_ms, 5.0)
        self.assertAlmostEqual(report.max_abs_ms, 5.0)
        self.assertTrue(report.passed)

    def test_common_overlap_excludes_unmatched_leading_sample(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            start = 1_783_000_000_000_000
            period = 19_531
            epochs_a = [start + index * period for index in range(10)]
            epochs_b = [epochs_a[index] + 1_000 for index in range(1, 10)]
            write_samples(sensor_a, epochs_a)
            write_samples(sensor_b, epochs_b)

            report = analyze_sensor_time_error(sensor_a, sensor_b)

        self.assertEqual(report.matched_count, 9)
        self.assertEqual(report.unmatched_a, 1)
        self.assertEqual(report.unmatched_b, 0)
        self.assertAlmostEqual(report.max_abs_ms, 1.0)

    def test_estimates_linear_clock_drift_in_ppm(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            start = 1_783_000_000_000_000
            period_us = 20_000
            epochs_a = [start + index * period_us for index in range(1001)]
            epochs_b = [
                epoch + 1_000 + round(index * period_us * 10 / 1_000_000)
                for index, epoch in enumerate(epochs_a)
            ]
            write_samples(sensor_a, epochs_a)
            write_samples(sensor_b, epochs_b)

            report = analyze_sensor_time_error(
                sensor_a, sensor_b, actual_rate_hz=50.0, max_error_ms=20.0
            )

        self.assertAlmostEqual(report.drift_ppm, 10.0, delta=0.1)
        self.assertAlmostEqual(report.first_signed_ms, 1.0, delta=0.001)
        self.assertAlmostEqual(report.last_signed_ms, 1.2, delta=0.001)

    def test_detects_duplicate_non_monotonic_and_gap(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            start = 1_783_000_000_000_000
            epochs_a = [start, start + 20_000, start + 20_000, start + 10_000, start + 100_000]
            epochs_b = [start + index * 20_000 for index in range(6)]
            write_samples(sensor_a, epochs_a)
            write_samples(sensor_b, epochs_b)

            report = analyze_sensor_time_error(
                sensor_a, sensor_b, actual_rate_hz=50.0, max_error_ms=20.0
            )

        self.assertEqual(report.quality_a.duplicate_timestamps, 1)
        self.assertEqual(report.quality_a.non_monotonic_timestamps, 1)
        self.assertEqual(report.quality_a.gap_count, 1)
        self.assertAlmostEqual(report.quality_a.max_gap_ms, 90.0)

    def test_linear_interpolation_writes_common_grid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            output = base / "aligned_common_grid.csv"
            start = 1_783_000_000_000_000
            epochs_a = [start + index * 20_000 for index in range(6)]
            epochs_b = [start + 5_000 + index * 20_000 for index in range(6)]
            values_a = [(epoch - start) / 1_000_000 for epoch in epochs_a]
            values_b = [(epoch - start) / 1_000_000 for epoch in epochs_b]
            write_samples(sensor_a, epochs_a, values_a)
            write_samples(sensor_b, epochs_b, values_b)

            rows = write_aligned_common_grid(sensor_a, sensor_b, output, actual_rate_hz=50.0)
            with output.open("r", newline="", encoding="utf-8") as fp:
                result = list(csv.DictReader(fp))

        self.assertEqual(rows, 5)
        self.assertEqual(len(result), 5)
        self.assertAlmostEqual(float(result[0]["sensor1001_ax"]), 0.02, places=6)
        self.assertAlmostEqual(float(result[0]["sensor1002_ax"]), 0.02, places=6)

    def test_alignment_does_not_interpolate_across_large_gap(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            output = base / "aligned_common_grid.csv"
            start = 1_783_000_000_000_000
            epochs_a = [start, start + 20_000, start + 80_000, start + 100_000]
            epochs_b = [start + index * 20_000 for index in range(6)]
            write_samples(sensor_a, epochs_a)
            write_samples(sensor_b, epochs_b)

            rows = write_aligned_common_grid(sensor_a, sensor_b, output, actual_rate_hz=50.0)

        self.assertEqual(rows, 4)

    def test_report_explains_pps_assumption_and_grid_limit(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            start = 1_783_000_000_000_000
            epochs = [start, start + 20_000]
            write_samples(sensor_a, epochs)
            write_samples(sensor_b, [epoch + 1_000 for epoch in epochs])
            report = analyze_sensor_time_error(
                sensor_a, sensor_b, actual_rate_hz=50.0, max_error_ms=20.0
            )

        text = format_time_error_report(report, csv_timebase="raw")
        self.assertIn("result=PASS", text)
        self.assertIn("PPS is treated as the accurate UTC reference", text)
        self.assertIn("does not prove physical INT1 edge synchronization", text)

    def test_current_data_regression_max_error_is_2_point_2_ms(self):
        project_root = Path(__file__).resolve().parents[1]
        sensor_a = project_root / "data1" / "sensor1001" / "20260711" / "tx_usb_capacity.csv"
        sensor_b = project_root / "data1" / "sensor1002" / "20260711" / "tx_usb_capacity.csv"
        if not sensor_a.exists() or not sensor_b.exists():
            self.skipTest("current data1 capture is not available")

        report = analyze_sensor_time_error(sensor_a, sensor_b)

        self.assertEqual(report.matched_count, 5999)
        self.assertAlmostEqual(report.mean_abs_ms, 1.517, delta=0.001)
        self.assertAlmostEqual(report.median_abs_ms, 1.5, delta=0.001)
        self.assertAlmostEqual(report.rms_abs_ms, 1.565, delta=0.001)
        self.assertAlmostEqual(report.p95_abs_ms, 2.1, delta=0.001)
        self.assertAlmostEqual(report.max_abs_ms, 2.2, delta=0.001)
        self.assertAlmostEqual(report.drift_ppm, 11.28, delta=0.02)
        self.assertTrue(report.passed)


if __name__ == "__main__":
    unittest.main()
