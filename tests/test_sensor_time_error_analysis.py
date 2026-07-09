import tempfile
import unittest
from pathlib import Path

from tools.sensor_time_error_analysis import analyze_sensor_time_error, format_time_error_report


class SensorTimeErrorAnalysisTest(unittest.TestCase):
    def test_analyzes_one_second_nearest_neighbor_and_same_index_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            sensor_a.write_text(
                "\n".join(
                    [
                        "datetime,ax,ay,az",
                        "2026/6/29 18:57:16.0000,0,0,1",
                        "2026/6/29 18:57:16.5000,0,0,1",
                        "2026/6/29 18:57:17.0000,0,0,1",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            sensor_b.write_text(
                "\n".join(
                    [
                        "datetime,ax,ay,az",
                        "2026/6/29 18:57:16.0050,0,0,1",
                        "2026/6/29 18:57:16.4950,0,0,1",
                        "2026/6/29 18:57:17.0050,0,0,1",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            report = analyze_sensor_time_error(sensor_a, sensor_b, window_seconds=1.0)

        self.assertEqual(len(report.windows), 2)
        first = report.windows[0]
        self.assertEqual(first.sensor_a_count, 2)
        self.assertEqual(first.sensor_b_count, 2)
        self.assertAlmostEqual(first.nearest_mean_ms, 5.0)
        self.assertAlmostEqual(first.nearest_max_ms, 5.0)
        self.assertAlmostEqual(first.same_index_max_ms, 5.0)

    def test_format_time_error_report_labels_aligned_and_raw_meanings(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            rows = "\n".join(
                [
                    "datetime,ax,ay,az",
                    "2026/6/29 18:57:16.0000,0,0,1",
                    "2026/6/29 18:57:16.5000,0,0,1",
                ]
            ) + "\n"
            sensor_a.write_text(rows, encoding="utf-8")
            sensor_b.write_text(rows, encoding="utf-8")

            report = analyze_sensor_time_error(sensor_a, sensor_b, window_seconds=1.0)
            text = format_time_error_report(report, csv_timebase="aligned")

        self.assertIn("csv_timebase=aligned", text)
        self.assertIn("aligned analysis grid difference", text)
        self.assertIn("0 ms means CSV grid aligned, not physical edge synchronization", text)
        self.assertIn("nearest_mean_ms=0.000", text)

    def test_format_time_error_report_labels_raw_as_gps_calibrated_edge_time(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            sensor_a = base / "sensor1001.csv"
            sensor_b = base / "sensor1002.csv"
            sensor_a.write_text(
                "datetime,ax,ay,az\n"
                "2026/6/29 18:57:16.0000,0,0,1\n",
                encoding="utf-8",
            )
            sensor_b.write_text(
                "datetime,ax,ay,az\n"
                "2026/6/29 18:57:16.0050,0,0,1\n",
                encoding="utf-8",
            )

            report = analyze_sensor_time_error(sensor_a, sensor_b, window_seconds=1.0)
            text = format_time_error_report(report, csv_timebase="raw")

        self.assertIn("csv_timebase=raw", text)
        self.assertIn("raw GPS-calibrated INT1 edge time error", text)


if __name__ == "__main__":
    unittest.main()
