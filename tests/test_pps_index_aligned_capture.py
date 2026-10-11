import csv
import math
import tempfile
import unittest
from pathlib import Path

from tools.tx_usb_capacity_test import (
    PPS_ALIGN_REORDER_WINDOW_SECONDS,
    RawAndPpsIndexAlignedOutput,
    UsbEpochSample,
    UsbPpsIndexAligner,
    actual_iim_rate_hz,
    parse_args,
)


def sample(device_id: str, epoch_us: int, value: int = 0) -> UsbEpochSample:
    return UsbEpochSample(device_id, epoch_us, value, value + 1, value + 2)


class PpsIndexAlignedCaptureTest(unittest.TestCase):
    def test_supported_rates_derive_actual_odr_and_floor_capacity(self):
        expected = {
            50.0: (51.2, 51),
            100.0: (102.4, 102),
            200.0: (204.8, 204),
            400.0: (409.6, 409),
        }

        for configured_rate, (actual_rate, capacity) in expected.items():
            with self.subTest(configured_rate=configured_rate):
                aligner = UsbPpsIndexAligner(
                    ("1001", "1002"), configured_rate
                )
                self.assertAlmostEqual(actual_iim_rate_hz(configured_rate), actual_rate)
                self.assertAlmostEqual(aligner.actual_rate_hz, actual_rate)
                self.assertEqual(aligner.capacity, capacity)
                self.assertEqual(aligner.capacity, math.floor(actual_rate))

    def test_52nd_sample_is_ignored_and_not_carried_into_next_second(self):
        aligner = UsbPpsIndexAligner(("1001", "1002"), 50.0)
        for index in range(52):
            epoch_us = 1_001_000 + index * 19_000
            aligner.add(sample("1001", epoch_us, 100 + index))
            aligner.add(sample("1002", epoch_us + 100, 200 + index))

        results = []
        results.extend(aligner.add(sample("1001", 2_010_000, 301)))
        results.extend(aligner.add(sample("1002", 2_011_000, 401)))

        first = results[0]
        self.assertEqual(first.capacity, 51)
        self.assertEqual(first.common_count, 51)
        self.assertEqual(first.count_by_device, {"1001": 51, "1002": 51})
        self.assertEqual(first.ignored_by_device, {"1001": 1, "1002": 1})
        self.assertEqual(first.paired["1001"][0][0], 1_019_531)
        self.assertEqual(first.paired["1001"][-1][0], 1_996_094)

        second = aligner.flush()[0]
        self.assertEqual(second.pps_epoch_us, 2_000_000)
        self.assertEqual(second.count_by_device, {"1001": 1, "1002": 1})
        self.assertEqual(second.common_count, 1)

    def test_three_nodes_use_the_received_index_intersection(self):
        aligner = UsbPpsIndexAligner(("1001", "1002", "1003"), 50.0)
        counts = {"1001": 51, "1002": 50, "1003": 49}
        for device_id, count in counts.items():
            for index in range(count):
                aligner.add(
                    sample(device_id, 1_005_000 + index * 19_000, index)
                )

        result = aligner.flush()[0]

        self.assertEqual(result.common_count, 49)
        self.assertEqual(result.count_by_device, counts)
        self.assertEqual(
            [len(result.paired[device_id]) for device_id in aligner.device_ids],
            [49, 49, 49],
        )
        logical_times = [time for time, _sample in result.paired["1001"]]
        for device_id in ("1002", "1003"):
            self.assertEqual(
                logical_times,
                [time for time, _sample in result.paired[device_id]],
            )

    def test_samples_are_sorted_before_assigning_per_second_indices(self):
        aligner = UsbPpsIndexAligner(("1001", "1002"), 50.0)
        for item in (
            sample("1001", 1_040_000, 12),
            sample("1001", 1_010_000, 10),
            sample("1001", 1_030_000, 11),
            sample("1002", 1_011_000, 20),
            sample("1002", 1_031_000, 21),
            sample("1002", 1_041_000, 22),
        ):
            aligner.add(item)

        result = aligner.flush()[0]

        self.assertEqual(
            [entry.ax_raw for _time, entry in result.paired["1001"]],
            [10, 11, 12],
        )
        self.assertEqual(
            [entry.ax_raw for _time, entry in result.paired["1002"]],
            [20, 21, 22],
        )

    def test_exact_pps_timestamp_is_not_the_first_post_pps_sample(self):
        aligner = UsbPpsIndexAligner(("1001", "1002"), 50.0)
        for device_id in aligner.device_ids:
            aligner.add(sample(device_id, 1_000_000, 1))
            aligner.add(sample(device_id, 1_010_000, 2))

        result = aligner.flush()[0]

        self.assertEqual(result.common_count, 1)
        self.assertEqual(result.paired["1001"][0][1].ax_raw, 2)

    def test_unhealthy_pps_excludes_the_whole_second(self):
        aligner = UsbPpsIndexAligner(
            ("1001", "1002"), 50.0, require_pps_health=True
        )
        aligner.mark_pps_health("1001", 1_000_000, True)
        aligner.mark_pps_health("1002", 1_000_000, False)
        aligner.add(sample("1001", 1_010_000, 1))
        aligner.add(sample("1002", 1_011_000, 2))

        aligner.mark_pps_health("1001", 2_000_000, True)
        results = aligner.mark_pps_health("1002", 2_000_000, True)

        first = results[0]
        self.assertEqual(first.common_count, 0)
        self.assertEqual(first.unhealthy_device_ids, ("1002",))

    def test_missing_node_is_finalized_after_five_second_window(self):
        aligner = UsbPpsIndexAligner(("1001", "1002"), 50.0)
        aligner.add(sample("1001", 1_010_000, 1))
        aligner.add(sample("1002", 1_011_000, 2))

        results = []
        for second in range(2, PPS_ALIGN_REORDER_WINDOW_SECONDS + 4):
            results.extend(
                aligner.add(sample("1001", second * 1_000_000 + 10_000, second))
            )

        first = next(result for result in results if result.pps_epoch_us == 1_000_000)
        missing = next(result for result in results if result.pps_epoch_us == 2_000_000)
        self.assertEqual(first.common_count, 1)
        self.assertEqual(missing.common_count, 0)
        self.assertEqual(missing.missing_device_ids, ("1002",))

    def test_close_finalizes_complete_missing_node_seconds_inside_reorder_window(self):
        aligner = UsbPpsIndexAligner(("1001", "1002"), 50.0)
        aligner.add(sample("1001", 1_010_000, 1))
        aligner.add(sample("1002", 1_011_000, 2))
        aligner.add(sample("1001", 2_010_000, 3))
        aligner.add(sample("1001", 3_010_000, 4))

        results = aligner.flush_complete()

        self.assertEqual(
            [result.pps_epoch_us for result in results],
            [1_000_000, 2_000_000],
        )
        self.assertEqual(results[1].missing_device_ids, ("1002",))

    def test_output_preserves_raw_values_and_writes_identical_shared_times(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = RawAndPpsIndexAlignedOutput(
                Path(temp_dir),
                ("1001", "1002", "1003"),
                50.0,
                1.0,
                log=lambda _line: None,
                require_pps_health=True,
            )
            output.set_start_epoch(1_000_000)
            for device_id in output.device_ids:
                output.mark_pps_health(device_id, 1_000_000, True)
                output.add(sample(device_id, 1_010_000, int(device_id)))
                output.add(sample(device_id, 1_030_000, int(device_id) + 1))
            for device_id in output.device_ids:
                output.mark_pps_health(device_id, 2_000_000, True)
            output.close()

            aligned_rows = {}
            for device_id in output.device_ids:
                path = next(
                    (Path(temp_dir) / "pps_index_aligned" / f"sensor{device_id}").rglob(
                        "*.csv"
                    )
                )
                with path.open(newline="", encoding="utf-8") as fp:
                    aligned_rows[device_id] = list(csv.DictReader(fp))

            raw_path = next(
                (Path(temp_dir) / "raw" / "sensor1001").rglob("*.csv")
            )
            raw_text = raw_path.read_text(encoding="utf-8")

        expected_times = [row["datetime"] for row in aligned_rows["1001"]]
        self.assertEqual(len(expected_times), 2)
        for device_id in ("1002", "1003"):
            self.assertEqual(
                expected_times,
                [row["datetime"] for row in aligned_rows[device_id]],
            )
        self.assertIn("1001.000000,1002.000000,1003.000000", raw_text)
        self.assertTrue(any(line.startswith("PPS_INDEX_ALIGN,") for line in output.report_lines))

    def test_output_close_drops_trailing_partial_second(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = RawAndPpsIndexAlignedOutput(
                Path(temp_dir),
                ("1001", "1002"),
                50.0,
                1.0,
                log=lambda _line: None,
                require_pps_health=True,
            )
            output.set_start_epoch(1_000_000)
            for device_id in output.device_ids:
                output.mark_pps_health(device_id, 1_000_000, True)
                output.add(sample(device_id, 1_010_000, 1))
            output.close()

            aligned_root = Path(temp_dir) / "pps_index_aligned"
            raw_root = Path(temp_dir) / "raw"
            self.assertFalse(aligned_root.exists())
            self.assertTrue(any(raw_root.rglob("*.csv")))

    def test_cli_accepts_three_nodes_and_requires_gps(self):
        args = parse_args(
            [
                "--ports",
                "COM5,COM6,COM7",
                "--device-ids",
                "1001,1002,1003",
                "--gps-sync",
                "--csv-timebase",
                "raw+pps-index-aligned",
            ]
        )
        self.assertEqual(args.device_ids, ["1001", "1002", "1003"])

        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--ports",
                    "COM5,COM6",
                    "--device-ids",
                    "1001,1002",
                    "--no-gps-sync",
                    "--csv-timebase",
                    "raw+pps-index-aligned",
                ]
            )

        with self.assertRaises(SystemExit):
            parse_args(
                [
                    "--ports",
                    "COM5,COM6",
                    "--device-ids",
                    "1001,1001",
                    "--gps-sync",
                    "--csv-timebase",
                    "raw+pps-index-aligned",
                ]
            )


if __name__ == "__main__":
    unittest.main()
