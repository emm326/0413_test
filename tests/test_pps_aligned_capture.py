import csv
import tempfile
import unittest
from pathlib import Path

from tools.serial_rx_to_file import (
    CaptureConfig,
    CsvWriterManager,
    EpochSample,
    PpsAlignedBatcher,
    PPS_ALIGNED_REORDER_WINDOW_SECONDS,
    scale_for_range_g,
    write_pps_aligned_results,
)


def sample(device_id: str, epoch_us: int, value: int = 0) -> EpochSample:
    return EpochSample(device_id, epoch_us, value, value, value)


class PpsAlignedBatcherTest(unittest.TestCase):
    def _finalize_second_one(self, batcher: PpsAlignedBatcher) -> list:
        results = []
        results.extend(batcher.add(sample("1001", 2_000_100)))
        results.extend(batcher.add(sample("1002", 2_000_100)))
        return results

    def test_uses_later_first_sample_as_common_anchor(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        batcher.add(sample("1001", 1_015_625, 1))
        batcher.add(sample("1001", 1_035_156, 2))
        batcher.add(sample("1002", 1_012_000, 3))
        batcher.add(sample("1002", 1_031_531, 4))

        result = self._finalize_second_one(batcher)[0]

        self.assertEqual(result.pps_epoch_us, 1_000_000)
        self.assertEqual(result.anchor_epoch_us, 1_015_625)
        self.assertEqual(result.paired_count, 2)
        self.assertEqual(result.samples_by_device["1001"][0].epoch_us, 1_015_625)
        self.assertEqual(result.samples_by_device["1002"][0].epoch_us, 1_012_000)

    def test_missing_node_second_is_reported_without_fabricating_rows(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        batcher.add(sample("1001", 1_010_000))

        result = self._finalize_second_one(batcher)[0]

        self.assertEqual(result.paired_count, 0)
        self.assertIsNone(result.anchor_epoch_us)
        self.assertEqual(result.missing_device_ids, ("1002",))
        self.assertEqual(result.dropped_by_device["1001"], 1)

    def test_limits_each_second_to_common_grid_before_next_pps(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        for index in range(52):
            epoch_us = 1_015_625 + index * 19_000
            batcher.add(sample("1001", epoch_us, index))
            batcher.add(sample("1002", epoch_us, index))

        result = self._finalize_second_one(batcher)[0]

        self.assertEqual(result.paired_count, 51)
        self.assertEqual(result.dropped_by_device["1001"], 1)
        self.assertEqual(result.dropped_by_device["1002"], 1)

    def test_late_samples_after_finalization_are_not_reopened(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        batcher.add(sample("1001", 1_010_000))
        batcher.add(sample("1002", 1_012_000))
        self._finalize_second_one(batcher)

        self.assertEqual(batcher.add(sample("1001", 1_020_000)), [])
        self.assertEqual(batcher.late_samples, 1)

    def test_missing_node_is_finalized_after_bounded_reorder_window(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        batcher.add(sample("1001", 1_010_000))

        results = []
        for second in range(2, PPS_ALIGNED_REORDER_WINDOW_SECONDS + 3):
            results.extend(batcher.add(sample("1001", second * 1_000_000 + 10_000)))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].pps_epoch_us, 1_000_000)
        self.assertEqual(results[0].missing_device_ids, ("1002",))

    def test_written_device_csvs_have_identical_logical_datetimes(self):
        batcher = PpsAlignedBatcher(("1001", "1002"), 50.0)
        batcher.add(sample("1001", 1_010_000, 11))
        batcher.add(sample("1001", 1_029_531, 12))
        batcher.add(sample("1002", 1_015_625, 21))
        batcher.add(sample("1002", 1_035_156, 22))
        results = self._finalize_second_one(batcher)

        with tempfile.TemporaryDirectory() as temp_dir:
            config = CaptureConfig(
                mode="capture",
                ports=[],
                baud=460800,
                output=Path(temp_dir),
                rotate_minutes=30,
                range_g=4,
                scale=scale_for_range_g(4),
                base_time=None,
                append=False,
                echo_data=False,
                write_header=True,
                include_id=False,
                legacy_device_id="1001",
                send_start_at=False,
                start_delay_seconds=10.0,
                sample_rate_hz=50.0,
                serial_startup_delay_seconds=0.0,
                csv_timebase="pps-aligned",
                pps_aligned_device_ids=("1001", "1002"),
                gap_csv_files=[],
                gap_factor=2.0,
                gap_samples_per_packet=24,
                gap_top=5,
            )
            writer = CsvWriterManager(config, log=lambda _message: None)
            try:
                write_pps_aligned_results(results, config, writer, log=lambda _message: None)
            finally:
                writer.close()

            paths = {
                device_id: next((Path(temp_dir) / f"sensor{device_id}").rglob("*.csv"))
                for device_id in ("1001", "1002")
            }
            rows = {}
            for device_id, path in paths.items():
                with path.open(newline="", encoding="utf-8") as fp:
                    rows[device_id] = list(csv.DictReader(fp))
            self.assertEqual([row["datetime"] for row in rows["1001"]],
                             [row["datetime"] for row in rows["1002"]])
            self.assertEqual(len(rows["1001"]), 2)


if __name__ == "__main__":
    unittest.main()
