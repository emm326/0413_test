"""Per-node CSV with run/sequence de-duplication and packet diagnostics."""

import csv
import json
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path


class Recorder:
    def __init__(self, root):
        self.root = Path(root)
        self.files = {}
        self.seen = OrderedDict()
        self.stats = {}
        self.pending = {}
        self.expected = {}

    def record(self, frame, metadata):
        key = (frame.node, frame.run, frame.sequence)
        node_key = (frame.node, frame.run)
        stats = self.stats.setdefault(
            node_key, {"packets": 0, "samples": 0, "duplicates": 0, "flagged_packets": 0}
        )
        if key in self.seen:
            stats["duplicates"] += 1
            return False
        self.seen[key] = True
        if len(self.seen) > 100000:
            self.seen.popitem(last=False)
        if node_key not in self.files:
            folder = self.root / f"run_{frame.run}" / f"sensor{frame.node}"
            folder.mkdir(parents=True, exist_ok=True)
            # A new run file is append-only; existing acquisition is never truncated.
            sample_path = folder / "acceleration.csv"
            packet_path = folder / "packets.csv"
            sample_new = not sample_path.exists() or not sample_path.stat().st_size
            packet_new = not packet_path.exists() or not packet_path.stat().st_size
            sample_file = sample_path.open("a", newline="", encoding="utf-8")
            packet_file = packet_path.open("a", newline="", encoding="utf-8")
            samples = csv.writer(sample_file)
            packets = csv.writer(packet_file)
            if sample_new:
                samples.writerow(["datetime", "ax", "ay", "az"])
            if packet_new:
                packets.writerow(
                    [
                        "sequence",
                        "first_utc_us",
                        "rate_hz",
                        "count",
                        "range_g",
                        "flags",
                        "freq_mhz",
                        "rssi_dbm",
                        "snr_db",
                        "received_utc_us",
                        "last_sample_latency_ms_estimated",
                    ]
                )
            self.files[node_key] = (sample_file, packet_file, samples, packets)
        sample_file, packet_file, samples, packets = self.files[node_key]
        received_utc = metadata.get("received_utc_us")
        last_sample_utc = frame.epoch_us + (frame.count - 1) * 1_000_000 // frame.rate
        latency_ms = (received_utc - last_sample_utc) / 1000 if received_utc is not None else ""
        packets.writerow(
            [
                frame.sequence,
                frame.epoch_us,
                frame.rate,
                frame.count,
                frame.range_g,
                frame.flags,
                metadata.get("freq"),
                metadata.get("rssi"),
                metadata.get("lsnr"),
                metadata.get("received_utc_us"),
                latency_ms,
            ]
        )
        packet_file.flush()
        stats["packets"] += 1
        stats["samples"] += frame.count
        stats["flagged_packets"] += bool(frame.flags & 12)
        pending = self.pending.setdefault(node_key, {})
        pending[frame.sequence] = frame
        self.expected.setdefault(node_key, 0)
        self._flush(node_key, force=len(pending) > 256)
        return True

    def _flush(self, key, force=False):
        pending = self.pending[key]
        expected = self.expected[key]
        file, _, writer, _ = self.files[key]
        stats = self.stats[key]
        while pending:
            if expected not in pending:
                if not force:
                    break
                first = min(pending)
                if first < expected:
                    # Retain late retransmissions separately without disordering the primary CSV.
                    frame = pending.pop(first)
                    late_path = (
                        self.root
                        / f"run_{frame.run}"
                        / f"sensor{frame.node}"
                        / "late_acceleration.csv"
                    )
                    new = not late_path.exists()
                    with late_path.open("a", newline="", encoding="utf-8") as late:
                        late_writer = csv.writer(late)
                        if new:
                            late_writer.writerow(["datetime", "ax", "ay", "az"])
                        self._write_samples(frame, late_writer)
                    stats["late_samples"] = stats.get("late_samples", 0) + frame.count
                    continue
                stats["unresolved_packet_gaps"] = (
                    stats.get("unresolved_packet_gaps", 0) + first - expected
                )
                expected = first
            frame = pending.pop(expected)
            self._write_samples(frame, writer)
            expected += 1
        self.expected[key] = expected
        file.flush()

    @staticmethod
    def _write_samples(frame, writer):
        for i, (x, y, z) in enumerate(frame.samples()):
            t = frame.epoch_us + i * 1_000_000 // frame.rate
            text = datetime.fromtimestamp(t / 1_000_000, timezone.utc).isoformat(
                timespec="microseconds"
            )
            scale = frame.range_g / 32768.0
            writer.writerow([text, x * scale, y * scale, z * scale])

    def close(self):
        for key in self.pending:
            self._flush(key, force=True)
        for sample_file, packet_file, _, _ in self.files.values():
            sample_file.close()
            packet_file.close()
        self.save_report()
        self.files.clear()
        self.pending.clear()

    def save_report(self):
        if self.stats:
            self.root.mkdir(parents=True, exist_ok=True)
            report = {
                f"{n}:{r}": dict(s, pending_packets=len(self.pending.get((n, r), {})))
                for (n, r), s in self.stats.items()
            }
            (self.root / "capture_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
