"""Per-node CSV with run/sequence de-duplication, diagnostics and PPS sync output."""

import csv
import json
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path


class Recorder:
    def __init__(self, root, mode="raw"):
        if mode not in ("raw", "pps"):
            raise ValueError("recorder mode must be raw or pps")
        self.root = Path(root)
        self.mode = mode
        self.files = {}
        self.pps_files = {}
        self.seen = OrderedDict()
        self.stats = {}
        self.pending = {}
        self.expected = {}
        self.next_sample_index = {}
        self.runs = {}
        self.finalized_runs = set()
        self.pps_results = {}

    def register_run(self, run, start_us, targets, rate):
        self.runs[int(run)] = {
            "start_us": int(start_us),
            "targets": tuple(int(n) for n in targets),
            "rate": int(rate),
        }

    def record(self, frame, metadata):
        key = (frame.node, frame.run, frame.sequence)
        node_key = (frame.node, frame.run)
        stats = self.stats.setdefault(
            node_key,
            {
                "packets": 0,
                "samples": 0,
                "ordered_packets": 0,
                "ordered_samples": 0,
                "duplicates": 0,
                "flagged_packets": 0,
                "latency_count": 0,
                "latency_within_10s": 0,
                "max_latency_ms": 0.0,
            },
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
            if self.mode == "pps":
                pps_path = folder / "pps_samples.csv"
                pps_new = not pps_path.exists() or not pps_path.stat().st_size
                pps_file = pps_path.open("a", newline="", encoding="utf-8")
                pps_writer = csv.writer(pps_file)
                if pps_new:
                    pps_writer.writerow(
                        ["sample_index", "raw_datetime", "ax", "ay", "az"]
                    )
                self.pps_files[node_key] = (pps_file, pps_writer)
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
        if latency_ms != "":
            stats["latency_count"] += 1
            stats["latency_within_10s"] += latency_ms <= 10000
            stats["max_latency_ms"] = max(stats["max_latency_ms"], latency_ms)
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
            self._write_ordered_samples(key, frame, writer)
            stats["ordered_packets"] += 1
            expected += 1
        self.expected[key] = expected
        file.flush()
        if key in self.pps_files:
            self.pps_files[key][0].flush()

    @staticmethod
    def _iter_samples(frame):
        scale = frame.range_g / 32768.0
        for i, (x, y, z) in enumerate(frame.samples()):
            t = frame.epoch_us + i * 1_000_000 // frame.rate
            text = datetime.fromtimestamp(t / 1_000_000, timezone.utc).isoformat(
                timespec="microseconds"
            )
            yield i, text, x * scale, y * scale, z * scale

    @classmethod
    def _write_samples(cls, frame, writer):
        for _, text, ax, ay, az in cls._iter_samples(frame):
            writer.writerow([text, ax, ay, az])

    def _write_ordered_samples(self, key, frame, writer):
        start = self.next_sample_index.setdefault(key, 0)
        pps = self.pps_files.get(key)
        stats = self.stats[key]
        for i, text, ax, ay, az in self._iter_samples(frame):
            writer.writerow([text, ax, ay, az])
            if pps:
                pps[1].writerow([start + i, text, ax, ay, az])
        self.next_sample_index[key] = start + frame.count
        stats["ordered_samples"] += frame.count
        stats["raw_first_utc_us"] = stats.get("raw_first_utc_us", frame.epoch_us)
        stats["raw_last_utc_us"] = frame.epoch_us + (frame.count - 1) * 1_000_000 // frame.rate

    def finalize_run(self, run, targets=None, complete=True):
        run = int(run)
        if run in self.finalized_runs:
            return self.pps_results.get(run, {})
        info = self.runs.get(run, {})
        target_nodes = tuple(int(n) for n in (targets or info.get("targets") or ()))
        for node in target_nodes:
            key = (node, run)
            if key in self.pending:
                self._flush(key, force=True)
            if key in self.pps_files:
                self.pps_files[key][0].flush()
        if self.mode != "pps":
            self.finalized_runs.add(run)
            return {}
        result = self._finalize_pps(run, target_nodes, info, complete=complete)
        self.pps_results[run] = result
        self.finalized_runs.add(run)
        return result

    def _pps_folder(self, run):
        return self.root / f"run_{run}" / "pps_sync"

    def _pps_source_path(self, node, run):
        return self.root / f"run_{run}" / f"sensor{node}" / "pps_samples.csv"

    @staticmethod
    def _sync_text(start_us, rate, index):
        t = int(start_us) + int(index) * 1_000_000 // int(rate)
        return datetime.fromtimestamp(t / 1_000_000, timezone.utc).isoformat(
            timespec="microseconds"
        )

    def _finalize_pps(self, run, target_nodes, info, complete=True):
        folder = self._pps_folder(run)
        folder.mkdir(parents=True, exist_ok=True)
        start_us = info.get("start_us")
        rate = info.get("rate")
        reasons = []
        if not complete:
            reasons.append("运行未正常结束")
        if not target_nodes:
            reasons.append("没有目标节点")
        if not start_us:
            reasons.append("缺少运行启动时间")
        if rate not in (50, 100):
            reasons.append("采样频率无效")
        nodes = {}
        for node in target_nodes:
            key = (node, run)
            stats = self.stats.get(key, {})
            item = {
                "packets": stats.get("packets", 0),
                "samples": stats.get("samples", 0),
                "ordered_packets": stats.get("ordered_packets", 0),
                "ordered_samples": stats.get("ordered_samples", 0),
                "duplicates": stats.get("duplicates", 0),
                "unresolved_packet_gaps": stats.get("unresolved_packet_gaps", 0),
                "late_samples": stats.get("late_samples", 0),
                "raw_first_utc_us": stats.get("raw_first_utc_us"),
                "raw_last_utc_us": stats.get("raw_last_utc_us"),
            }
            if start_us and item["raw_first_utc_us"] is not None:
                item["raw_start_offset_us"] = item["raw_first_utc_us"] - start_us
            if start_us and item["raw_last_utc_us"] is not None:
                item["raw_end_offset_us"] = item["raw_last_utc_us"] - start_us
            if item["raw_first_utc_us"] is not None and item["raw_last_utc_us"] is not None:
                item["raw_span_us"] = item["raw_last_utc_us"] - item["raw_first_utc_us"]
            if not item["packets"]:
                reasons.append(f"节点{node}未收到数据")
            if item["unresolved_packet_gaps"]:
                reasons.append(f"节点{node}有未补齐包缺口{item['unresolved_packet_gaps']}")
            if item["late_samples"]:
                reasons.append(f"节点{node}有迟到样本{item['late_samples']}")
            if not item["ordered_samples"]:
                reasons.append(f"节点{node}没有可对齐样本")
            source = self._pps_source_path(node, run)
            item["source_exists"] = source.is_file()
            if not source.is_file():
                reasons.append(f"节点{node}缺少pps_samples.csv")
            item["first_index"] = 0 if item["ordered_samples"] else None
            item["last_index"] = item["ordered_samples"] - 1 if item["ordered_samples"] else None
            nodes[str(node)] = item
        common_first = max(
            (item["first_index"] for item in nodes.values() if item["first_index"] is not None),
            default=None,
        )
        common_last = min(
            (item["last_index"] for item in nodes.values() if item["last_index"] is not None),
            default=None,
        )
        if common_first is None or common_last is None or common_last < common_first:
            reasons.append("没有公共样本区间")
        common_rows = common_last - common_first + 1 if common_last is not None and common_first is not None and common_last >= common_first else 0
        if reasons:
            result = {
                "mode": "pps",
                "run": run,
                "valid": False,
                "reasons": reasons,
                "start_utc_us": start_us,
                "rate_hz": rate,
                "time_axis_source": "scheduled_start_plus_sample_index",
                "physical_sample_sync": False,
                "targets": list(target_nodes),
                "common_first_index": common_first,
                "common_last_index": common_last,
                "rows": 0,
                "nodes": nodes,
                "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            (folder / "manifest.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return result
        # Validate the indexed source files before creating aligned outputs.
        for node in target_nodes:
            item = nodes[str(node)]
            source = self._pps_source_path(node, run)
            expected = common_first
            count = 0
            try:
                with source.open(newline="", encoding="utf-8") as stream:
                    reader = csv.DictReader(stream)
                    for row in reader:
                        index = int(row["sample_index"])
                        if index < common_first:
                            continue
                        if index > common_last:
                            break
                        if index != expected:
                            reasons.append(f"节点{node}同步样本序号不连续：期望{expected}，实际{index}")
                            break
                        expected += 1
                        count += 1
            except (OSError, ValueError, KeyError) as exc:
                reasons.append(f"节点{node}同步源文件无效：{exc}")
            item["source_rows"] = count
            if not reasons and count != common_rows:
                reasons.append(f"节点{node}公共区间行数为{count}，期望{common_rows}")
        if reasons:
            result = {
                "mode": "pps",
                "run": run,
                "valid": False,
                "reasons": reasons,
                "start_utc_us": start_us,
                "rate_hz": rate,
                "time_axis_source": "scheduled_start_plus_sample_index",
                "physical_sample_sync": False,
                "targets": list(target_nodes),
                "common_first_index": common_first,
                "common_last_index": common_last,
                "rows": 0,
                "nodes": nodes,
                "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            (folder / "manifest.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return result
        self._write_aligned_outputs(run, target_nodes, nodes, common_first, common_last, start_us, rate)
        result = {
            "mode": "pps",
            "run": run,
            "valid": True,
            "reasons": [],
            "start_utc_us": start_us,
            "rate_hz": rate,
            "time_axis_source": "scheduled_start_plus_sample_index",
            "physical_sample_sync": False,
            "targets": list(target_nodes),
            "common_first_index": common_first,
            "common_last_index": common_last,
            "rows": common_rows,
            "nodes": nodes,
            "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        (folder / "manifest.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return result

    def _write_aligned_outputs(self, run, target_nodes, nodes, common_first, common_last, start_us, rate):
        folder = self._pps_folder(run)
        for node in target_nodes:
            source = self._pps_source_path(node, run)
            output = folder / f"sensor{node}" / "acceleration.csv"
            output.parent.mkdir(parents=True, exist_ok=True)
            with source.open(newline="", encoding="utf-8") as stream, output.open(
                "w", newline="", encoding="utf-8"
            ) as target:
                reader = csv.DictReader(stream)
                writer = csv.writer(target)
                writer.writerow(["datetime", "ax", "ay", "az"])
                for row in reader:
                    index = int(row["sample_index"])
                    if index < common_first:
                        continue
                    if index > common_last:
                        break
                    writer.writerow(
                        [
                            self._sync_text(start_us, rate, index),
                            row["ax"],
                            row["ay"],
                            row["az"],
                        ]
                    )
        aligned = folder / "aligned_acceleration.csv"
        sources = []
        try:
            for node in target_nodes:
                path = folder / f"sensor{node}" / "acceleration.csv"
                stream = path.open(newline="", encoding="utf-8")
                sources.append((node, stream, csv.DictReader(stream)))
            with aligned.open("w", newline="", encoding="utf-8") as target:
                writer = csv.writer(target)
                header = ["datetime"]
                for node in target_nodes:
                    header.extend([f"sensor{node}_ax", f"sensor{node}_ay", f"sensor{node}_az"])
                writer.writerow(header)
                for offset in range(common_last - common_first + 1):
                    row = [self._sync_text(start_us, rate, common_first + offset)]
                    for node, _, reader in sources:
                        item = next(reader)
                        row.extend([item["ax"], item["ay"], item["az"]])
                    writer.writerow(row)
        finally:
            for _, stream, _ in sources:
                stream.close()

    def close(self):
        for key in list(self.pending):
            self._flush(key, force=True)
        for run in list(self.runs):
            self.finalize_run(run, complete=False)
        for sample_file, packet_file, _, _ in self.files.values():
            sample_file.close()
            packet_file.close()
        for pps_file, _ in self.pps_files.values():
            pps_file.close()
        self.save_report()
        self.files.clear()
        self.pps_files.clear()
        self.pending.clear()

    def save_report(self):
        if self.stats:
            self.root.mkdir(parents=True, exist_ok=True)
            report = {}
            totals = {
                "packets": 0,
                "samples": 0,
                "duplicates": 0,
                "unresolved_packet_gaps": 0,
                "latency_count": 0,
                "latency_within_10s": 0,
                "max_latency_ms": 0.0,
            }
            for (node, run), stats in self.stats.items():
                item = dict(stats, pending_packets=len(self.pending.get((node, run), {})))
                expected_packets = item["packets"] + item.get("unresolved_packet_gaps", 0)
                received_attempts = item["packets"] + item["duplicates"]
                item["arrival_rate"] = (
                    item["packets"] / expected_packets if expected_packets else 1.0
                )
                item["retransmission_rate"] = (
                    item["duplicates"] / received_attempts if received_attempts else 0.0
                )
                item["final_missing_rate"] = (
                    item.get("unresolved_packet_gaps", 0) / expected_packets
                    if expected_packets
                    else 0.0
                )
                item["within_10s_rate"] = (
                    item["latency_within_10s"] / item["latency_count"]
                    if item["latency_count"]
                    else None
                )
                report[f"{node}:{run}"] = item
                for key in totals:
                    if key == "max_latency_ms":
                        totals[key] = max(totals[key], item[key])
                    else:
                        totals[key] += item.get(key, 0)
            expected_packets = totals["packets"] + totals["unresolved_packet_gaps"]
            received_attempts = totals["packets"] + totals["duplicates"]
            report["_overall"] = dict(
                totals,
                nodes=len(self.stats),
                arrival_rate=(totals["packets"] / expected_packets if expected_packets else 1.0),
                retransmission_rate=(totals["duplicates"] / received_attempts if received_attempts else 0.0),
                final_missing_rate=(totals["unresolved_packet_gaps"] / expected_packets if expected_packets else 0.0),
                within_10s_rate=(totals["latency_within_10s"] / totals["latency_count"] if totals["latency_count"] else None),
            )
            if self.pps_results:
                report["_pps_sync"] = {str(run): item for run, item in self.pps_results.items()}
            (self.root / "capture_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
