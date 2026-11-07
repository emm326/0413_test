import base64
import json
import socket
import struct
import tempfile
import time
import unittest
from pathlib import Path

from host.gateway import GatewayServer
from host.recorder import Recorder
from host.wire import (
    ACK,
    ACK_ENTRY,
    BROADCAST,
    CLOCK_HEALTHY,
    DATA,
    GPS_HEALTHY,
    NODE_FREQUENCIES,
    START_OPTIONS,
    STATUS,
    STATUS_PAYLOAD,
    Frame,
    start_frame,
)


class EightNodeProtocolTests(unittest.TestCase):
    def test_default_frequency_map_and_mask(self):
        self.assertEqual(
            NODE_FREQUENCIES,
            {
                1001: 470.3,
                1002: 470.5,
                1003: 470.7,
                1004: 470.9,
                1005: 471.1,
                1006: 471.3,
                1007: 471.5,
                1008: 471.7,
            },
        )
        frame = start_frame(1, 1_800_000_000_000_000)
        self.assertEqual(START_OPTIONS.unpack(frame.payload)[-1], 0xFF)

    def test_rate_requires_explicit_spreading_factor(self):
        start_frame(1, 1_800_000_000_000_000, rate=50, sf=6)
        start_frame(1, 1_800_000_000_000_000, rate=100, sf=5)
        for rate, sf in ((50, 5), (100, 6)):
            with self.assertRaises(ValueError):
                start_frame(1, 1_800_000_000_000_000, rate=rate, sf=sf)


class EightNodeGatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.events = []
        self.server = GatewayServer(
            "127.0.0.1",
            0,
            self.tmp.name,
            emit=lambda kind, payload: self.events.append((kind, payload)),
            tx_range_verified=True,
        )
        self.server.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.socket.bind(("127.0.0.1", 0))

    def tearDown(self):
        self.server.close()
        self.tmp.cleanup()

    @staticmethod
    def data(node, sequence, run=77):
        return Frame(
            DATA,
            node,
            flags=GPS_HEALTHY | CLOCK_HEALTHY,
            run=run,
            sequence=sequence,
            epoch_us=1_800_000_000_000_000 + sequence * 500_000,
            rate=50,
            count=1,
            payload=struct.pack("<hhh", node, sequence, 0),
        )

    def test_default_targets_are_all_eight_nodes(self):
        self.assertEqual(self.server.targets, tuple(range(1001, 1009)))
        self.assertEqual(
            [self.server.nodes[n].channel for n in self.server.targets],
            list(NODE_FREQUENCIES.values()),
        )

    def test_unverified_tx_rejects_start(self):
        server = GatewayServer(self.tmp.name, nodes=(1001,), tx_range_verified=False)
        try:
            with self.assertRaisesRegex(RuntimeError, "禁止启动"):
                server._request("start", {})
        finally:
            server.close()

    def test_unknown_node_is_not_recorded(self):
        frame = self.data(1009, 0)
        body = json.dumps(
            {
                "rxpk": [
                    {
                        "stat": 1,
                        "data": base64.b64encode(frame.encode()).decode("ascii"),
                    }
                ]
            }
        ).encode("utf-8")
        datagram = b"\x02ab\x00" + bytes.fromhex("0011223344556677") + body
        peer = self.server.socket.getsockname()
        self.server.handle_datagram(datagram, peer)
        self.assertEqual(self.server.unknown_nodes, {1009: 1})
        self.assertFalse(any(Path(self.tmp.name).rglob("*.csv")))

    def test_missing_confirm_cancels_start(self):
        self.server.run_id = 123
        self.server.start_us = self.server.gnss_now_us + 1_000_000
        self.server.awaiting_start = {1008}
        self.server.downlinks = [
            (time.monotonic(), start_frame(123, self.server.start_us), "START", False)
        ]
        self.server._schedule()
        self.assertEqual(self.server.run_id, 0)
        self.assertEqual(self.server.start_us, 0)
        self.assertFalse(self.server.awaiting_start)
        self.assertTrue(any("节点未确认" in str(payload) for _, payload in self.events))

    def test_repeated_start_does_not_replace_current_run(self):
        self.server.run_id = 77
        with self.assertRaisesRegex(ValueError, "拒绝重复START"):
            self.server._request("start", {})
        self.assertEqual(self.server.run_id, 77)

    def test_run_finishes_only_after_all_targets_return_to_waiting(self):
        self.server.run_id = 77
        self.server.start_us = self.server.gnss_now_us - 10_000_000
        self.server.start_reported = True
        self.server.active_targets = self.server.targets
        for node in self.server.targets:
            status = Frame(
                STATUS,
                node,
                flags=GPS_HEALTHY | CLOCK_HEALTHY,
                run=77,
                epoch_us=1_800_000_000_000_000,
                rate=50,
                range_g=4,
                payload=STATUS_PAYLOAD.pack(32000.0, 0.0, 0, 0, 0, 1, 0),
            )
            self.server._frame(status, {"datr": "SF6BW125"})
            if node != self.server.targets[-1]:
                self.assertEqual(self.server.run_id, 77)
        self.assertEqual(self.server.run_id, 0)
        self.assertEqual(self.server.start_us, 0)

    def test_one_ack_frame_contains_eight_node_entries(self):
        self.server.run_id = 77
        for node in self.server.targets:
            self.server._frame(self.data(node, node - 1000), {})
        self.server.start_us = self.server.gnss_now_us - 4_050_000
        self.server.tx_ready = time.monotonic() + 60
        self.server._schedule()
        ack_frames = [item[1] for item in self.server.downlinks if item[1].kind == ACK]
        self.assertEqual(len(ack_frames), 1)
        payload = ack_frames[0].payload
        self.assertEqual(len(payload), ACK_ENTRY.size * 8)
        entries = [ACK_ENTRY.unpack_from(payload, offset) for offset in range(0, len(payload), ACK_ENTRY.size)]
        self.assertEqual([entry[0] for entry in entries], list(self.server.targets))
        self.assertTrue(all(entry[2] == 1 for entry in entries))


class QualityReportTests(unittest.TestCase):
    def test_per_node_and_overall_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            recorder = Recorder(tmp)
            frame = EightNodeGatewayTests.data(1001, 2)
            recorder.record(frame, {"received_utc_us": frame.epoch_us + 500_000})
            recorder.record(frame, {"received_utc_us": frame.epoch_us + 500_000})
            recorder.close()
            report = json.loads((Path(tmp) / "capture_report.json").read_text(encoding="utf-8"))
            node = report["1001:77"]
            self.assertEqual(node["unresolved_packet_gaps"], 2)
            self.assertEqual(node["arrival_rate"], 1 / 3)
            self.assertEqual(node["retransmission_rate"], 0.5)
            self.assertEqual(node["within_10s_rate"], 1.0)
            self.assertEqual(report["_overall"]["final_missing_rate"], 2 / 3)


if __name__ == "__main__":
    unittest.main()
