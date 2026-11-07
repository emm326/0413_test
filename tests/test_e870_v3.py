import base64
import binascii
import csv
import json
import socket
import struct
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock
from host.wire import *
from host.gateway import GatewayServer
from host.recorder import Recorder


class ProtocolTests(unittest.TestCase):
    def sample(self, seq=0, count=25):
        return Frame(
            DATA,
            1001,
            flags=3,
            run=12,
            sequence=seq,
            epoch_us=1_800_000_000_000_000 + seq * 250000,
            rate=100,
            count=count,
            range_g=4,
            payload=struct.pack("<hhh", -32768, 0, 32767) * count,
        )

    def test_crc_reference(self):
        self.assertEqual(binascii.crc_hqx(b"123456789", 0xFFFF), 0x29B1)

    def test_180_bytes_and_signed_xyz(self):
        frame = self.sample()
        raw = frame.encode()
        self.assertEqual(len(raw), 180)
        self.assertEqual(raw[:6], bytes.fromhex("5aa50301e903"))
        self.assertEqual(Frame.decode(raw), frame)
        self.assertEqual(Frame.decode(raw).samples()[0], (-32768, 0, 32767))

    def test_crc_rejects_corruption(self):
        raw = bytearray(self.sample().encode())
        raw[40] ^= 1
        with self.assertRaisesRegex(ValueError, "CRC"):
            Frame.decode(raw)

    def test_length_and_version_rejected(self):
        for raw in [b"", b"x" * 29, b"x" * 256]:
            with self.assertRaises(ValueError):
                Frame.decode(raw)
        raw = bytearray(self.sample().encode())
        raw[2] = 2
        with self.assertRaises(ValueError):
            Frame.decode(raw)

    def test_tail_and_bad_counts(self):
        self.assertEqual(Frame.decode(self.sample(count=3).encode()).count, 3)
        with self.assertRaises(ValueError):
            Frame.decode(Frame(DATA, 1001, rate=100, count=2, payload=b"x" * 6).encode())

    def test_start_mask_preserves_packet_length(self):
        f = start_frame(123, 1_800_000_000_000_000, node_mask=(1 << 0) | (1 << 3))
        self.assertEqual(len(f.encode()), 42)
        self.assertEqual(START_OPTIONS.unpack(Frame.decode(f.encode()).payload)[-1], 9)

    def test_invalid_rate(self):
        with self.assertRaises(ValueError):
            start_frame(1, 100, rate=102)

    def test_airtime_budget(self):
        self.assertAlmostEqual(airtime_seconds(180, 6), 0.1696)
        self.assertAlmostEqual(airtime_seconds(180, 5), 0.10016)


class RecorderTests(unittest.TestCase):
    sample = ProtocolTests.sample

    def test_reorder_and_deduplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            rec = Recorder(tmp)
            self.assertTrue(rec.record(self.sample(seq=1), {}))
            self.assertTrue(rec.record(self.sample(seq=0), {}))
            self.assertFalse(rec.record(self.sample(seq=0), {}))
            rec.close()
            p = Path(tmp) / "run_12" / "sensor1001" / "acceleration.csv"
            with p.open(encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 50)
            self.assertEqual([r["datetime"] for r in rows], sorted(r["datetime"] for r in rows))
            self.assertEqual(float(rows[0]["ax"]), -4.0)
            report = json.loads((Path(tmp) / "capture_report.json").read_text())
            self.assertEqual(report["1001:12"]["duplicates"], 1)

    def test_packet_receipt_time_and_latency(self):
        with tempfile.TemporaryDirectory() as tmp:
            rec = Recorder(tmp)
            f = self.sample()
            rec.record(f, {"received_utc_us": f.epoch_us + 1_000_000})
            rec.close()
            path = Path(tmp) / "run_12" / "sensor1001" / "packets.csv"
            with path.open(encoding="utf-8") as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(float(row["last_sample_latency_ms_estimated"]), 760.0)
            self.assertEqual(int(row["received_utc_us"]), f.epoch_us + 1_000_000)

    def test_close_flushes_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            rec = Recorder(tmp)
            rec.record(self.sample(seq=2, count=1), {})
            rec.close()
            self.assertEqual(rec.stats[(1001, 12)]["unresolved_packet_gaps"], 2)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.events = []
        self.server = GatewayServer(
            "127.0.0.1",
            0,
            self.tmp.name,
            nodes=(1001, 1002),
            emit=lambda k, p: self.events.append((k, p)),
            tx_range_verified=True,
        )
        self.server.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.socket.bind(("127.0.0.1", 0))
        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.bind(("127.0.0.1", 0))
        self.client.settimeout(1)
        self.eui = bytes.fromhex("0011223344556677")

    def tearDown(self):
        self.server.close()
        self.client.close()
        self.tmp.cleanup()

    def gateway_packet(self, kind, body=b"", token=b"ab"):
        return b"\x02" + token + bytes([kind]) + self.eui + body

    def test_pull_ack_and_raw_downlink(self):
        self.server.handle_datagram(self.gateway_packet(2), self.client.getsockname())
        raw, _ = self.client.recvfrom(4096)
        self.assertEqual(raw, b"\x02ab\x04")
        self.server._send(Frame(DISCOVER, BROADCAST), "discover")
        raw, _ = self.client.recvfrom(4096)
        self.assertEqual(raw[3], 3)
        tx = json.loads(raw[4:])["txpk"]
        self.assertFalse(tx["ipol"])
        self.assertEqual(tx["freq"], 470.3)
        self.assertEqual(Frame.decode(base64.b64decode(tx["data"])).kind, DISCOVER)

    def test_push_status_and_start_broadcast(self):
        for n in [1001, 1002]:
            frame = Frame(
                STATUS,
                n,
                flags=3,
                epoch_us=int(time.time() * 1e6),
                payload=STATUS_PAYLOAD.pack(32000, 2.0, 0, 0, 0, 1, 0),
            )
            body = json.dumps(
                {
                    "rxpk": [
                        {
                            "stat": 1,
                            "datr": "SF6BW125",
                            "data": base64.b64encode(frame.encode()).decode(),
                        }
                    ]
                }
            ).encode()
            self.server.handle_datagram(self.gateway_packet(0, body), self.client.getsockname())
            self.assertEqual(self.client.recvfrom(1024)[0], b"\x02ab\x01")
        self.server.downlink_peer = self.client.getsockname()
        self.server.peer_seen = time.monotonic()
        self.server._request("start", {"rate": 50, "range_g": 4})
        self.assertEqual(len(self.server.downlinks), 3)
        f = self.server.downlinks[0][1]
        self.assertEqual(f.node, BROADCAST)
        self.assertEqual(START_OPTIONS.unpack(f.payload)[-1], 3)
        self.assertGreater(f.epoch_us, int(time.time() * 1e6) + 9_000_000)

    def test_stop_cancels_start_repeats(self):
        self.server._queue(start_frame(12, 1_800_000_000_000_000), "START")
        self.server._request("stop", {})
        self.assertEqual([item[1].kind for item in self.server.downlinks], [STOP])

    def test_start_rejects_unready_nodes(self):
        with self.assertRaises(ValueError):
            self.server._request("start", {})

    def test_bad_datagram_and_crc(self):
        for raw in [b"", b"x" * 12]:
            with self.assertRaises(ValueError):
                self.server.handle_datagram(raw, self.client.getsockname())

    def test_data_duplicate_still_gets_ack_bitmap(self):
        self.server.run_id = 77
        f = Frame(
            DATA,
            1001,
            flags=3,
            run=77,
            sequence=35,
            epoch_us=1_800_000_000_000_000,
            rate=50,
            count=1,
            payload=struct.pack("<hhh", 1, 2, 3),
        )
        self.server._frame(f, {})
        self.server.pending_acks.clear()
        self.server._frame(f, {})
        self.assertEqual(self.server.pending_acks[1001], {35})
        self.assertEqual(self.server.nodes[1001].samples, 1)

    def test_udp_worker_accepts_real_socket_packet(self):
        self.server.socket.close()
        self.server.socket = None
        self.server.start()
        self.client.sendto(self.gateway_packet(2), ("127.0.0.1", self.server.port))
        self.assertEqual(self.client.recvfrom(1024)[0], b"\x02ab\x04")


class GatewayConfigurationTests(unittest.TestCase):
    def test_preserves_calibration_and_tx_limits(self):
        from host.configure_gateway import configure

        factory = {
            "SX130x_conf": {
                "radio_0": {
                    "type": "SX1250",
                    "tx_enable": True,
                    "tx_freq_min": 500000000,
                    "tx_freq_max": 510000000,
                    "rssi_offset": -215.4,
                },
                "radio_1": {"type": "SX1250"},
                "tx_gain_lut": [{"pa_gain": 3}],
            },
            "gateway_conf": {"gateway_ID": "0011"},
        }
        result, supports = configure(factory, "192.168.1.100")
        self.assertFalse(supports)
        self.assertEqual(result["SX130x_conf"]["radio_0"]["tx_freq_min"], 500000000)
        self.assertEqual(result["SX130x_conf"]["radio_0"]["rssi_offset"], -215.4)
        self.assertEqual(result["gateway_conf"]["server_address"], "192.168.1.100")
        self.assertNotIn("server_address", factory["gateway_conf"])
        for i, frequency in enumerate(
            [470300000, 470500000, 470700000, 470900000, 471100000, 471300000, 471500000, 471700000]
        ):
            chan = result["SX130x_conf"][f"chan_multiSF_{i}"]
            self.assertEqual(
                result["SX130x_conf"][f'radio_{chan["radio"]}']["freq"] + chan["if"], frequency
            )

    def test_json_comments_do_not_damage_urls(self):
        from host.configure_gateway import strip_comments

        text = '// comment\n{"url":"https://example.com/a/*b*/", /* note */ "n":1}'
        self.assertEqual(
            json.loads(strip_comments(text)), {"url": "https://example.com/a/*b*/", "n": 1}
        )

    def test_factory_hardware_definition_required(self):
        from host.configure_gateway import configure

        with self.assertRaises(ValueError):
            configure({"SX130x_conf": {}, "gateway_conf": {}}, "127.0.0.1")

    def test_unsupported_tx_range_does_not_write_output(self):
        from host.configure_gateway import main

        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "factory.json"
            target = Path(tmp) / "generated.json"
            source.write_text(
                json.dumps(
                    {
                        "SX130x_conf": {
                            "radio_0": {
                                "type": "SX1250",
                                "tx_enable": True,
                                "tx_freq_min": 500000000,
                                "tx_freq_max": 510000000,
                            }
                        },
                        "gateway_conf": {},
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch(
                "sys.argv",
                [
                    "configure_gateway",
                    "--input",
                    str(source),
                    "--output",
                    str(target),
                    "--server",
                    "127.0.0.1",
                ],
            ):
                with self.assertRaises(SystemExit):
                    main()
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
