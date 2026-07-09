import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class P2pTxQueuePolicyTest(unittest.TestCase):
    def test_p2p_queue_capacity_is_256_and_uses_16_bit_counters(self):
        platformio_text = (PROJECT_ROOT / "platformio.ini").read_text(encoding="utf-8")
        config_text = (PROJECT_ROOT / "include" / "config.h").read_text(encoding="utf-8")
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")

        p2p_flags = platformio_text[
            platformio_text.index("p2p_common_build_flags =") :
            platformio_text.index("p2p_tx_build_flags =")
        ]
        self.assertIn("-D APP_TX_QUEUE_PACKET_CAPACITY_VALUE=256", p2p_flags)
        self.assertIn("constexpr uint16_t TX_QUEUE_PACKET_CAPACITY", config_text)
        self.assertIn("uint16_t txQueueHead = 0;", main_text)
        self.assertIn("uint16_t txQueueCount = 0;", main_text)

    def test_send_success_pops_queue_before_waiting_for_ack(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        send_fn = self._function_body(main_text, "bool sendTxQueueHead()")

        self.assertIn("const uint16_t qBefore = txQueueCount;", send_fn)
        self.assertIn("const uint16_t sentSeq = item.seq;", send_fn)
        self.assertIn("popTxQueueFront();", send_fn)
        self.assertIn("txWaitingAck = true;", send_fn)
        self.assertIn("q_after_pop=%u", send_fn)
        self.assertLess(send_fn.index("popTxQueueFront();"), send_fn.index("txWaitingAck = true;"))

    def test_p2p_tx_uses_poll_granted_streaming_burst(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        handler_fn = self._function_body(main_text, "void handleTxControlFrame")
        poll_section = handler_fn[
            handler_fn.index("FRAME_TYPE_POLL") :
            handler_fn.index("if (frame.frameType == Protocol::FRAME_TYPE_ACK")
        ]
        stream_fn = self._function_body(main_text, "uint8_t sendTxStreamBurst")

        self.assertIn("sendTxStreamBurst(frame.param)", poll_section)
        self.assertNotIn("txWaitingAck", poll_section)
        self.assertIn("TX stream sent", stream_fn)
        self.assertNotIn("txWaitingAck", stream_fn)

    def test_rx_streaming_burst_does_not_send_per_packet_ack(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        rx_fn = self._function_body(main_text, "void handleRxDataPacket")

        self.assertIn("RX stream:", rx_fn)
        self.assertNotIn("FRAME_TYPE_ACK", rx_fn)
        self.assertNotIn("grantNext", rx_fn)

    def test_tx_timestamp_offset_is_guarded_against_pre_run_underflow(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        send_batch_fn = self._function_body(main_text, "bool sendCurrentBatch()")
        process_fn = self._function_body(main_text, "void processFifoSamples()")

        self.assertIn("txBatchStartOffsetUs(batchStartUs, batchStartOffsetUs)", send_batch_fn)
        self.assertIn("TX dropped timebase batch", send_batch_fn)
        self.assertIn("TX dropped pre-run batch", send_batch_fn)
        self.assertIn("txTimestampBeforeRunStart(sampleTimestampUs)", process_fn)
        self.assertIn("TX dropped pre-run sample", process_fn)

    @staticmethod
    def _function_body(source: str, signature: str) -> str:
        start = source.index(signature)
        brace_start = source.index("{", start)
        depth = 0
        for index in range(brace_start, len(source)):
            char = source[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return source[start : index + 1]
        raise AssertionError(f"Function not found: {signature}")


if __name__ == "__main__":
    unittest.main()
