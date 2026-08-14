import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class GpsPeriodicTimebaseTest(unittest.TestCase):
    def test_gps_timebase_exposes_reverse_periodic_mapping(self):
        header_text = (PROJECT_ROOT / "lib" / "GpsTimebase" / "src" / "GpsTimebase.h").read_text(
            encoding="utf-8"
        )
        cpp_text = (PROJECT_ROOT / "lib" / "GpsTimebase" / "src" / "GpsTimebase.cpp").read_text(
            encoding="utf-8"
        )

        self.assertIn("bool localMicrosToEpoch(uint32_t localUs, uint64_t& outEpochUs) const;", header_text)
        self.assertIn("uint32_t localUsAtLastPps() const;", header_text)
        self.assertIn("uint32_t timebaseAgeMs() const;", header_text)
        self.assertIn("lastMappedPpsLocalUs_", header_text)

        self.assertIn("bool GpsTimebase::localMicrosToEpoch", cpp_text)
        self.assertIn("const int64_t deltaUs", cpp_text)
        self.assertIn("lastMappedPpsLocalUs64_", cpp_text)
        self.assertIn("lastPpsEpochUs_ + deltaUs", cpp_text)

    def test_gps_timebase_uses_64_bit_local_timer_and_health_diagnostics(self):
        header_text = (PROJECT_ROOT / "lib" / "GpsTimebase" / "src" / "GpsTimebase.h").read_text(
            encoding="utf-8"
        )
        cpp_text = (PROJECT_ROOT / "lib" / "GpsTimebase" / "src" / "GpsTimebase.cpp").read_text(
            encoding="utf-8"
        )

        self.assertIn("uint64_t localUs64AtLastPps() const;", header_text)
        self.assertIn("uint32_t ppsBadCount() const;", header_text)
        self.assertIn("uint32_t localTimerJumpCount() const;", header_text)
        self.assertIn("bool healthy() const;", header_text)
        self.assertIn("volatile uint64_t lastPpsLocalUs64_", header_text)

        self.assertIn("#include <esp_timer.h>", cpp_text)
        self.assertIn("esp_timer_get_time()", cpp_text)
        self.assertIn("lastPpsLocalUs64_", cpp_text)
        self.assertIn("ppsBadCount_", cpp_text)
        self.assertIn("localTimerJumpCount_", cpp_text)

    def test_gps_timebase_rejects_stale_nmea_pps_epoch_regression(self):
        cpp_text = (PROJECT_ROOT / "lib" / "GpsTimebase" / "src" / "GpsTimebase.cpp").read_text(
            encoding="utf-8"
        )

        self.assertIn("candidatePpsEpochUs", cpp_text)
        self.assertIn("candidatePpsEpochUs <= lastPpsEpochUs_", cpp_text)
        self.assertIn("return;", cpp_text)

    def test_p2p_tx_packet_offsets_use_latest_gps_pps_mapping(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        offset_fn = self._function_body(
            main_text, "bool txBatchStartOffsetUs(uint32_t batchStartUs, uint32_t& outOffsetUs)"
        )
        send_batch_fn = self._function_body(main_text, "bool sendCurrentBatch()")

        self.assertIn("gpsTimebase.localMicrosToEpoch(batchStartUs, sampleEpochUs)", offset_fn)
        self.assertIn("sampleEpochUs < txRunStartEpochUs", offset_fn)
        self.assertIn("sampleEpochUs - txRunStartEpochUs", offset_fn)
        self.assertIn("outOffsetUs", offset_fn)

        self.assertIn("uint32_t batchStartOffsetUs = 0;", send_batch_fn)
        self.assertIn("txBatchStartOffsetUs(batchStartUs, batchStartOffsetUs)", send_batch_fn)
        self.assertIn("TX dropped timebase batch", send_batch_fn)
        self.assertIn("batchStartUs = batchStartOffsetUs;", send_batch_fn)

    def test_usb_gps_sync_raw_output_uses_per_sample_gps_mapped_int1_timestamps(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        send_batch_fn = self._function_body(main_text, "bool sendCurrentBatch()")

        self.assertIn("#if defined(APP_TX_USB_STREAM_TEST)", send_batch_fn)
        self.assertIn("sampleTimestampBuffer[i]", send_batch_fn)
        self.assertIn("gpsTimebase.localMicrosToEpoch(sampleTimestampBuffer[i], sampleEpochUs)", send_batch_fn)
        self.assertIn("const bool queued =", send_batch_fn)

        usb_start = send_batch_fn.index(
            "#if defined(APP_TX_USB_STREAM_TEST)",
            send_batch_fn.index("Protocol::encodePacket"),
        )
        p2p_marker = "#else\n  const bool queued ="
        usb_branch = send_batch_fn[
            usb_start:
            send_batch_fn.index(p2p_marker)
        ]
        self.assertNotIn("txBatchStartOffsetUs", usb_branch)
        self.assertNotIn("batchStartUs - txRunStartLocalUs", usb_branch)
        self.assertNotIn("sampleIntervalUsRounded(txUsbSamples + i", usb_branch)

    def test_pending_batch_stores_timestamp_for_each_sample(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        process_fn = self._function_body(main_text, "void processFifoSamples()")

        self.assertIn("uint32_t sampleTimestampBuffer[AppConfig::MAX_SAMPLES_PER_PACKET]", main_text)
        self.assertIn("sampleTimestampBuffer[pendingSamples] = sampleTimestampUs;", process_fn)

    def test_iim_fifo_raw_timestamps_are_reconstructed_from_burst_anchor(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        process_fn = self._function_body(main_text, "void processFifoSamples()")
        reconstruct_fn = self._function_body(
            main_text,
            "bool reconstructIimFifoSampleTimestamps(size_t sampleCount,",
        )

        self.assertIn("collectIimInt1TimestampBurst(sampleCount, fifoAnchorUs, anchorUs)", reconstruct_fn)
        self.assertIn("const uint32_t readLagSamples", reconstruct_fn)
        self.assertIn("fifoDepth > sampleCount", reconstruct_fn)
        self.assertIn(
            "sampleIntervalUsRounded(static_cast<uint32_t>(sampleCount - 1U)",
            reconstruct_fn,
        )
        self.assertIn("outTimestamps[i] =", reconstruct_fn)
        self.assertIn("nextIimSampleTimelineTimestamp()", reconstruct_fn)
        self.assertIn("if (!iimSampleTimelineValid)", reconstruct_fn)
        self.assertIn("iimSampleTimelineStartUs = firstSampleUs;", reconstruct_fn)
        self.assertNotIn("popIimInt1Timestamp(sampleTimestampUs)", process_fn)
        self.assertIn("const uint32_t fifoAnchorUs = iimInt1LastEdgeTimestampUs();", process_fn)

    def test_iim_raw_sample_timeline_resets_with_int1_state(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        clear_fn = self._function_body(main_text, "void clearIimInt1Timestamps()")
        next_fn = self._function_body(main_text, "uint32_t nextIimSampleTimelineTimestamp()")

        self.assertIn("bool iimSampleTimelineValid = false;", main_text)
        self.assertIn("resetIimSampleTimeline();", clear_fn)
        self.assertIn("iimSampleTimelineStartUs +", next_fn)
        self.assertIn("sampleIntervalUsRounded(iimSampleTimelineNextIndex", next_fn)
        self.assertIn("++iimSampleTimelineNextIndex;", next_fn)

    def test_tx_logs_periodic_gps_sync_updates(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        update_fn = self._function_body(main_text, "void updateGpsTimebase()")

        self.assertIn("GPS sync:", update_fn)
        self.assertIn("gpsTimebase.epochUsAtLastPps()", update_fn)
        self.assertIn("gpsTimebase.localUsAtLastPps()", update_fn)
        self.assertIn("gpsTimebase.timebaseAgeMs()", update_fn)

    def test_usb_gps_sync_mode_requires_and_uses_gps_timebase(self):
        platformio_text = (PROJECT_ROOT / "platformio.ini").read_text(encoding="utf-8")
        config_text = (PROJECT_ROOT / "include" / "config.h").read_text(encoding="utf-8")
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        arm_usb_fn = self._function_body(main_text, "void armTxRunUsb")
        setup_fn = self._function_body(main_text, "void setup()")
        loop_fn = self._function_body(main_text, "void txLoop()")
        send_batch_fn = self._function_body(main_text, "bool sendCurrentBatch()")

        self.assertIn("APP_TX_USB_GPS_SYNC_TEST", platformio_text)
        self.assertIn("heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test", platformio_text)
        self.assertIn("heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test", platformio_text)
        self.assertIn("defined(APP_TX_USB_GPS_SYNC_TEST)", config_text)

        self.assertIn("#if defined(APP_TX_USB_GPS_SYNC_TEST)", arm_usb_fn)
        self.assertIn("gpsTimebase.localMicrosForEpoch(startEpochUs, startLocalUs)", arm_usb_fn)
        self.assertIn("TX USB ignored START_AT waiting GPS/PPS", arm_usb_fn)
        self.assertIn("txRunStartLocalUs = startLocalUs;", arm_usb_fn)

        self.assertIn("!defined(APP_TX_USB_STREAM_TEST) || defined(APP_TX_USB_GPS_SYNC_TEST)", setup_fn)
        self.assertIn("!defined(APP_TX_USB_STREAM_TEST) || defined(APP_TX_USB_GPS_SYNC_TEST)", loop_fn)
        self.assertIn("TXG,%u,%llu,%lu,%lu,%lu", send_batch_fn)

    def test_tx_usb_outputs_timebase_diagnostic_and_rejects_unhealthy_start(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        arm_usb_fn = self._function_body(main_text, "void armTxRunUsb")
        send_batch_fn = self._function_body(main_text, "bool sendCurrentBatch()")

        self.assertIn("gpsTimebase.healthy()", arm_usb_fn)
        self.assertIn("TX USB ignored START_AT unhealthy timebase", arm_usb_fn)
        self.assertIn("TIMEBASE,%u,%u,%llu,%llu,%lu,%lu,%lu", send_batch_fn)
        self.assertIn("gpsTimebase.localUs64AtLastPps()", send_batch_fn)
        self.assertIn("gpsTimebase.ppsBadCount()", send_batch_fn)
        self.assertIn("gpsTimebase.localTimerJumpCount()", send_batch_fn)

    def test_p2p_capture_waits_for_target_pps_and_preserves_pre_gate_fifo_window(self):
        main_text = (PROJECT_ROOT / "src" / "main.cpp").read_text(encoding="utf-8")
        start_fn = self._function_body(main_text, "void startTxCaptureIfDue()")
        loop_fn = self._function_body(main_text, "void txLoop()")

        self.assertIn("TX PPS gate opened", start_fn)
        self.assertIn("ppsEpochUs < txRunStartEpochUs", start_fn)
        self.assertIn("txRunStartLocalUs = ppsLocalUs", start_fn)
        self.assertIn("kPpsStartFifoGuardUs", main_text)
        self.assertIn("discardSensorFifo(false);", loop_fn)
        self.assertIn("clearIimInt1Timestamps();", loop_fn)

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
