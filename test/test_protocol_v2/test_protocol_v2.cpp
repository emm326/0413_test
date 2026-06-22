#include <unity.h>

#include "protocol.h"

void test_vibration_frame_uses_version_2_and_microsecond_t0() {
  Protocol::AccelRawSample samples[2] = {
      {1, -2, 3},
      {4, -5, 6},
  };
  uint8_t buffer[Protocol::MAX_PACKET_SIZE] = {};
  size_t length = 0;

  TEST_ASSERT_TRUE(Protocol::encodePacket(
      1001, 42, 1234567UL, 1000, samples, 2, buffer, sizeof(buffer), length));

  Protocol::DecodedPacketView packet = {};
  TEST_ASSERT_EQUAL(Protocol::DecodeError::None,
                    Protocol::decodePacket(buffer, length, packet));
  TEST_ASSERT_EQUAL_UINT8(2, packet.header.version);
  TEST_ASSERT_EQUAL_UINT32(1234567UL, packet.header.t0Us);
  TEST_ASSERT_EQUAL_INT16(-5, packet.samples[1].ay);
}

void test_max_sample_packet_fits_lora_payload() {
  Protocol::AccelRawSample samples[AppConfig::MAX_SAMPLES_PER_PACKET] = {};
  for (uint8_t i = 0; i < AppConfig::MAX_SAMPLES_PER_PACKET; ++i) {
    samples[i] = {
        static_cast<int16_t>(i),
        static_cast<int16_t>(-i),
        static_cast<int16_t>(i * 2),
    };
  }

  uint8_t buffer[Protocol::MAX_PACKET_SIZE] = {};
  size_t length = 0;
  TEST_ASSERT_TRUE(Protocol::encodePacket(
      1002,
      77,
      9876543UL,
      1000,
      samples,
      AppConfig::MAX_SAMPLES_PER_PACKET,
      buffer,
      sizeof(buffer),
      length));

  TEST_ASSERT_LESS_OR_EQUAL_UINT16(255, length);

  Protocol::DecodedPacketView packet = {};
  TEST_ASSERT_EQUAL(Protocol::DecodeError::None,
                    Protocol::decodePacket(buffer, length, packet));
  TEST_ASSERT_EQUAL_UINT8(AppConfig::MAX_SAMPLES_PER_PACKET, packet.header.count);
  TEST_ASSERT_EQUAL_INT16(
      static_cast<int16_t>(AppConfig::MAX_SAMPLES_PER_PACKET - 1),
      packet.samples[AppConfig::MAX_SAMPLES_PER_PACKET - 1].ax);
}

void test_ack_grant_burst_policy() {
  TEST_ASSERT_TRUE(Protocol::shouldGrantBurstContinuation(true, false, 1, 6));
  TEST_ASSERT_TRUE(Protocol::shouldGrantBurstContinuation(true, false, 5, 6));
  TEST_ASSERT_FALSE(Protocol::shouldGrantBurstContinuation(true, false, 6, 6));
  TEST_ASSERT_FALSE(Protocol::shouldGrantBurstContinuation(true, true, 1, 6));
  TEST_ASSERT_FALSE(Protocol::shouldGrantBurstContinuation(false, false, 1, 6));
  TEST_ASSERT_FALSE(Protocol::shouldGrantBurstContinuation(true, false, 1, 0));
}

void test_control_frame_preserves_ack_grant_param() {
  uint8_t buffer[Protocol::MAX_PACKET_SIZE] = {};
  size_t length = 0;

  TEST_ASSERT_TRUE(Protocol::encodeControlFrame(
      Protocol::FRAME_TYPE_ACK, 1001, 123, 0, 1, 1000, buffer, sizeof(buffer), length));

  Protocol::ControlFrame frame = {};
  TEST_ASSERT_EQUAL(Protocol::DecodeError::None,
                    Protocol::decodeControlFrame(buffer, length, frame));
  TEST_ASSERT_EQUAL_UINT8(Protocol::FRAME_TYPE_ACK, frame.frameType);
  TEST_ASSERT_EQUAL_UINT16(1001, frame.devId);
  TEST_ASSERT_EQUAL_UINT16(123, frame.seq);
  TEST_ASSERT_EQUAL_UINT32(1, frame.param);
}

void test_tx_usb_compact_packet_size_uses_12_byte_header_plus_crc() {
  TEST_ASSERT_EQUAL_UINT16(12, sizeof(Protocol::TxUsbCompactHeader));
  TEST_ASSERT_EQUAL_UINT16(158, Protocol::calcTxUsbCompactPacketSize(24));
  TEST_ASSERT_EQUAL_UINT16(236, Protocol::calcTxUsbCompactPacketSize(37));
  TEST_ASSERT_EQUAL_UINT16(156, Protocol::calcTxUsbCompactPacketSize(24, false));
}

void setup() {
  UNITY_BEGIN();
  RUN_TEST(test_vibration_frame_uses_version_2_and_microsecond_t0);
  RUN_TEST(test_max_sample_packet_fits_lora_payload);
  RUN_TEST(test_ack_grant_burst_policy);
  RUN_TEST(test_control_frame_preserves_ack_grant_param);
  RUN_TEST(test_tx_usb_compact_packet_size_uses_12_byte_header_plus_crc);
  UNITY_END();
}

void loop() {}
