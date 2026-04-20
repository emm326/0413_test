#pragma once

#include <Arduino.h>

#include "config.h"

namespace Protocol {

constexpr uint16_t FRAME_MAGIC = 0xA55A;
constexpr uint8_t FRAME_VERSION = 0x01;
constexpr uint8_t FRAME_TYPE_VIBRATION_RAW = 0x01;
constexpr size_t MAX_PACKET_SIZE =
    64 + AppConfig::MAX_SAMPLES_PER_PACKET * sizeof(int16_t) * 3;

#pragma pack(push, 1)
struct AccelRawSample {
  int16_t ax;
  int16_t ay;
  int16_t az;
};

struct VibrationFrameHeader {
  uint16_t magic;
  uint8_t version;
  uint8_t frameType;
  uint16_t devId;
  uint16_t seq;
  uint32_t t0Ms;
  uint16_t fsHzX10;
  uint8_t count;
  uint8_t reserved;
};
#pragma pack(pop)

enum class DecodeError : uint8_t {
  None = 0,
  TooShort,
  BadMagic,
  BadVersion,
  BadFrameType,
  BadCount,
  LengthMismatch,
  CrcMismatch,
};

struct DecodedPacketView {
  VibrationFrameHeader header;
  const AccelRawSample* samples;
  uint16_t expectedCrc;
  uint16_t actualCrc;
};

const char* decodeErrorToString(DecodeError err);
uint16_t crc16Ccitt(const uint8_t* data, size_t len);
size_t calcPacketSize(uint8_t sampleCount);
bool encodePacket(uint16_t devId,
                  uint16_t seq,
                  uint32_t t0Ms,
                  uint16_t fsHzX10,
                  const AccelRawSample* samples,
                  uint8_t count,
                  uint8_t* outBuffer,
                  size_t outCapacity,
                  size_t& outLength);
DecodeError decodePacket(const uint8_t* buffer,
                         size_t length,
                         DecodedPacketView& outView);

}  // namespace Protocol
