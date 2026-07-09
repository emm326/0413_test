#pragma once

#include <Arduino.h>

#include "config.h"

namespace Protocol {

constexpr uint16_t FRAME_MAGIC = 0xA55A;
constexpr uint8_t FRAME_VERSION = 0x02;
constexpr uint8_t FRAME_TYPE_VIBRATION_RAW = 0x01;
constexpr uint8_t FRAME_TYPE_START_AT = 0x02;
constexpr uint8_t FRAME_TYPE_POLL = 0x03;
constexpr uint8_t FRAME_TYPE_ACK = 0x04;
constexpr uint8_t FRAME_TYPE_NO_DATA = 0x05;
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
  uint32_t t0Us;
  uint16_t fsHzX10;
  uint8_t count;
  uint8_t reserved;
};

struct TxUsbCompactHeader {
  uint16_t devId;
  uint16_t seq;
  uint32_t t0Us;
  uint16_t fsHzX10;
  uint8_t count;
  uint8_t flags;
};

struct ControlFrame {
  uint16_t magic;
  uint8_t version;
  uint8_t frameType;
  uint16_t devId;
  uint16_t seq;
  uint64_t epochUs;
  uint32_t param;
  uint16_t fsHzX10;
  uint16_t reserved;
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
bool isControlFrameType(uint8_t frameType);
bool peekFrameType(const uint8_t* buffer, size_t length, uint8_t& outFrameType);
uint16_t crc16Ccitt(const uint8_t* data, size_t len);
size_t calcPacketSize(uint8_t sampleCount);
size_t calcTxUsbCompactPacketSize(uint8_t sampleCount, bool includeCrc = true);
size_t controlFrameSize();
bool shouldGrantBurstContinuation(bool pollWaiting,
                                  bool duplicatePacket,
                                  uint8_t receivedPacketsInBurst,
                                  uint8_t burstLimit);
bool encodePacket(uint16_t devId,
                  uint16_t seq,
                  uint32_t t0Us,
                  uint16_t fsHzX10,
                  const AccelRawSample* samples,
                  uint8_t count,
                  uint8_t* outBuffer,
                  size_t outCapacity,
                  size_t& outLength);
DecodeError decodePacket(const uint8_t* buffer,
                         size_t length,
                         DecodedPacketView& outView);
bool encodeControlFrame(uint8_t frameType,
                        uint16_t devId,
                        uint16_t seq,
                        uint64_t epochUs,
                        uint32_t param,
                        uint16_t fsHzX10,
                        uint16_t reserved,
                        uint8_t* outBuffer,
                        size_t outCapacity,
                        size_t& outLength);
DecodeError decodeControlFrame(const uint8_t* buffer,
                               size_t length,
                               ControlFrame& outFrame);

}  // namespace Protocol
