#include "protocol.h"

#include <string.h>

namespace Protocol {

namespace {

constexpr size_t kHeaderSize = sizeof(VibrationFrameHeader);
constexpr size_t kCrcSize = sizeof(uint16_t);

}  // namespace

const char* decodeErrorToString(DecodeError err) {
  switch (err) {
    case DecodeError::None:
      return "ok";
    case DecodeError::TooShort:
      return "packet too short";
    case DecodeError::BadMagic:
      return "bad frame magic";
    case DecodeError::BadVersion:
      return "bad frame version";
    case DecodeError::BadFrameType:
      return "bad frame type";
    case DecodeError::BadCount:
      return "invalid sample count";
    case DecodeError::LengthMismatch:
      return "packet length mismatch";
    case DecodeError::CrcMismatch:
      return "crc16 mismatch";
    default:
      return "unknown error";
  }
}

uint16_t crc16Ccitt(const uint8_t* data, size_t len) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint16_t>(data[i]) << 8;
    for (uint8_t bit = 0; bit < 8; ++bit) {
      if (crc & 0x8000) {
        crc = static_cast<uint16_t>((crc << 1) ^ 0x1021);
      } else {
        crc <<= 1;
      }
    }
  }
  return crc;
}

size_t calcPacketSize(uint8_t sampleCount) {
  return kHeaderSize + static_cast<size_t>(sampleCount) * sizeof(AccelRawSample) +
         kCrcSize;
}

bool encodePacket(uint16_t devId,
                  uint16_t seq,
                  uint32_t t0Ms,
                  uint16_t fsHzX10,
                  const AccelRawSample* samples,
                  uint8_t count,
                  uint8_t* outBuffer,
                  size_t outCapacity,
                  size_t& outLength) {
  outLength = 0;
  if (samples == nullptr || outBuffer == nullptr || count == 0 ||
      count > AppConfig::MAX_SAMPLES_PER_PACKET) {
    return false;
  }

  const size_t packetSize = calcPacketSize(count);
  if (packetSize > outCapacity) {
    return false;
  }

  VibrationFrameHeader header = {};
  header.magic = FRAME_MAGIC;
  header.version = FRAME_VERSION;
  header.frameType = FRAME_TYPE_VIBRATION_RAW;
  header.devId = devId;
  header.seq = seq;
  header.t0Ms = t0Ms;
  header.fsHzX10 = fsHzX10;
  header.count = count;
  header.reserved = 0;

  memcpy(outBuffer, &header, sizeof(header));
  memcpy(outBuffer + sizeof(header), samples, sizeof(AccelRawSample) * count);

  const uint16_t crc = crc16Ccitt(outBuffer, packetSize - kCrcSize);
  memcpy(outBuffer + packetSize - kCrcSize, &crc, sizeof(crc));

  outLength = packetSize;
  return true;
}

DecodeError decodePacket(const uint8_t* buffer,
                         size_t length,
                         DecodedPacketView& outView) {
  if (buffer == nullptr || length < calcPacketSize(1)) {
    return DecodeError::TooShort;
  }

  VibrationFrameHeader header = {};
  memcpy(&header, buffer, sizeof(header));

  if (header.magic != FRAME_MAGIC) {
    return DecodeError::BadMagic;
  }
  if (header.version != FRAME_VERSION) {
    return DecodeError::BadVersion;
  }
  if (header.frameType != FRAME_TYPE_VIBRATION_RAW) {
    return DecodeError::BadFrameType;
  }
  if (header.count == 0 || header.count > AppConfig::MAX_SAMPLES_PER_PACKET) {
    return DecodeError::BadCount;
  }

  const size_t expectedLength = calcPacketSize(header.count);
  if (length != expectedLength) {
    return DecodeError::LengthMismatch;
  }

  uint16_t actualCrc = 0;
  memcpy(&actualCrc, buffer + length - kCrcSize, sizeof(actualCrc));
  const uint16_t expectedCrc = crc16Ccitt(buffer, length - kCrcSize);
  if (actualCrc != expectedCrc) {
    outView.expectedCrc = expectedCrc;
    outView.actualCrc = actualCrc;
    return DecodeError::CrcMismatch;
  }

  outView.header = header;
  outView.samples = reinterpret_cast<const AccelRawSample*>(buffer + sizeof(header));
  outView.expectedCrc = expectedCrc;
  outView.actualCrc = actualCrc;
  return DecodeError::None;
}

}  // namespace Protocol
