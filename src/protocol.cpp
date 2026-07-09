#include "protocol.h"

#include <string.h>

namespace Protocol {

namespace {

constexpr size_t kHeaderSize = sizeof(VibrationFrameHeader);
constexpr size_t kControlFrameSize = sizeof(ControlFrame);
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

bool isControlFrameType(uint8_t frameType) {
  return frameType == FRAME_TYPE_START_AT || frameType == FRAME_TYPE_POLL ||
         frameType == FRAME_TYPE_ACK || frameType == FRAME_TYPE_NO_DATA;
}

bool peekFrameType(const uint8_t* buffer, size_t length, uint8_t& outFrameType) {
  outFrameType = 0;
  if (buffer == nullptr || length < 4) {
    return false;
  }

  uint16_t magic = 0;
  memcpy(&magic, buffer, sizeof(magic));
  if (magic != FRAME_MAGIC || buffer[2] != FRAME_VERSION) {
    return false;
  }

  outFrameType = buffer[3];
  return true;
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

size_t calcTxUsbCompactPacketSize(uint8_t sampleCount, bool includeCrc) {
  return sizeof(TxUsbCompactHeader) +
         static_cast<size_t>(sampleCount) * sizeof(AccelRawSample) +
         (includeCrc ? kCrcSize : 0U);
}

size_t controlFrameSize() { return kControlFrameSize + kCrcSize; }

bool shouldGrantBurstContinuation(bool pollWaiting,
                                  bool duplicatePacket,
                                  uint8_t receivedPacketsInBurst,
                                  uint8_t burstLimit) {
  if (!pollWaiting || duplicatePacket || burstLimit == 0) {
    return false;
  }
  return receivedPacketsInBurst < burstLimit;
}

bool encodePacket(uint16_t devId,
                  uint16_t seq,
                  uint32_t t0Us,
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
  header.t0Us = t0Us;
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

bool encodeControlFrame(uint8_t frameType,
                        uint16_t devId,
                        uint16_t seq,
                        uint64_t epochUs,
                        uint32_t param,
                        uint16_t fsHzX10,
                        uint16_t reserved,
                        uint8_t* outBuffer,
                        size_t outCapacity,
                        size_t& outLength) {
  outLength = 0;
  if (outBuffer == nullptr || !isControlFrameType(frameType)) {
    return false;
  }

  const size_t frameSize = controlFrameSize();
  if (frameSize > outCapacity) {
    return false;
  }

  ControlFrame frame = {};
  frame.magic = FRAME_MAGIC;
  frame.version = FRAME_VERSION;
  frame.frameType = frameType;
  frame.devId = devId;
  frame.seq = seq;
  frame.epochUs = epochUs;
  frame.param = param;
  frame.fsHzX10 = fsHzX10;
  frame.reserved = reserved;

  memcpy(outBuffer, &frame, sizeof(frame));
  const uint16_t crc = crc16Ccitt(outBuffer, frameSize - kCrcSize);
  memcpy(outBuffer + frameSize - kCrcSize, &crc, sizeof(crc));

  outLength = frameSize;
  return true;
}

DecodeError decodeControlFrame(const uint8_t* buffer,
                               size_t length,
                               ControlFrame& outFrame) {
  if (buffer == nullptr || length < controlFrameSize()) {
    return DecodeError::TooShort;
  }

  if (length != controlFrameSize()) {
    return DecodeError::LengthMismatch;
  }

  ControlFrame frame = {};
  memcpy(&frame, buffer, sizeof(frame));

  if (frame.magic != FRAME_MAGIC) {
    return DecodeError::BadMagic;
  }
  if (frame.version != FRAME_VERSION) {
    return DecodeError::BadVersion;
  }
  if (!isControlFrameType(frame.frameType)) {
    return DecodeError::BadFrameType;
  }

  uint16_t actualCrc = 0;
  memcpy(&actualCrc, buffer + length - kCrcSize, sizeof(actualCrc));
  const uint16_t expectedCrc = crc16Ccitt(buffer, length - kCrcSize);
  if (actualCrc != expectedCrc) {
    return DecodeError::CrcMismatch;
  }

  outFrame = frame;
  return DecodeError::None;
}

}  // namespace Protocol
