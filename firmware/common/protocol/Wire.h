#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string.h>
namespace WireProtocol {
constexpr uint16_t magic = 0xa55a;
constexpr uint8_t version = 3;
constexpr uint16_t broadcast = 0xffff;
constexpr size_t maxPacket = 255;
enum Type : uint8_t {
  Data = 1,
  Start = 2,
  Stop = 3,
  Confirm = 4,
  Status = 5,
  Discover = 6,
  Ack = 7
};
enum Flag : uint16_t {
  GpsHealthy = 1,
  ClockHealthy = 2,
  SampleGap = 4,
  QueueOverflow = 8,
  Tail = 16
};
enum State : uint8_t { Boot = 0, Waiting = 1, Armed = 2, Capturing = 3, Draining = 4, Fault = 5 };
enum Result : uint8_t { Accepted = 0, NotReady = 1, BadSettings = 2, Busy = 3, TooLate = 4 };
#pragma pack(push, 1)
struct Header {
  uint16_t magicValue;
  uint8_t wireVersion, type;
  uint16_t node, flags;
  uint32_t run, sequence;
  uint64_t epochUs;
  uint16_t rate;
  uint8_t count, range;
};
struct Sample {
  int16_t x, y, z;
};
struct StartOptions {
  float uplinkMHz;
  uint32_t durationSeconds;
  uint8_t sf, reserved;
  uint16_t nodeMask;
};
struct StatusPayload {
  float clockHz, clockPpm;
  uint32_t lostSamples, lostPackets;
  uint16_t queued;
  uint8_t state, result;
};
struct AckEntry {
  uint16_t node;
  uint32_t firstSequence, bitmap;
};
#pragma pack(pop)
static_assert(sizeof(Header) == 28, "Wire header must be 28 bytes");
static_assert(sizeof(Sample) == 6, "XYZ must be int16");
static_assert(sizeof(Header) + 25 * sizeof(Sample) + 2 == 180,
              "Normal data packet must be 180 bytes");
uint16_t crc16(const uint8_t* data, size_t length);
size_t encode(Header header, const void* payload, size_t payloadBytes, uint8_t* output);
bool decode(const uint8_t* data, size_t length, Header& header, const uint8_t*& payload,
            size_t& payloadBytes);
}  // namespace WireProtocol
