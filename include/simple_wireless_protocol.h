#pragma once
#include <stddef.h>
#include <stdint.h>
#include "protocol.h"

namespace SimpleWireless {
constexpr uint16_t MAGIC = 0x5357;
constexpr uint8_t VERSION = 1;
constexpr uint16_t DEVICE_ID = 1001;
constexpr uint16_t SAMPLE_RATE_HZ = 50;
constexpr uint32_t SAMPLE_PERIOD_US = 1000000UL / SAMPLE_RATE_HZ;
constexpr uint8_t RANGE_G = 4;
constexpr uint8_t SAMPLES_PER_PACKET = 10;
enum class Status : uint8_t { Data = 0, SensorInitFailed = 1, SensorStalled = 2 };
using Sample = Protocol::AccelRawSample;
#pragma pack(push, 1)
struct Header {
  uint16_t magic;
  uint8_t version;
  uint8_t status;
  uint16_t deviceId;
  uint16_t seq;
  uint64_t firstSampleUs;  // TX-local sample timeline, not UTC/GPS time.
  uint32_t samplePeriodUs;
  uint8_t count;
  uint8_t rangeG;
  uint8_t sensorWho;
};
#pragma pack(pop)
static_assert(sizeof(Header) == 23, "Unexpected simple packet layout");
constexpr size_t MAX_PACKET_SIZE = sizeof(Header) + sizeof(Sample) * SAMPLES_PER_PACKET + 2;
struct Packet {
  Header header;
  Sample samples[SAMPLES_PER_PACKET];
};
bool encode(Status status, uint8_t sensorWho, uint16_t seq, uint64_t firstSampleUs,
            const Sample* samples, uint8_t count, uint8_t* output, size_t capacity,
            size_t& length);
bool decode(const uint8_t* input, size_t length, Packet& packet);
}  // namespace SimpleWireless
