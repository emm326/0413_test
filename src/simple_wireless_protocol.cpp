#include "simple_wireless_protocol.h"
#include <string.h>
namespace SimpleWireless {
namespace {
bool validStatusAndCount(uint8_t status, uint8_t count) {
  if (status == static_cast<uint8_t>(Status::Data)) {
    return count > 0 && count <= SAMPLES_PER_PACKET;
  }
  return count == 0 && (status == static_cast<uint8_t>(Status::SensorInitFailed) ||
                        status == static_cast<uint8_t>(Status::SensorStalled));
}
}
bool encode(Status status, uint8_t sensorWho, uint16_t seq, uint64_t firstSampleUs,
            const Sample* samples, uint8_t count, uint8_t* output, size_t capacity,
            size_t& length) {
  length = 0;
  if (!output || !validStatusAndCount(static_cast<uint8_t>(status), count) ||
      (count && !samples)) return false;
  const size_t bytes = sizeof(Header) + sizeof(Sample) * count + sizeof(uint16_t);
  if (bytes > capacity) return false;
  Header header = {MAGIC, VERSION, static_cast<uint8_t>(status), DEVICE_ID, seq,
                   firstSampleUs, SAMPLE_PERIOD_US, count, RANGE_G, sensorWho};
  memcpy(output, &header, sizeof(header));
  if (count) memcpy(output + sizeof(header), samples, sizeof(Sample) * count);
  const uint16_t crc = Protocol::crc16Ccitt(output, bytes - sizeof(crc));
  memcpy(output + bytes - sizeof(crc), &crc, sizeof(crc));
  length = bytes;
  return true;
}
bool decode(const uint8_t* input, size_t length, Packet& packet) {
  if (!input || length < sizeof(Header) + sizeof(uint16_t)) return false;
  Header header;
  memcpy(&header, input, sizeof(header));
  if (header.magic != MAGIC || header.version != VERSION || header.deviceId != DEVICE_ID ||
      header.samplePeriodUs != SAMPLE_PERIOD_US || header.rangeG != RANGE_G ||
      !validStatusAndCount(header.status, header.count)) return false;
  const size_t expected = sizeof(Header) + sizeof(Sample) * header.count + sizeof(uint16_t);
  if (length != expected) return false;
  uint16_t crc;
  memcpy(&crc, input + length - sizeof(crc), sizeof(crc));
  if (crc != Protocol::crc16Ccitt(input, length - sizeof(crc))) return false;
  packet = {};
  packet.header = header;
  if (header.count) memcpy(packet.samples, input + sizeof(header), sizeof(Sample) * header.count);
  return true;
}
}  // namespace SimpleWireless
