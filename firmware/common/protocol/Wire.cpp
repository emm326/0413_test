#include "Wire.h"
namespace WireProtocol {
uint16_t crc16(const uint8_t* data, size_t length) {
  uint16_t crc = 0xffff;
  for (size_t i = 0; i < length; ++i) {
    crc ^= uint16_t(data[i]) << 8;
    for (unsigned b = 0; b < 8; ++b)
      crc = crc & 0x8000 ? (crc << 1) ^ 0x1021 : crc << 1;
  }
  return crc;
}
size_t encode(Header h, const void* payload, size_t bytes, uint8_t* out) {
  const size_t length = sizeof(h) + bytes + 2;
  if (length > maxPacket)
    return 0;
  h.magicValue = magic;
  h.wireVersion = version;
  memcpy(out, &h, sizeof(h));
  if (bytes)
    memcpy(out + sizeof(h), payload, bytes);
  const uint16_t crc = crc16(out, length - 2);
  out[length - 2] = crc & 255;
  out[length - 1] = crc >> 8;
  return length;
}
bool decode(const uint8_t* data, size_t length, Header& h, const uint8_t*& payload, size_t& bytes) {
  if (length < sizeof(h) + 2 || length > maxPacket)
    return false;
  memcpy(&h, data, sizeof(h));
  if (h.magicValue != magic || h.wireVersion != version || h.type < Data || h.type > Ack)
    return false;
  if (crc16(data, length - 2) != (uint16_t(data[length - 2]) | (uint16_t(data[length - 1]) << 8)))
    return false;
  bytes = length - sizeof(h) - 2;
  payload = data + sizeof(h);
  return h.type != Data ||
         (h.count > 0 && h.count <= 25 && bytes == size_t(h.count) * sizeof(Sample) &&
          (h.rate == 50 || h.rate == 100));
}
}  // namespace WireProtocol
