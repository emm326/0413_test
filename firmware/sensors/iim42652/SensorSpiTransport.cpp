#include "SensorSpiTransport.h"

namespace {
constexpr uint32_t kSpiHz = 100000;
constexpr uint8_t kSpiMode = SPI_MODE0;
}  // namespace

SensorSpiTransport::SensorSpiTransport(int sck, int miso, int mosi, int cs)
    : spi_(HSPI), sck_(sck), miso_(miso), mosi_(mosi), cs_(cs) {}

bool SensorSpiTransport::begin() {
  end();

  // Keep CS inactive before changing its direction.
  digitalWrite(cs_, HIGH);
  pinMode(cs_, OUTPUT);
  digitalWrite(cs_, HIGH);

  // The sensor CS is driven by software, so the hardware bus has no CS pin.
  spi_.begin(sck_, miso_, mosi_, -1);
  delayMicroseconds(20);
  started_ = true;
  return true;
}

void SensorSpiTransport::end() {
  if (started_) {
    spi_.end();
  }
  digitalWrite(cs_, HIGH);
  pinMode(cs_, OUTPUT);
  digitalWrite(cs_, HIGH);
  started_ = false;
}

void SensorSpiTransport::select(bool active) {
  digitalWrite(cs_, active ? LOW : HIGH);
}

int SensorSpiTransport::read(uint8_t reg, uint8_t* buffer, uint32_t length) {
  if (!started_ || buffer == nullptr || length == 0) {
    return -1;
  }

  spi_.beginTransaction(SPISettings(kSpiHz, MSBFIRST, kSpiMode));
  select(true);
  delayMicroseconds(1);
  spi_.transfer(static_cast<uint8_t>(reg | 0x80));
  for (uint32_t i = 0; i < length; ++i) {
    buffer[i] = spi_.transfer(0);
  }
  delayMicroseconds(1);
  select(false);
  spi_.endTransaction();
  return 0;
}

int SensorSpiTransport::write(uint8_t reg, const uint8_t* buffer,
                              uint32_t length) {
  if (!started_ || buffer == nullptr || length == 0) {
    return -1;
  }

  spi_.beginTransaction(SPISettings(kSpiHz, MSBFIRST, kSpiMode));
  select(true);
  delayMicroseconds(1);
  spi_.transfer(static_cast<uint8_t>(reg & 0x7f));
  for (uint32_t i = 0; i < length; ++i) {
    spi_.transfer(buffer[i]);
  }
  delayMicroseconds(1);
  select(false);
  spi_.endTransaction();
  return 0;
}

int SensorSpiTransport::readThunk(struct inv_ixm42xxx_serif* serif,
                                  uint8_t reg, uint8_t* buffer,
                                  uint32_t length) {
  auto* transport = static_cast<SensorSpiTransport*>(serif->context);
  return transport == nullptr ? -1 : transport->read(reg, buffer, length);
}

int SensorSpiTransport::writeThunk(struct inv_ixm42xxx_serif* serif,
                                   uint8_t reg, const uint8_t* buffer,
                                   uint32_t length) {
  auto* transport = static_cast<SensorSpiTransport*>(serif->context);
  return transport == nullptr ? -1 : transport->write(reg, buffer, length);
}
