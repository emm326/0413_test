#include "SensorSpiTransport.h"

SensorSpiTransport::SensorSpiTransport(int sck, int miso, int mosi, int cs)
    : primaryBus_(HSPI),
      alternateBus_(FSPI),
      sck_(sck),
      miso_(miso),
      mosi_(mosi),
      cs_(cs) {}

bool SensorSpiTransport::begin(const Config& config) {
  end();
  config_ = config;

  // Keep CS inactive before changing the pin direction.
  digitalWrite(cs_, HIGH);
  pinMode(cs_, OUTPUT);
  digitalWrite(cs_, HIGH);

  if (!config_.software) {
    if (config_.mode != SPI_MODE0 && config_.mode != SPI_MODE3) {
      return false;
    }
    hardwareBus_ = config_.alternateBus ? &alternateBus_ : &primaryBus_;
    hardwareBus_->begin(sck_, miso_, mosi_, -1);
    delayMicroseconds(20);
  } else {
    if (config_.mode != SPI_MODE0) {
      return false;
    }
    digitalWrite(sck_, LOW);
    pinMode(sck_, OUTPUT);
    digitalWrite(sck_, LOW);
    pinMode(mosi_, OUTPUT);
    digitalWrite(mosi_, LOW);
    pinMode(miso_, INPUT);
    const uint32_t hz = config_.hz == 0 ? 100000U : config_.hz;
    halfPeriodUs_ = (1000000U + hz - 1U) / (hz * 2U);
    if (halfPeriodUs_ == 0) {
      halfPeriodUs_ = 1;
    }
  }

  started_ = true;
  return true;
}

void SensorSpiTransport::end() {
  if (started_ && !config_.software && hardwareBus_ != nullptr) {
    hardwareBus_->end();
  }
  hardwareBus_ = nullptr;
  digitalWrite(cs_, HIGH);
  pinMode(cs_, OUTPUT);
  digitalWrite(cs_, HIGH);
  started_ = false;
}

void SensorSpiTransport::selectCs(bool active) {
  digitalWrite(cs_, active ? LOW : HIGH);
}

void SensorSpiTransport::softwareTransfer(uint8_t value, uint8_t* received) {
  uint8_t result = 0;
  for (int bit = 7; bit >= 0; --bit) {
    digitalWrite(mosi_, (value >> bit) & 0x01);
    delayMicroseconds(halfPeriodUs_);
    if (digitalRead(miso_) == HIGH) {
      result |= static_cast<uint8_t>(1U << bit);
    }
    digitalWrite(sck_, HIGH);
    delayMicroseconds(halfPeriodUs_);
    digitalWrite(sck_, LOW);
  }
  if (received != nullptr) {
    *received = result;
  }
}

int SensorSpiTransport::read(uint8_t reg, uint8_t* buffer, uint32_t length) {
  if (!started_ || buffer == nullptr || length == 0) {
    return -1;
  }

  if (!config_.software) {
    hardwareBus_->beginTransaction(
        SPISettings(config_.hz, MSBFIRST, config_.mode));
    selectCs(true);
    delayMicroseconds(1);
    hardwareBus_->transfer(static_cast<uint8_t>(reg | 0x80));
    hardwareBus_->transferBytes(nullptr, buffer, length);
    selectCs(false);
    hardwareBus_->endTransaction();
  } else {
    selectCs(true);
    delayMicroseconds(halfPeriodUs_);
    softwareTransfer(static_cast<uint8_t>(reg | 0x80), nullptr);
    for (uint32_t i = 0; i < length; ++i) {
      softwareTransfer(0xFF, &buffer[i]);
    }
    selectCs(false);
  }
  return 0;
}

int SensorSpiTransport::write(uint8_t reg, const uint8_t* buffer,
                              uint32_t length) {
  if (!started_ || buffer == nullptr || length == 0) {
    return -1;
  }

  if (!config_.software) {
    hardwareBus_->beginTransaction(
        SPISettings(config_.hz, MSBFIRST, config_.mode));
    selectCs(true);
    delayMicroseconds(1);
    hardwareBus_->transfer(static_cast<uint8_t>(reg & 0x7F));
    hardwareBus_->transferBytes(buffer, nullptr, length);
    selectCs(false);
    hardwareBus_->endTransaction();
  } else {
    selectCs(true);
    delayMicroseconds(halfPeriodUs_);
    softwareTransfer(static_cast<uint8_t>(reg & 0x7F), nullptr);
    for (uint32_t i = 0; i < length; ++i) {
      softwareTransfer(buffer[i], nullptr);
    }
    selectCs(false);
  }
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