#include "Ds3231Clock.h"

Ds3231Clock::Ds3231Clock(TwoWire& wire,
                         uint8_t address,
                         int8_t sdaPin,
                         int8_t sclPin,
                         uint32_t wireClockHz)
    : wire_(wire),
      address_(address),
      sdaPin_(sdaPin),
      sclPin_(sclPin),
      wireClockHz_(wireClockHz),
      present_(false),
      lastStatus_(0) {}

bool Ds3231Clock::begin() {
  wire_.begin(sdaPin_, sclPin_, wireClockHz_);
  present_ = readRegister(REG_STATUS, lastStatus_);
  if (!present_) {
    return false;
  }
  return enable32kHz();
}

bool Ds3231Clock::isPresent() const { return present_; }

bool Ds3231Clock::enable32kHz() {
  uint8_t status = 0;
  if (!readRegister(REG_STATUS, status)) {
    present_ = false;
    return false;
  }

  status |= STATUS_EN32KHZ;
  status &= ~STATUS_OSF;
  if (!writeRegister(REG_STATUS, status)) {
    present_ = false;
    return false;
  }

  lastStatus_ = status;
  present_ = true;
  return true;
}

bool Ds3231Clock::is32kHzEnabled() {
  uint8_t status = 0;
  if (!readRegister(REG_STATUS, status)) {
    return false;
  }
  lastStatus_ = status;
  return (status & STATUS_EN32KHZ) != 0;
}

bool Ds3231Clock::oscillatorStopped() {
  uint8_t status = 0;
  if (!readRegister(REG_STATUS, status)) {
    return true;
  }
  lastStatus_ = status;
  return (status & STATUS_OSF) != 0;
}

uint8_t Ds3231Clock::lastStatus() const { return lastStatus_; }

bool Ds3231Clock::readRegister(uint8_t reg, uint8_t& value) {
  wire_.beginTransmission(address_);
  wire_.write(reg);
  if (wire_.endTransmission(false) != 0) {
    return false;
  }

  const uint8_t requested = wire_.requestFrom(static_cast<int>(address_), 1, 1);
  if (requested != 1 || !wire_.available()) {
    return false;
  }

  value = wire_.read();
  return true;
}

bool Ds3231Clock::writeRegister(uint8_t reg, uint8_t value) {
  wire_.beginTransmission(address_);
  wire_.write(reg);
  wire_.write(value);
  return wire_.endTransmission(true) == 0;
}
