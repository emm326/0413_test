#pragma once

#include <Arduino.h>
#include <Wire.h>

class Ds3231Clock {
 public:
  Ds3231Clock(TwoWire& wire,
              uint8_t address,
              int8_t sdaPin,
              int8_t sclPin,
              uint32_t wireClockHz = 400000UL);

  bool begin();
  bool isPresent() const;
  bool enable32kHz();
  bool is32kHzEnabled();
  bool oscillatorStopped();
  uint8_t lastStatus() const;

 private:
  static constexpr uint8_t REG_STATUS = 0x0F;
  static constexpr uint8_t STATUS_OSF = 0x80;
  static constexpr uint8_t STATUS_EN32KHZ = 0x08;

  bool readRegister(uint8_t reg, uint8_t& value);
  bool writeRegister(uint8_t reg, uint8_t value);

  TwoWire& wire_;
  uint8_t address_;
  int8_t sdaPin_;
  int8_t sclPin_;
  uint32_t wireClockHz_;
  bool present_;
  uint8_t lastStatus_;
};
