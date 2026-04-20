#pragma once

#include <Arduino.h>
#include <Wire.h>

class ADXL345Driver {
 public:
  struct RawSample {
    int16_t x;
    int16_t y;
    int16_t z;
  };

  enum class Range : uint8_t {
    Range2G = 0,
    Range4G = 1,
    Range8G = 2,
    Range16G = 3,
  };

  enum class DataRate : uint8_t {
    Hz12_5 = 0x07,
    Hz25 = 0x08,
    Hz50 = 0x09,
    Hz100 = 0x0A,
    Hz200 = 0x0B,
  };

  explicit ADXL345Driver(TwoWire& wire,
                         uint8_t address,
                         int8_t sdaPin,
                         int8_t sclPin,
                         uint32_t wireClockHz = 400000UL);

  bool begin();
  bool checkDeviceId();
  uint8_t lastDeviceId() const;
  uint8_t expectedDeviceId() const;
  uint8_t regDataFormat() const;
  uint8_t regPowerCtl() const;
  uint8_t regFifoStatus() const;
  void setRange(Range range);
  void setFullResolution(bool en);
  void setDataRate(DataRate rate);
  bool readXYZRaw(int16_t& x, int16_t& y, int16_t& z);
  uint8_t fifoAvailable();
  size_t readFifoBurst(RawSample* outSamples, size_t maxSamples);

  uint8_t readRegister(uint8_t reg);
  void writeRegister(uint8_t reg, uint8_t value);

 private:
  static constexpr uint8_t REG_DEVID = 0x00;
  static constexpr uint8_t REG_BW_RATE = 0x2C;
  static constexpr uint8_t REG_POWER_CTL = 0x2D;
  static constexpr uint8_t REG_INT_ENABLE = 0x2E;
  static constexpr uint8_t REG_DATA_FORMAT = 0x31;
  static constexpr uint8_t REG_DATAX0 = 0x32;
  static constexpr uint8_t REG_FIFO_CTL = 0x38;
  static constexpr uint8_t REG_FIFO_STATUS = 0x39;

  static constexpr uint8_t DEVID_EXPECTED = 0xE5;
  static constexpr uint8_t DATA_FORMAT_FULL_RES = 0x08;
  static constexpr uint8_t POWER_CTL_MEASURE = 0x08;
  static constexpr uint8_t FIFO_MODE_STREAM = 0x80;

  bool readMulti(uint8_t startReg, uint8_t* buffer, size_t length);

  TwoWire& wire_;
  uint8_t address_;
  int8_t sdaPin_;
  int8_t sclPin_;
  uint32_t wireClockHz_;
  uint8_t lastDeviceId_;
};
