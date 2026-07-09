#pragma once

#include <Arduino.h>
#include <SPI.h>

#ifndef APP_IIM42352_WHO_AM_I_EXPECTED_VALUE
#define APP_IIM42352_WHO_AM_I_EXPECTED_VALUE 0x6D
#endif

class IIM42352Driver {
 public:
  struct RawSample {
    int16_t x;
    int16_t y;
    int16_t z;
  };

  enum class Range : uint8_t {
    Range2G = 3,
    Range4G = 2,
    Range8G = 1,
    Range16G = 0,
  };

  enum class DataRate : uint8_t {
    Hz50 = 0x09,
    Hz100 = 0x08,
    Hz200 = 0x07,
    Hz400 = 0x06,
  };

  IIM42352Driver(SPIClass& spi,
                 int8_t sckPin,
                 int8_t misoPin,
                 int8_t mosiPin,
                 int8_t csPin,
                 uint32_t spiClockHz = 8000000UL);

  bool begin();
  bool checkDeviceId();
  uint8_t lastDeviceId() const;
  uint8_t expectedDeviceId() const;
  uint8_t regDataFormat() const;
  uint8_t regPowerCtl() const;
  uint8_t regFifoStatus() const;
  uint8_t regIntConfig() const;
  uint8_t regIntStatus() const;
  uint8_t regIntConfig1() const;
  uint8_t regIntSource0() const;
  uint8_t regIntfConfig1() const;
  uint8_t regIntfConfig5() const;
  void setRange(Range range);
  void setFullResolution(bool en);
  void setDataRate(DataRate rate);
  bool configureExternalClockAndDataReady();
  bool restartAccel();
  bool externalClockConfigured();
  bool dataReadyInt1Configured();
  uint8_t readBankedRegister(uint8_t bank, uint8_t reg);
  void writeBankedRegister(uint8_t bank, uint8_t reg, uint8_t value);
  bool readXYZRaw(int16_t& x, int16_t& y, int16_t& z);
  uint8_t fifoAvailable();
  size_t readFifoBurst(RawSample* outSamples, size_t maxSamples);

  uint8_t readRegister(uint8_t reg);
  void writeRegister(uint8_t reg, uint8_t value);

 private:
  static constexpr uint8_t REG_DEVICE_CONFIG = 0x11;
  static constexpr uint8_t REG_INT_CONFIG = 0x14;
  static constexpr uint8_t REG_INTF_CONFIG1 = 0x4D;
  static constexpr uint8_t REG_FIFO_CONFIG = 0x16;
  static constexpr uint8_t REG_INT_STATUS = 0x2D;
  static constexpr uint8_t REG_FIFO_COUNTH = 0x2E;
  static constexpr uint8_t REG_FIFO_COUNTL = 0x2F;
  static constexpr uint8_t REG_FIFO_DATA = 0x30;
  static constexpr uint8_t REG_ACCEL_DATA_X1 = 0x1F;
  static constexpr uint8_t REG_PWR_MGMT0 = 0x4E;
  static constexpr uint8_t REG_ACCEL_CONFIG0 = 0x50;
  static constexpr uint8_t REG_FIFO_CONFIG1 = 0x5F;
  static constexpr uint8_t REG_FIFO_CONFIG2 = 0x60;
  static constexpr uint8_t REG_FIFO_CONFIG3 = 0x61;
  static constexpr uint8_t REG_INT_CONFIG1 = 0x64;
  static constexpr uint8_t REG_INT_SOURCE0 = 0x65;
  static constexpr uint8_t REG_WHO_AM_I = 0x75;
  static constexpr uint8_t REG_BANK_SEL = 0x76;
  static constexpr uint8_t REG_BANK1_INTF_CONFIG5 = 0x7B;

  static constexpr uint8_t WHO_AM_I_EXPECTED = APP_IIM42352_WHO_AM_I_EXPECTED_VALUE;
  static constexpr uint8_t FIFO_PACKET_SIZE = 8;
  static constexpr uint8_t FIFO_HEADER_ACCEL = 0x40;
  static constexpr uint8_t FIFO_HEADER_EMPTY = 0x80;
  static constexpr uint8_t BIT_INT_ASYNC_RESET = 0x10;
  static constexpr uint8_t BIT_UI_DRDY_INT1_EN = 0x08;
  static constexpr uint8_t BIT_RTC_MODE = 0x04;
  static constexpr uint8_t MASK_PIN9_FUNCTION = 0x06;
  static constexpr uint8_t PIN9_FUNCTION_CLKIN = 0x04;

  bool readMulti(uint8_t startReg, uint8_t* buffer, size_t length);
  bool readFifoPacket(RawSample& outSample);
  uint16_t fifoByteCount();
  void select();
  void deselect();

  SPIClass& spi_;
  SPISettings settings_;
  int8_t sckPin_;
  int8_t misoPin_;
  int8_t mosiPin_;
  int8_t csPin_;
  uint8_t lastDeviceId_;
  Range range_;
  DataRate dataRate_;
};
