#pragma once

#include <Arduino.h>
#include <SPI.h>

#include "sensor/AccelSample.h"

// Formal IIM-42352 driver.
//
// This driver deliberately does not inherit the legacy IimBase. It mirrors the
// IIM-42652 formal driver interface and FIFO pipeline so NodeApp can use both
// sensors without sensor-specific branches.
class Iim42352 final {
 public:
  struct FifoSample {
    AccelSample accel;
    uint16_t timestamp;
    uint8_t header;
    uint16_t packetBytes;
    bool timed;
  };

  Iim42352();

  bool begin(bool resetDevice = true, bool externalClock = true);
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  bool read(AccelSample& sample, uint16_t& fifoTimestamp, bool& timed);
  bool read(FifoSample& sample);
  bool readDirect(AccelSample& sample);

  const char* name() const { return "IIM-42352"; }
  uint8_t expectedIdentity() const { return 0x6d; }
  uint8_t readRegister(uint8_t address);
  uint16_t lastFifoBytes() const { return lastFifoBytes_; }
  uint16_t fifoPacketBytes() const { return packetBytes_; }
  uint8_t lastHeader() const { return lastHeader_; }
  bool fifoCountValid() const;
  uint8_t lastFifoIdentity() const { return lastFifoIdentity_; }
  uint32_t fifoRecoveryTotal() const { return fifoRecoveryTotal_; }
  uint16_t lastFifoCountRaw() const { return lastFifoCountRaw_; }
  uint8_t lastFifoCountAttempts() const { return lastFifoCountAttempts_; }
  bool lastFifoCountOk() const { return lastFifoCountOk_; }
  bool lastFifoReadOk() const { return lastFifoReadOk_; }
  uint8_t fifoConsecutiveFailures() const { return fifoConsecutiveFailures_; }
  uint32_t fifoErrorTotal() const { return fifoErrorTotal_; }
  const char* lastFifoErrorReason() const { return lastFifoErrorReason_; }
  uint16_t lastFifoTimestamp() const { return lastFifoTimestamp_; }
  bool lastFifoTimestampValid() const { return lastFifoTimestampValid_; }
  uint8_t timestampConfig() const { return timestampConfig_; }
  bool timestampRtcMode() const { return timestampRtcMode_; }
  bool timestampSupported() const { return timestampSupported_; }
  uint32_t timestampTicksPerSecond() const;
  uint32_t timestampUs(uint16_t ticks) const;

  void diagnostic();
  void linkTest();
  void i2cScan();

 private:
  static constexpr uint16_t kSampleCapacity = 256;
  static constexpr uint16_t kFifoCapacityBytes = 2048;
  static constexpr uint16_t kFifoBatchCapacityBytes = 2080;
  static constexpr uint16_t kFifoHeaderBytes = 1;
  static constexpr uint16_t kFifoAxisBytes = 6;
  static constexpr uint16_t kFifoTempBytes = 1;
  static constexpr uint16_t kFifoTimeBytes = 2;

  bool selectTransport(uint8_t& observedIdentity);
  bool stableIdentity(uint8_t& observedIdentity, uint8_t attempts = 12);
  bool setBank(uint8_t bank);
  bool writeVerified(uint8_t address, uint8_t value, uint8_t mask,
                     const char* stage);
  bool readRegisters(uint8_t address, uint8_t* data, size_t count);
  uint8_t readRegisterRaw(uint8_t address);
  void writeRegister(uint8_t address, uint8_t value);

  bool configureInterface();
  bool configureClock(bool externalClock);
  bool configureTimestamp();
  bool configureFifo();
  bool forceSixteenBitFifo();
  bool configureInterrupt();
  bool applyAccelConfig(uint16_t rate, uint8_t range);
  bool resetDeviceAndWait();
  bool recoverFifoState();

  void updateFifoPacketSize();
  uint16_t maxFifoPackets() const;
  static uint16_t fifoPacketBytesForHeader(uint8_t header);
  bool readFifoPacket();
  void clearQueue();

  SPIClass spi_;
  uint32_t spiHz_ = 1000000;
  uint8_t spiMode_ = SPI_MODE0;

  FifoSample samples_[kSampleCapacity]{};
  uint8_t fifoPayload_[20]{};
  uint8_t fifoBatch_[kFifoBatchCapacityBytes]{};
  uint16_t fifoBatchBytes_ = 0;
  uint16_t fifoParseOffset_ = 0;
  uint16_t head_ = 0;
  uint16_t tail_ = 0;
  uint16_t count_ = 0;

  uint16_t rate_ = 50;
  uint8_t range_ = 4;
  uint16_t packetBytes_ = 16;
  uint16_t lastFifoBytes_ = 0;
  uint16_t lastFifoCountRaw_ = 0;
  uint8_t lastFifoCountAttempts_ = 0;
  bool lastFifoCountOk_ = true;
  bool lastFifoReadOk_ = true;
  uint8_t fifoConsecutiveFailures_ = 0;
  uint8_t lastFifoIdentity_ = 0;
  uint32_t fifoRecoveryTotal_ = 0;
  uint32_t fifoErrorTotal_ = 0;
  const char* lastFifoErrorReason_ = "none";
  uint8_t lastHeader_ = 0;
  uint16_t lastFifoTimestamp_ = 0;
  bool lastFifoTimestampValid_ = false;
  uint8_t timestampConfig_ = 0;
  bool timestampRtcMode_ = false;
  bool timestampSupported_ = false;
  bool initialized_ = false;
  bool running_ = false;
  bool overflow_ = false;
};
