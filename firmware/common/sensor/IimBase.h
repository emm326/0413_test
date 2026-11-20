#pragma once
#include <Arduino.h>
#include <SPI.h>

#include "sensor/AccelSample.h"

class IimBase {
 public:
  struct FifoSample {
    AccelSample accel;
    uint16_t timestamp;
    uint8_t header;
    uint16_t packetBytes;
    bool timed;
  };

  IimBase(const char* name, uint8_t identity);
  bool begin(bool resetDevice = true, bool externalClock = true);
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  bool read(FifoSample& sample);
  bool readDirect(AccelSample& sample);
  const char* name() const {
    return name_;
  }
  uint8_t expectedIdentity() const {
    return identity_;
  }
  uint8_t readRegister(uint8_t address);
  uint16_t lastFifoBytes() const { return lastFifoBytes_; }
  uint16_t fifoPacketBytes() const { return 8; }
  uint16_t lastFifoCountRaw() const { return lastFifoCountRaw_; }
  uint8_t lastFifoCountAttempts() const { return lastFifoCountAttempts_; }
  bool lastFifoCountOk() const { return lastFifoCountOk_; }
  bool lastFifoReadOk() const { return lastFifoReadOk_; }
  uint8_t fifoConsecutiveFailures() const { return fifoConsecutiveFailures_; }
  uint32_t fifoErrorTotal() const { return fifoErrorTotal_; }
  const char* lastFifoErrorReason() const { return lastFifoErrorReason_; }
  uint16_t lastFifoTimestamp() const { return 0; }
  bool lastFifoTimestampValid() const { return false; }
  uint32_t timestampTicksPerSecond() const { return 1000000UL; }
  uint32_t timestampUs(uint16_t ticks) const { return ticks; }
  uint8_t lastHeader() const { return lastHeader_; }
  bool fifoCountValid() const { return lastFifoBytes_ <= 2048; }
  void diagnostic();
  void linkTest();
  void i2cScan();

 protected:
  virtual uint8_t standbyPower() const { return 0; }
  virtual bool configureInterface() { return true; }
  virtual void configureFifo() = 0;
  void writeRegister(uint8_t address, uint8_t value);
  bool writeVerified(uint8_t address, uint8_t value, uint8_t mask, const char* stage);
  bool selectBank(uint8_t bank);

 private:
  bool selectTransport(uint8_t& observedIdentity);
  bool stableIdentity(uint8_t& observedIdentity, uint8_t attempts = 12);
  void readBytes(uint8_t address, uint8_t* data, size_t count);
  SPIClass spi_;
  uint32_t spiHz_ = 1000000;
  uint8_t spiMode_ = SPI_MODE0;
  uint16_t lastFifoBytes_ = 0;
  uint16_t lastFifoCountRaw_ = 0;
  uint8_t lastFifoCountAttempts_ = 0;
  bool lastFifoCountOk_ = true;
  bool lastFifoReadOk_ = true;
  uint8_t fifoConsecutiveFailures_ = 0;
  uint32_t fifoErrorTotal_ = 0;
  const char* lastFifoErrorReason_ = "none";
  uint8_t lastHeader_ = 0;
  const char* name_;
  uint8_t identity_;
};
