#pragma once
#include <Arduino.h>
#include <SPI.h>
struct AccelSample {
  int16_t x, y, z;
};
class IimBase {
 public:
  IimBase(const char* name, uint8_t identity);
  bool begin(bool resetDevice = true, bool externalClock = true);
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  bool readDirect(AccelSample& sample);
  const char* name() const {
    return name_;
  }
  uint8_t expectedIdentity() const {
    return identity_;
  }
  uint8_t readRegister(uint8_t address);
  uint16_t lastFifoBytes() const { return lastFifoBytes_; }
  uint8_t lastHeader() const { return lastHeader_; }
  bool fifoCountValid() const { return lastFifoBytes_ <= 2048; }
  void diagnostic();
  void linkTest();

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
  uint8_t lastHeader_ = 0;
  const char* name_;
  uint8_t identity_;
};
