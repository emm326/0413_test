#pragma once
#include <Arduino.h>
#include <SPI.h>
struct AccelSample {
  int16_t x, y, z;
};
class IimBase {
 public:
  IimBase(const char* name, uint8_t identity);
  bool begin();
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  const char* name() const {
    return name_;
  }
  uint8_t expectedIdentity() const {
    return identity_;
  }
  uint8_t readRegister(uint8_t address);

 protected:
  virtual void configureFifo() = 0;
  void writeRegister(uint8_t address, uint8_t value);

 private:
  void readBytes(uint8_t address, uint8_t* data, size_t count);
  SPIClass spi_;
  const char* name_;
  uint8_t identity_;
};
