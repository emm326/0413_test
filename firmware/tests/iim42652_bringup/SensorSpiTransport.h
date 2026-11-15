#pragma once

#include <Arduino.h>
#include <SPI.h>

#include "Ixm42xxxTransport.h"

// ESP32 SPI transport for the unmodified InvenSense Ixm42xxx driver.
// The driver requires one CS-low transaction for the address and all data bytes.
class SensorSpiTransport {
 public:
  struct Config {
    const char* name;
    uint8_t mode;
    uint32_t hz;
    bool software;
    bool alternateBus;
  };

  SensorSpiTransport(int sck, int miso, int mosi, int cs);

  bool begin(const Config& config);
  void end();

  int read(uint8_t reg, uint8_t* buffer, uint32_t length);
  int write(uint8_t reg, const uint8_t* buffer, uint32_t length);

  const Config& config() const { return config_; }

  static int readThunk(struct inv_ixm42xxx_serif* serif, uint8_t reg,
                       uint8_t* buffer, uint32_t length);
  static int writeThunk(struct inv_ixm42xxx_serif* serif, uint8_t reg,
                        const uint8_t* buffer, uint32_t length);

 private:
  void softwareTransfer(uint8_t value, uint8_t* received);
  void selectCs(bool active);

  SPIClass primaryBus_;
  SPIClass alternateBus_;
  SPIClass* hardwareBus_ = nullptr;

  int sck_;
  int miso_;
  int mosi_;
  int cs_;
  uint32_t halfPeriodUs_ = 5;
  Config config_{"uninitialized", SPI_MODE0, 100000, false, false};
  bool started_ = false;
};