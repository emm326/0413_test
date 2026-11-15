#pragma once

#include <Arduino.h>
#include <SPI.h>

#include "sensors/iim42652_vendor/Ixm42xxxTransport.h"

// Formal IIM42652 SPI transport. The bus settings are fixed to the
// configuration proven by the six-wire bring-up: HSPI, mode 0, 100 kHz,
// and GPIO-controlled chip select.
class SensorSpiTransport {
 public:
  SensorSpiTransport(int sck, int miso, int mosi, int cs);

  bool begin();
  void end();

  int read(uint8_t reg, uint8_t* buffer, uint32_t length);
  int write(uint8_t reg, const uint8_t* buffer, uint32_t length);

  static int readThunk(struct inv_ixm42xxx_serif* serif, uint8_t reg,
                       uint8_t* buffer, uint32_t length);
  static int writeThunk(struct inv_ixm42xxx_serif* serif, uint8_t reg,
                        const uint8_t* buffer, uint32_t length);

 private:
  void select(bool active);

  SPIClass spi_;
  int sck_;
  int miso_;
  int mosi_;
  int cs_;
  bool started_ = false;
};
