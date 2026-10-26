#pragma once
#include <Arduino.h>
#include <cstring>
enum {
  RADIOLIB_ERR_NONE = 0, RADIOLIB_ERR_RX_TIMEOUT = -1,
  RADIOLIB_ERR_CRC_MISMATCH = -2, RADIOLIB_ERR_LORA_HEADER_DAMAGED = -3,
  RADIOLIB_ERR_PACKET_TOO_SHORT = -4, RADIOLIB_ERR_PACKET_TOO_LONG = -5,
  RADIOLIB_ERR_SPI_WRITE_FAILED = -6, RADIOLIB_ERR_SPI_CMD_TIMEOUT = -7,
  RADIOLIB_ERR_SPI_CMD_INVALID = -8, RADIOLIB_ERR_SPI_CMD_FAILED = -9,
  RADIOLIB_ERR_CHIP_NOT_FOUND = -10, RADIOLIB_ERR_INVALID_FREQUENCY = -11,
  RADIOLIB_ERR_INVALID_MODULATION_PARAMETERS = -12,
  RADIOLIB_ERR_INVALID_SYNC_WORD = -13, RADIOLIB_ERR_NULL_POINTER = -14
};
constexpr uint32_t RADIOLIB_SX126X_IRQ_RX_DONE = 0x0002;
constexpr uint32_t RADIOLIB_SX126X_IRQ_HEADER_ERR = 0x0020;
constexpr uint32_t RADIOLIB_SX126X_IRQ_CRC_ERR = 0x0040;
constexpr uint32_t RADIOLIB_SX126X_IRQ_TIMEOUT = 0x0200;
struct FakeRadio {
  uint32_t irq = 0;
  int16_t readState = 0, armState = 0;
  size_t length = 4;
  int arms = 0, reads = 0;
};
inline FakeRadio fakeRadio;
struct Module { Module(int, int, int, int) {} };
struct SX1262 {
  SX1262(Module*) {}
  template<class... T> int16_t begin(T...) { return 0; }
  int16_t setCRC(bool) { return 0; }
  int16_t startReceive() {
    ++fakeRadio.arms;
    if (!fakeRadio.armState) fakeRadio.irq = 0;
    return fakeRadio.armState;
  }
  int16_t transmit(const uint8_t*, size_t) { return 0; }
  uint32_t getIrqFlags() { return fakeRadio.irq; }
  size_t getPacketLength() { return fakeRadio.length; }
  int16_t readData(uint8_t* buffer, size_t length) {
    ++fakeRadio.reads;
    std::memset(buffer, 0x42, length);
    fakeRadio.irq = 0;
    return fakeRadio.readState;
  }
  float getRSSI() { return -70; }
  float getSNR() { return 8; }
};
