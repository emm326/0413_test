#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
constexpr int MSBFIRST = 1, SPI_MODE0 = 0;
struct SPISettings { SPISettings(uint32_t, int, int) {} };
struct SPIClass {
  uint8_t registers[5][128] = {};
  uint8_t bank = 0, address = 0;
  bool first = true, reading = false;
  SPIClass() { reset(); }
  void reset() {
    std::memset(registers, 0, sizeof(registers));
    registers[0][0x75] = 0x6D;
    registers[0][0x4D] = 0x90;
    registers[0][0x64] = 0x10;
    bank = 0;
  }
  void begin(int, int, int, int) {}
  void beginTransaction(const SPISettings&) { first = true; }
  void endTransaction() {}
  uint8_t transfer(uint8_t value) {
    if (first) { first = false; reading = (value & 0x80) != 0; address = value & 0x7F; return 0; }
    const uint8_t reg = address++;
    if (reading) return registers[bank][reg];
    if (reg == 0x76) { bank = value < 5 ? value : 0; return 0; }
    if (bank == 0 && reg == 0x11 && value == 1) { reset(); return 0; }
    registers[bank][reg] = value;
    return 0;
  }
};
inline SPIClass SPI;
