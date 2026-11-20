#pragma once

#include <cstddef>
#include <cstdint>

class TwoWire {
 public:
  void begin(int, int, uint32_t) {}
  void end() {}
  void beginTransmission(uint8_t) {}
  uint8_t endTransmission(bool = true) { return 4; }
  size_t write(uint8_t) { return 1; }
  uint8_t requestFrom(uint8_t, uint8_t) { return 0; }
  int read() { return 0; }
};

extern TwoWire Wire;