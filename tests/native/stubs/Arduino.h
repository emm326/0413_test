#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <algorithm>
using std::min;
#define IRAM_ATTR
constexpr int LOW = 0, HIGH = 1, INPUT = 0, OUTPUT = 1, RISING = 1, SERIAL_8N1 = 0;
constexpr int SS = 8, SCK = 9, MOSI = 10, MISO = 11;
constexpr int RST_LoRa = 12, BUSY_LoRa = 13, DIO0 = 14;
constexpr int SDA_OLED = 17, SCL_OLED = 18, RST_OLED = 21, Vext = 36;
inline uint32_t testMillis = 1000;
inline int testDio = LOW;
inline uint32_t millis() { return testMillis; }
inline int digitalRead(int) { return testDio; }
inline void pinMode(int, int) {}
inline void digitalWrite(int, int) {}
inline void delay(uint32_t ms) { testMillis += ms; }
inline int digitalPinToInterrupt(int pin) { return pin; }
inline void attachInterrupt(int, void (*)(), int) {}
inline void noInterrupts() {}
inline void interrupts() {}
struct Stream { template<class... T> void printf(const char*, T...) {} };
struct HardwareSerial {
  std::string input;
  size_t position = 0;
  void begin(uint32_t, int, int, int) {}
  int available() { return static_cast<int>(input.size() - position); }
  int read() { return available() ? input[position++] : -1; }
};
