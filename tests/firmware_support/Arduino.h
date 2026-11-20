#pragma once
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
constexpr int OUTPUT=1, INPUT=2, HIGH=1, LOW=0;
void pinMode(int pin, int mode);
void digitalWrite(int pin, int value);
int digitalRead(int pin);
void delay(unsigned long ms);
void delayMicroseconds(unsigned int us);
struct TestSerial {
  std::string output;
  template<class... Args> int printf(const char* format, Args... args) {
    char line[1024]; int n=std::snprintf(line,sizeof(line),format,args...);
    if (n > 0) { output.append(line, static_cast<size_t>(n) < sizeof(line) ? static_cast<size_t>(n) : sizeof(line)-1); }
    return n;
  }
  void println(const char* text) { output+=text;output+='\n'; }
};
extern TestSerial Serial;

constexpr int INPUT_PULLUP = 3;
unsigned long millis();