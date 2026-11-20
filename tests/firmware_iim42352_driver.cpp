#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "Arduino.h"
#include "SPI.h"
#include "Wire.h"
#include "sensor/Registers.h"
#include "sensors/iim42352/Driver.h"

namespace {
uint8_t readFifoByte();

struct Bus {
  uint8_t bank = 0;
  uint8_t regs[8][128]{};
  bool selected = false;
  bool started = false;
  bool ignoreTimestampWrites = false;
  bool signalResetDone = true;

  void defaults() {
    std::memset(regs, 0, sizeof(regs));
    bank = 0;
    selected = false;
    started = false;
    ignoreTimestampWrites = false;
    signalResetDone = true;
    regs[0][IimRegister::WHO_AM_I] = 0x6d;
    regs[0][IimRegister::INT_CONFIG1] = 0x10;
  }

  uint8_t read(uint8_t reg) {
    if (reg == IimRegister::REG_BANK_SEL) {
      return bank;
    }
    if (bank == 0 && reg == IimRegister::FIFO_DATA) {
      return readFifoByte();
    }
    return regs[bank][reg & 0x7f];
  }

  void write(uint8_t reg, uint8_t value) {
    if (reg == IimRegister::REG_BANK_SEL) {
      bank = value & 0x07;
      return;
    }
    if (reg == IimRegister::INTF_CONFIG1 && (value & 0x03) != 0) {
      throw std::runtime_error("reserved CLKSEL written on IIM-42352");
    }
    if (reg == 0x54 && ignoreTimestampWrites) {
      return;
    }
    regs[bank][reg & 0x7f] = value;
    if (reg == IimRegister::DEVICE_CONFIG && (value & 0x01) != 0 && signalResetDone) {
      regs[0][IimRegister::INT_STATUS] |= 0x10;
    }
  }
};

Bus bus;
std::vector<uint8_t> fifo;
std::size_t fifoPos = 0;

void buildFifoTimed(const AccelSample* samples, const uint16_t* timestamps,
                    std::size_t count) {
  fifo.clear();
  fifoPos = 0;
  for (std::size_t i = 0; i < count; ++i) {
    fifo.push_back(0x68);  // Packet 2 + ODR timestamp (DS-000436 section 6).
    const int16_t axes[3] = {samples[i].x, samples[i].y, samples[i].z};
    for (int16_t axis : axes) {
      fifo.push_back(static_cast<uint8_t>((axis >> 8) & 0xff));
      fifo.push_back(static_cast<uint8_t>(axis & 0xff));
    }
    // Six reserved bytes must not be decoded as axes or as a timestamp.
    for (uint8_t reserved = 0; reserved < 6; ++reserved) {
      fifo.push_back(static_cast<uint8_t>(0xa0 + reserved));
    }
    fifo.push_back(0);  // Temperature at byte 13.
    fifo.push_back(static_cast<uint8_t>((timestamps[i] >> 8) & 0xff));
    fifo.push_back(static_cast<uint8_t>(timestamps[i] & 0xff));
  }
}

void buildFifoUntimed(const AccelSample* samples, std::size_t count) {
  fifo.clear();
  fifoPos = 0;
  for (std::size_t i = 0; i < count; ++i) {
    fifo.push_back(0x40);  // ACCEL only.
    const int16_t axes[3] = {samples[i].x, samples[i].y, samples[i].z};
    for (int16_t axis : axes) {
      fifo.push_back(static_cast<uint8_t>((axis >> 8) & 0xff));
      fifo.push_back(static_cast<uint8_t>(axis & 0xff));
    }
    fifo.push_back(0);
  }
}

uint8_t readFifoByte() {
  return fifoPos < fifo.size() ? fifo[fifoPos++] : 0;
}

void setFifoPackets(uint16_t packets) {
  bus.regs[0][IimRegister::FIFO_COUNTH] =
      static_cast<uint8_t>(packets & 0xff);
  bus.regs[0][IimRegister::FIFO_COUNTL] =
      static_cast<uint8_t>((packets >> 8) & 0xff);
}

void require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}
}  // namespace

TestSerial Serial;
TwoWire Wire;
unsigned long testMillis = 0;
unsigned long millis() { return testMillis; }
void pinMode(int, int) {}
void digitalWrite(int, int value) { bus.selected = value == LOW; }
int digitalRead(int) { return 0; }
void delay(unsigned long ms) { testMillis += ms; }
void delayMicroseconds(unsigned int) {}

SPIClass::SPIClass(uint8_t) {}
void SPIClass::begin(int, int, int, int) {
  started_ = true;
  bus.started = true;
}
void SPIClass::end() {
  started_ = false;
  bus.started = false;
}
void SPIClass::beginTransaction(SPISettings settings) {
  (void)settings;
  command_ = true;
}
void SPIClass::endTransaction() {}
uint8_t SPIClass::transfer(uint8_t value) {
  if (!started_ || !bus.selected) {
    throw std::runtime_error("SPI transfer without selected CS");
  }
  if (command_) {
    address_ = value & 0x7f;
    reading_ = (value & 0x80) != 0;
    command_ = false;
    return 0;
  }
  if (reading_) {
    if (address_ == IimRegister::FIFO_DATA) {
      return readFifoByte();
    }
    return bus.read(address_++);
  }
  bus.write(address_++, value);
  return 0;
}
void SPIClass::transferBytes(const uint8_t* data, uint8_t* out,
                             uint32_t length) {
  for (uint32_t i = 0; i < length; ++i) {
    const uint8_t received = transfer(data == nullptr ? 0 : data[i]);
    if (out != nullptr) {
      out[i] = received;
    }
  }
}

void timedFifoAndInterface() {
  bus.defaults();
  Iim42352 sensor;
  require(sensor.begin(false, true), "IIM-42352 initialization failed");
  require(sensor.readRegister(IimRegister::WHO_AM_I) == 0x6d,
          "identity register mismatch");
  require(sensor.timestampSupported(), "timestamp configuration was not accepted");
  require(sensor.fifoPacketBytes() == 16, "timed packet size is not 16 bytes");

  require((bus.regs[0][IimRegister::FIFO_CONFIG1] & 0x1b) == 0x03,
          "Packet 2 must be selected without high resolution or FSYNC");
  require((bus.regs[0][IimRegister::INTF_CONFIG1] & 0x07) == 0x04,
          "external clock must use RTC_MODE=1 and CLKSEL=00");
  sensor.start();
  const AccelSample expected[] = {{100, -200, 300}, {400, -500, 600}};
  const uint16_t timestamps[] = {320, 640};
  buildFifoTimed(expected, timestamps, 2);
  setFifoPackets(2);
  require(sensor.available() == 2, "two timed FIFO samples were not buffered");

  for (std::size_t i = 0; i < 2; ++i) {
    Iim42352::FifoSample item{};
    require(sensor.read(item), "timed FIFO sample could not be read");
    require(item.accel.x == expected[i].x && item.accel.y == expected[i].y &&
                item.accel.z == expected[i].z,
            "timed FIFO axes were decoded incorrectly");
    require(item.timed && item.timestamp == timestamps[i],
            "FIFO timestamp was not decoded");
    require(item.packetBytes == 16, "parsed packet size is not 16 bytes");
  }
  require(sensor.timestampTicksPerSecond() == 57220,
          "external timestamp tick rate does not apply datasheet scaling");
  require(sensor.timestampUs(1875) == 32768,
          "external timestamps must scale 30000 nominal us to 32768 us");

  sensor.standby();
  AccelSample stale{};
  require(!sensor.read(stale), "standby left a stale sample");
}

void untimedFallback() {
  bus.defaults();
  bus.ignoreTimestampWrites = true;
  Iim42352 sensor;
  require(sensor.begin(false, false), "fallback initialization failed");
  require(!sensor.timestampSupported(), "unsupported timestamp was reported valid");
  require(sensor.fifoPacketBytes() == 8, "fallback packet size is not 8 bytes");

  require((bus.regs[0][IimRegister::FIFO_CONFIG1] & 0x03) == 0x01,
          "fallback must select Packet 1");
  require((bus.regs[0][IimRegister::INTF_CONFIG1] & 0x07) == 0,
          "internal clock must use RTC_MODE=0 and CLKSEL=00");
  sensor.start();
  const AccelSample expected[] = {{7, 8, 9}};
  buildFifoUntimed(expected, 1);
  setFifoPackets(1);
  require(sensor.available() == 1, "untimed FIFO sample was not buffered");
  Iim42352::FifoSample item{};
  require(sensor.read(item), "untimed FIFO sample could not be read");
  require(!item.timed, "untimed FIFO record reported a timestamp");
  require(item.accel.x == 7 && item.accel.y == 8 && item.accel.z == 9,
          "untimed FIFO axes were decoded incorrectly");
}

void invalidSettingsAndOverflow() {
  bus.defaults();
  Iim42352 sensor;
  require(sensor.begin(false, false), "initialization failed");
  require(sensor.timestampTicksPerSecond() == 58594,
          "internal timestamp tick rate does not apply datasheet scaling");
  require(sensor.timestampUs(1875) == 32000,
          "internal timestamps must scale 30000 nominal us to 32000 us");
  require(!sensor.configure(60, 4), "invalid rate was accepted");
  require(!sensor.configure(50, 3), "invalid range was accepted");
  sensor.start();
  setFifoPackets(250);
  require(sensor.available() == 0, "oversized FIFO count was accepted");
  require(!sensor.lastFifoCountOk(), "oversized FIFO count was not diagnosed");
  require(sensor.fifoConsecutiveFailures() == 1,
          "FIFO failure counter did not increment");
}

void invalidPacketFormat() {
  bus.defaults();
  Iim42352 sensor;
  require(sensor.begin(false, false), "initialization failed");
  sensor.start();
  const AccelSample expected[] = {{1, 2, 3}};
  const uint16_t timestamps[] = {100};
  buildFifoTimed(expected, timestamps, 1);
  fifo[0] = 0x48;  // Nonexistent 10-byte layout accepted by the old driver.
  setFifoPackets(1);
  require(sensor.available() == 0, "invalid packet format was accepted");
  require(sensor.fifoErrorTotal() == 1, "packet format error was not recorded");
}

void resetCompletionRequired() {
  bus.defaults();
  Iim42352 sensor;
  require(sensor.begin(true, false), "completed reset should succeed");
  bus.defaults();
  bus.signalResetDone = false;
  require(!sensor.begin(true, false), "identity alone must not confirm reset");
  bus.defaults();
  bus.regs[0][IimRegister::WHO_AM_I] = 0;
  require(!sensor.begin(false, false), "zero identity must not initialize");
}

int main() {
  struct Test {
    const char* name;
    void (*run)();
  };
  const Test tests[] = {
      {"timed_fifo_and_interface", timedFifoAndInterface},
      {"untimed_fallback", untimedFallback},
      {"invalid_settings_and_overflow", invalidSettingsAndOverflow},
      {"invalid_packet_format", invalidPacketFormat},
      {"reset_completion_required", resetCompletionRequired},
  };

  unsigned failures = 0;
  for (const Test& test : tests) {
    try {
      test.run();
      std::cout << "PASS " << test.name << '\n';
    } catch (const std::exception& error) {
      ++failures;
      std::cout << "FAIL " << test.name << ": " << error.what() << '\n';
    }
  }
  std::cout << "RESULT " << (sizeof(tests) / sizeof(tests[0]) - failures)
            << '/' << (sizeof(tests) / sizeof(tests[0])) << " passed\n";
  return failures == 0 ? 0 : 1;
}