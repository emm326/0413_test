#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "Arduino.h"
#include "SPI.h"
#include "driver/gpio.h"
#include "sensor/Registers.h"
#include "sensor/StartupCheck.h"
#include "sensors/iim42652/Driver.h"

uint8_t readFifoByteForTest();

struct Write {
  uint8_t reg;
  uint8_t value;
  uint32_t hz;
  uint8_t mode;
};

struct Bus {
  uint8_t bank = 0;
  uint8_t regs[8][128]{};
  uint32_t hz = 0;
  uint8_t mode = 0;
  bool selected = false;
  bool started = false;
  std::vector<Write> writes;

  void defaults() {
    std::memset(regs, 0, sizeof(regs));
    bank = 0;
    // A real chip answers this address without any prior configuration.
    regs[0][0x75] = 0x6f;
  }

  uint8_t read(uint8_t reg) {
    if (bank == 0 && reg == MPUREG_FIFO_DATA) {
      return readFifoByteForTest();
    }
    return regs[bank][reg & 0x7f];
  }

  void write(uint8_t reg, uint8_t value) {
    if (reg == 0x76) {
      bank = value & 0x07;
      return;
    }
    regs[bank][reg & 0x7f] = value;
    writes.push_back({reg, value, hz, mode});
  }
} bus;

TestSerial Serial;
void pinMode(int, int) {}
void digitalWrite(int, int value) { bus.selected = value == LOW; }
int digitalRead(int) { return 0; }
void delay(unsigned long) {}
void delayMicroseconds(unsigned int) {}
int gpio_set_pull_mode(gpio_num_t, gpio_pull_mode_t) { return 0; }
int64_t esp_timer_get_time(void) { return 0; }

SPIClass::SPIClass(uint8_t) {}
void SPIClass::begin(int, int, int, int) { started_ = true; bus.started = true; }
void SPIClass::end() { started_ = false; bus.started = false; }
void SPIClass::beginTransaction(SPISettings settings) {
  bus.hz = settings.hz;
  bus.mode = settings.mode;
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
    // FIFO_DATA streams bytes without advancing into the next register.
    if (address_ == MPUREG_FIFO_DATA) {
      return readFifoByteForTest();
    }
    return bus.read(address_++);
  }
  bus.write(address_++, value);
  return 0;
}

struct VendorState {
  int initRc = 0;
  int clkinStatus = 1;
  int clkinRequests = 0;
  int fifoConfig = -1;
  int watermark = -1;
  int int1Fifo = -1;
  int enableAccelCalls = 0;
  int disableAccelCalls = 0;
  int fifoCalls = 0;
  int resets = 0;
  int odr = -1;
  int fsr = -1;
  int packets = 0;
  std::vector<AccelSample> events;
} vendor;

std::vector<uint8_t> fifoStream;
std::size_t fifoPos = 0;

void buildFifoStream() {
  fifoStream.clear();
  fifoPos = 0;
  for (const AccelSample& sample : vendor.events) {
    fifoStream.push_back(FIFO_HEADER_ACC | FIFO_HEADER_GYRO | FIFO_HEADER_TMST);
    const int16_t axis[3] = {sample.x, sample.y, sample.z};
    for (int16_t value : axis) {
      fifoStream.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
      fifoStream.push_back(static_cast<uint8_t>(value & 0xff));
    }
    for (int i = 0; i < 6 + 1 + 2; ++i) {
      fifoStream.push_back(0);
    }
  }
}

uint8_t readFifoByteForTest() {
  if (fifoStream.empty()) {
    buildFifoStream();
  }
  return fifoPos < fifoStream.size() ? fifoStream[fifoPos++] : 0;
}
extern "C" int inv_ixm42xxx_init(struct inv_ixm42xxx* s,
                                  struct inv_ixm42xxx_serif* serif,
                                  void (*callback)(inv_ixm42xxx_sensor_event_t*)) {
  std::memset(s, 0, sizeof(*s));
  s->transport.serif = *serif;
  s->sensor_event_cb = callback;
  s->endianess_data = IXM42XXX_INTF_CONFIG0_DATA_BIG_ENDIAN;
  // The real chip powers up with 20-bit FIFO packets enabled.
  bus.regs[0][MPUREG_FIFO_CONFIG1] = BIT_FIFO_CONFIG1_HIRES_MASK;
  return vendor.initRc;
}

extern "C" int inv_ixm42xxx_set_reg_bank(struct inv_ixm42xxx* s, uint8_t bank) {
  s->transport.register_cache.bank_sel_reg = bank & 0x07;
  bus.bank = bank & 0x07;
  return 0;
}

extern "C" int inv_ixm42xxx_read_reg(struct inv_ixm42xxx* s, uint8_t reg,
                                     uint32_t length, uint8_t* buffer) {
  const uint8_t bank = s->transport.register_cache.bank_sel_reg & 0x07;
  for (uint32_t i = 0; i < length; ++i) {
    const uint8_t address = static_cast<uint8_t>((reg + i) & 0x7f);
    uint8_t value = 0;
    if (bank == 0 && address == MPUREG_FIFO_COUNTH) {

      value = static_cast<uint8_t>(vendor.packets & 0xff);
    } else if (bank == 0 && address == MPUREG_FIFO_COUNTH + 1) {
      value = static_cast<uint8_t>((vendor.packets >> 8) & 0xff);
    } else if (bank == 0 && address == MPUREG_FIFO_DATA) {
      value = readFifoByteForTest();
    } else {
      value = bus.regs[bank][address];
    }
    buffer[i] = value;
  }
  return 0;
}

extern "C" int inv_ixm42xxx_write_reg(struct inv_ixm42xxx* s, uint8_t reg,
                                      uint32_t length, const uint8_t* buffer) {
  const uint8_t bank = s->transport.register_cache.bank_sel_reg & 0x07;
  for (uint32_t i = 0; i < length; ++i) {
    const uint8_t address = static_cast<uint8_t>((reg + i) & 0x7f);
    if (address == MPUREG_REG_BANK_SEL) {
      s->transport.register_cache.bank_sel_reg = buffer[i] & 0x07;
      bus.bank = buffer[i] & 0x07;
    } else {
      bus.regs[bank][address] = buffer[i];
    }
  }
  return 0;
}

extern "C" int inv_ixm42xxx_set_accel_frequency(
    struct inv_ixm42xxx* s, const IXM42XXX_ACCEL_CONFIG0_ODR_t frequency) {
  vendor.odr = static_cast<int>(frequency);
  s->transport.register_cache.accel_cfg_0_reg =
      static_cast<uint8_t>((s->transport.register_cache.accel_cfg_0_reg & 0xf0) |
                           static_cast<uint8_t>(frequency));
  bus.regs[0][MPUREG_ACCEL_CONFIG0] =
      s->transport.register_cache.accel_cfg_0_reg;
  return 0;
}

extern "C" int inv_ixm42xxx_set_accel_fsr(
    struct inv_ixm42xxx* s, IXM42XXX_ACCEL_CONFIG0_FS_SEL_t range) {
  vendor.fsr = static_cast<int>(range);
  s->transport.register_cache.accel_cfg_0_reg =
      static_cast<uint8_t>((s->transport.register_cache.accel_cfg_0_reg & 0x1f) |
                           static_cast<uint8_t>(range));
  bus.regs[0][MPUREG_ACCEL_CONFIG0] =
      s->transport.register_cache.accel_cfg_0_reg;
  return 0;
}

extern "C" int inv_ixm42xxx_configure_fifo(
    struct inv_ixm42xxx* s, INV_IXM42XXX_FIFO_CONFIG_t config) {
  (void)s;
  vendor.fifoConfig = static_cast<int>(config);
  bus.regs[0][MPUREG_FIFO_CONFIG] =
      static_cast<uint8_t>(IXM42XXX_FIFO_CONFIG_MODE_STOP_ON_FULL);
  bus.regs[0][MPUREG_FIFO_CONFIG1] = static_cast<uint8_t>(
      (bus.regs[0][MPUREG_FIFO_CONFIG1] &
       ~(BIT_FIFO_CONFIG1_ACCEL_MASK | BIT_FIFO_CONFIG1_GYRO_MASK)) |
      BIT_FIFO_CONFIG1_TEMP_MASK | BIT_FIFO_CONFIG1_TMST_FSYNC_MASK |
      BIT_FIFO_CONFIG1_WM_GT_TH_MASK);
  return 0;
}

extern "C" int inv_ixm42xxx_configure_fifo_wm(struct inv_ixm42xxx* s,
                                               uint16_t watermark) {
  (void)s;
  vendor.watermark = watermark;
  bus.regs[0][MPUREG_FIFO_CONFIG2] = static_cast<uint8_t>(watermark);
  return 0;
}

extern "C" int inv_ixm42xxx_disable_high_resolution_fifo(
    struct inv_ixm42xxx* s) {
  s->fifo_highres_enabled = 0;
  return 0;
}

extern "C" int inv_ixm42xxx_set_config_int1(
    struct inv_ixm42xxx* s, inv_ixm42xxx_interrupt_parameter_t* config) {
  (void)s;
  vendor.int1Fifo = config->INV_IXM42XXX_FIFO_THS;
  bus.regs[0][MPUREG_INT_SOURCE0] =
      vendor.int1Fifo > 0 ? static_cast<uint8_t>(BIT_INT_SOURCE0_FIFO_THS_INT1_EN)
                          : 0;
  return 0;
}

extern "C" int inv_ixm42xxx_get_config_int1(
    struct inv_ixm42xxx* s, inv_ixm42xxx_interrupt_parameter_t* config) {
  (void)s;
  std::memset(config, 0, sizeof(*config));
  config->INV_IXM42XXX_FIFO_THS = vendor.int1Fifo > 0 ? INV_IXM42XXX_ENABLE
                                                       : INV_IXM42XXX_DISABLE;
  return 0;
}

extern "C" int inv_ixm42xxx_reset_fifo(struct inv_ixm42xxx* s) {
  (void)s;
  ++vendor.resets;
  return 0;
}

extern "C" int inv_ixm42xxx_enable_clkin_rtc(struct inv_ixm42xxx* s,
                                             uint8_t enable) {
  (void)s;
  ++vendor.clkinRequests;
  vendor.clkinStatus = enable ? 1 : 0;
  if (enable) {
    bus.regs[0][MPUREG_INTF_CONFIG1] = static_cast<uint8_t>(
        (bus.regs[0][MPUREG_INTF_CONFIG1] & ~BIT_RTC_MODE_MASK) |
        static_cast<uint8_t>(IXM42XXX_INTF_CONFIG1_RTC_MODE_EN));
    bus.regs[1][MPUREG_INTF_CONFIG5_B1] =
        static_cast<uint8_t>(2 << BIT_INTF_CONFIG5_GPIO_PAD_SEL_POS);
  } else {
    bus.regs[0][MPUREG_INTF_CONFIG1] &= static_cast<uint8_t>(~BIT_RTC_MODE_MASK);
  }
  return 0;
}

extern "C" int inv_ixm42xxx_get_clkin_rtc_status(struct inv_ixm42xxx* s) {
  (void)s;
  return vendor.clkinStatus;
}

extern "C" int inv_ixm42xxx_enable_accel_low_noise_mode(
    struct inv_ixm42xxx* s) {
  (void)s;
  ++vendor.enableAccelCalls;
  bus.regs[0][MPUREG_PWR_MGMT_0] =
      static_cast<uint8_t>((bus.regs[0][MPUREG_PWR_MGMT_0] & ~0x03) | 0x02);
  bus.regs[0][MPUREG_FIFO_CONFIG1] |= static_cast<uint8_t>(
      BIT_FIFO_CONFIG1_ACCEL_MASK | BIT_FIFO_CONFIG1_GYRO_MASK);
  return 0;
}

extern "C" int inv_ixm42xxx_disable_accel(struct inv_ixm42xxx* s) {
  (void)s;
  ++vendor.disableAccelCalls;
  bus.regs[0][MPUREG_PWR_MGMT_0] &= static_cast<uint8_t>(~0x03);
  return 0;
}

extern "C" int inv_ixm42xxx_get_data_from_fifo(struct inv_ixm42xxx* s) {
  ++vendor.fifoCalls;
  s->fifo_data[0] = 0x48;
  for (const AccelSample& sample : vendor.events) {
    inv_ixm42xxx_sensor_event_t event = {};
    event.sensor_mask = 1 << INV_IXM42XXX_SENSOR_ACCEL;
    event.accel[0] = sample.x;
    event.accel[1] = sample.y;
    event.accel[2] = sample.z;
    if (s->sensor_event_cb) {
      s->sensor_event_cb(&event);
    }
  }
  vendor.events.clear();
  vendor.packets = 0;
  return 0;
}

void resetState() {
  bus.defaults();
  bus.writes.clear();
  bus.hz = 0;
  bus.mode = 0;
  bus.selected = false;
  vendor = VendorState{};
  fifoStream.clear();
  fifoPos = 0;
}

void require(bool condition, const char* message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void transportContract() {
  resetState();
  SensorSpiTransport transport(47, 39, 48, 40);
  require(transport.begin(), "transport begin failed");
  const uint8_t value = 0x49;
  require(transport.write(0x50, &value, 1) == 0, "transport write failed");
  bus.regs[0][0x75] = 0x6f;
  uint8_t readback = 0;
  require(transport.read(0x75, &readback, 1) == 0 && readback == 0x6f,
          "transport read failed");
  require(bus.hz == 100000, "transport did not use 100 kHz");
  require(bus.mode == SPI_MODE0, "transport did not use mode 0");
  require(bus.writes.size() == 1 && bus.writes[0].reg == 0x50 &&
              bus.writes[0].value == 0x49,
          "transport write framing changed");
  transport.end();
}

void driverConfiguration() {
  resetState();
  Iim42652 sensor;
  const bool begun = sensor.begin(false, true);
  if (!begun) {
    std::cout << Serial.output;
  }
  require(begun, "driver begin failed");
  require(sensor.expectedIdentity() == 0x6f, "unexpected identity");
  require(sensor.readRegister(0x75) == 0x6f, "driver register read failed");
  require(vendor.clkinRequests > 0 && vendor.clkinStatus == 1,
          "CLKIN was not enabled");
  require(vendor.odr == IXM42XXX_ACCEL_CONFIG0_ODR_50_HZ,
          "default rate is not 50 Hz");
  require(vendor.fsr == IXM42XXX_ACCEL_CONFIG0_FS_SEL_4g,
          "default range is not 4 g");
  require(vendor.fifoConfig == INV_IXM42XXX_FIFO_ENABLED,
          "FIFO was not enabled");
  require(vendor.watermark == 1, "FIFO watermark is not one packet");
  require((bus.regs[0][MPUREG_FIFO_CONFIG1] &
           BIT_FIFO_CONFIG1_HIRES_MASK) == 0,
          "high-resolution FIFO was not disabled");
  require(vendor.int1Fifo == INV_IXM42XXX_ENABLE,
          "INT1 FIFO threshold was not enabled");

  // readRegister must be a physical read. A value changed behind the vendor
  // register cache has to be visible, otherwise clock verification would only
  // echo what the driver believes it wrote.
  bus.regs[0][MPUREG_INTF_CONFIG1] = 0x00;
  require(sensor.readRegister(MPUREG_INTF_CONFIG1) == 0x00,
          "readRegister did not bypass the vendor cache");
}

void fifoOrderAndCount() {
  resetState();
  Iim42652 sensor;
  require(sensor.begin(false, true), "initialization failed");
  sensor.start();
  require(sensor.fifoPacketBytes() == 16,
          "capture packet size is not accel+gyro+temp+timestamp");
  vendor.packets = 2;
  vendor.events = {{1, 2, 3}, {4, 5, 6}};
  require(sensor.available() == 2, "FIFO sample count is wrong");
  require(sensor.lastFifoBytes() == 32, "packet size accounting is wrong");
  AccelSample first{};
  AccelSample second{};
  require(sensor.read(first) && first.x == 1 && first.y == 2 && first.z == 3,
          "first FIFO sample is wrong");
  require(sensor.read(second) && second.x == 4 && second.y == 5 && second.z == 6,
          "second FIFO sample is out of order");
  require(!sensor.read(first), "empty buffer returned a sample");
}

void fifoOverflowGuard() {
  resetState();
  Iim42652 sensor;
  require(sensor.begin(false, true), "initialization failed");
  sensor.start();
  vendor.packets = 200;
  require(sensor.available() == 0, "oversized FIFO count was accepted");
  require(!sensor.fifoCountValid(), "oversized FIFO count passed validation");
}

void standbyClearsSamples() {
  resetState();
  Iim42652 sensor;
  require(sensor.begin(false, true), "initialization failed");
  sensor.start();
  vendor.packets = 1;
  vendor.events = {{1, 2, 3}};
  require(sensor.available() == 1, "sample was not buffered");
  sensor.standby();
  require(vendor.disableAccelCalls == 1, "accelerometer was not disabled");
  AccelSample sample{};
  require(!sensor.read(sample), "standby left a stale sample");
}

void invalidSettings() {
  resetState();
  Iim42652 sensor;
  require(sensor.begin(false, true), "initialization failed");
  require(!sensor.configure(60, 4), "invalid sample rate was accepted");
  require(!sensor.configure(50, 3), "invalid range was accepted");
}

void startupPolicy() {
  using SensorChecks::StartupCheck;
  using SensorChecks::startupPassed;
  StartupCheck good{12, 12, 3840, true, true, true};
  require(startupPassed(good), "normal startup rejected");
  auto bad = good;
  bad.sampleValid = false;
  require(!startupPassed(bad), "invalid sample passed startup");
  bad = good;
  bad.dataReadyEdges = 6400;
  require(!startupPassed(bad), "clock-rate IRQ passed startup");
}

void recoveryPolicy() {
  using namespace WireProtocol;
  using SensorChecks::canRetry;
  require(canRetry(Fault, false, 0) && canRetry(Waiting, false, 0),
          "idle recovery rejected");
  require(!canRetry(Fault, true, 0) && !canRetry(Waiting, false, 1),
          "active or queued recovery was allowed");
}

int main() {
  struct Test {
    const char* name;
    void (*run)();
  };
  const std::vector<Test> tests = {
      {"spi_transport", transportContract},
      {"vendor_configuration", driverConfiguration},
      {"fifo_order", fifoOrderAndCount},
      {"fifo_overflow", fifoOverflowGuard},
      {"standby_clear", standbyClearsSamples},
      {"invalid_settings", invalidSettings},
      {"startup_policy", startupPolicy},
      {"recovery_policy", recoveryPolicy},
  };

  unsigned failures = 0;
  for (const auto& test : tests) {
    try {
      test.run();
      std::cout << "PASS " << test.name << '\n';
    } catch (const std::exception& error) {
      ++failures;
      std::cout << "FAIL " << test.name << ": " << error.what() << '\n';
    }
  }
  std::cout << "RESULT " << tests.size() - failures << '/' << tests.size()
            << " passed\n";
  return failures == 0 ? 0 : 1;
}

