#include "IimBase.h"
#include <driver/gpio.h>

#include "SelectedBoard.h"
#include "sensor/Registers.h"
IimBase::IimBase(const char* name, uint8_t identity)
    : spi_(HSPI), name_(name), identity_(identity) {}
void IimBase::writeRegister(uint8_t address, uint8_t value) {
  spi_.beginTransaction(SPISettings(1000000, MSBFIRST, SPI_MODE0));
  digitalWrite(Board::sensorCs, LOW);
  spi_.transfer(address & 0x7f);
  spi_.transfer(value);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.endTransaction();
}
void IimBase::readBytes(uint8_t address, uint8_t* data, size_t count) {
  spi_.beginTransaction(SPISettings(1000000, MSBFIRST, SPI_MODE0));
  digitalWrite(Board::sensorCs, LOW);
  spi_.transfer(address | 0x80);
  for (size_t i = 0; i < count; ++i)
    data[i] = spi_.transfer(0);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.endTransaction();
}
uint8_t IimBase::readRegister(uint8_t address) {
  uint8_t value;
  readBytes(address, &value, 1);
  return value;
}
bool IimBase::begin() {
  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi, Board::sensorCs);
  delay(5);
  writeRegister(IimRegister::REG_BANK_SEL, 0);
  bool found = false;
  uint8_t observedIdentity = 0xff;
  unsigned attempts = 0;
  for (; attempts < 20; ++attempts) {
    observedIdentity = readRegister(IimRegister::WHO_AM_I);
    if (observedIdentity == identity_) {
      found = true;
      break;
    }
    delay(10);
  }
  Serial.printf("SENSOR_ID,name=%s,expected=0x%02X,actual=0x%02X,ok=%u\n", name_,
                identity_, observedIdentity, found);
  if (!found) {
    Serial.println("SENSOR_ERROR,stage=identity");
    return false;
  }
  writeRegister(IimRegister::DEVICE_CONFIG, 1);
  delay(50);
  writeRegister(IimRegister::REG_BANK_SEL, 0);
  observedIdentity = readRegister(IimRegister::WHO_AM_I);
  Serial.printf("SENSOR_RESET,identity=0x%02X,ok=%u\n", observedIdentity,
                observedIdentity == identity_);
  if (observedIdentity != identity_) {
    Serial.println("SENSOR_ERROR,stage=identity_after_reset");
    diagnostic();
    return false;
  }
  standby();
  writeRegister(IimRegister::FIFO_CONFIG, 0x40);  // FIFO stream mode.
  configureFifo();
  writeRegister(IimRegister::FIFO_CONFIG2, 8);
  writeRegister(IimRegister::FIFO_CONFIG3, 0);
  writeRegister(IimRegister::INT_CONFIG, 0x03);  // INT1 active high, push-pull, pulse.
  writeRegister(IimRegister::INT_CONFIG1, readRegister(IimRegister::INT_CONFIG1) &
                                              ~0x10);  // Required INT_ASYNC_RESET setting.
  writeRegister(IimRegister::INTF_CONFIG1,
                readRegister(IimRegister::INTF_CONFIG1) | 0x04);  // RTC_MODE.
  writeRegister(IimRegister::REG_BANK_SEL, 1);
  uint8_t pin9 = readRegister(IimRegister::INTF_CONFIG5);
  writeRegister(IimRegister::INTF_CONFIG5, (pin9 & ~0x06) | 0x04);
  writeRegister(IimRegister::REG_BANK_SEL, 0);
  writeRegister(IimRegister::INT_SOURCE0, 0x08);  // UI data ready -> INT1.
  const bool accelOk = configure(100, 4);
  const uint8_t clockConfig = readRegister(IimRegister::INTF_CONFIG1);
  Serial.printf(
      "SENSOR_CONFIG,accel=0x%02X,accel_ok=%u,clock=0x%02X,clock_select_ok=%u,"
      "fifo=0x%02X,int_source=0x%02X,int_config=0x%02X\n",
      readRegister(IimRegister::ACCEL_CONFIG0), accelOk, clockConfig, bool(clockConfig & 4),
      readRegister(IimRegister::FIFO_CONFIG1), readRegister(IimRegister::INT_SOURCE0),
      readRegister(IimRegister::INT_CONFIG));
  if (!accelOk || !(clockConfig & 4))
    Serial.println("SENSOR_ERROR,stage=configuration");
  return accelOk && (clockConfig & 4);
}
bool IimBase::configure(uint16_t rate, uint8_t range) {
  if ((rate != 50 && rate != 100) || (range != 2 && range != 4 && range != 8 && range != 16))
    return false;
  uint8_t fs = range == 2 ? 3 : range == 4 ? 2 : range == 8 ? 1 : 0;
  uint8_t value = (fs << 5) | (rate == 50 ? 9 : 8);
  writeRegister(IimRegister::ACCEL_CONFIG0, value);
  return readRegister(IimRegister::ACCEL_CONFIG0) == value;
}
void IimBase::start() {
  writeRegister(IimRegister::PWR_MGMT0, 0x03);
}  // Accel LN, gyro OFF on both parts.
void IimBase::standby() {
  writeRegister(IimRegister::PWR_MGMT0, 0);
  delayMicroseconds(250);
  flush();
}
void IimBase::flush() {
  writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
}
uint16_t IimBase::available() {
  // COUNTL latches both bytes. Reading COUNTH first can combine old high
  // bits with a newly latched low byte (TDK DS-000440, FIFO_COUNTL).
  const uint8_t low = readRegister(IimRegister::FIFO_COUNTL);
  const uint8_t high = readRegister(IimRegister::FIFO_COUNTH);
  lastFifoBytes_ = (uint16_t(high) << 8) | low;
  return fifoCountValid() ? lastFifoBytes_ / 8 : 0;
}
bool IimBase::read(AccelSample& s) {
  uint8_t b[8];
  readBytes(IimRegister::FIFO_DATA, b, 8);
  lastHeader_ = b[0];
  if ((b[0] & 0x80) || !(b[0] & 0x40) || (b[0] & 0x20))
    return false;
  s.x = int16_t((uint16_t(b[1]) << 8) | b[2]);
  s.y = int16_t((uint16_t(b[3]) << 8) | b[4]);
  s.z = int16_t((uint16_t(b[5]) << 8) | b[6]);
  return s.x != INT16_MIN && s.y != INT16_MIN && s.z != INT16_MIN;
}

void IimBase::diagnostic() {
  for (uint8_t mode : {uint8_t(SPI_MODE0), uint8_t(SPI_MODE3)}) {
    for (uint32_t hz : {100000UL, 1000000UL}) {
      spi_.beginTransaction(SPISettings(hz, MSBFIRST, mode));
      digitalWrite(Board::sensorCs, LOW);
      delayMicroseconds(2);
      spi_.transfer(IimRegister::WHO_AM_I | 0x80);
      const uint8_t who = spi_.transfer(0);
      digitalWrite(Board::sensorCs, HIGH);
      spi_.endTransaction();
      Serial.printf("SENSOR_LINK,mode=%u,hz=%lu,who=0x%02X\n", mode, (unsigned long)hz, who);
    }
  }

  const gpio_num_t miso = static_cast<gpio_num_t>(Board::sensorMiso);
  for (gpio_pull_mode_t pull : {GPIO_FLOATING, GPIO_PULLDOWN_ONLY, GPIO_PULLUP_ONLY}) {
    gpio_set_pull_mode(miso, pull);
    delayMicroseconds(50);
    Serial.printf("SENSOR_MISO_PULL,mode=%u,idle_level=%u,who=0x%02X\n", unsigned(pull), digitalRead(Board::sensorMiso), readRegister(IimRegister::WHO_AM_I));
  }
  gpio_set_pull_mode(miso, GPIO_FLOATING);
  Serial.printf("SENSOR_DIAG,who=0x%02X,accel=0x%02X,power=0x%02X,fifo_cfg=0x%02X,fifo1=0x%02X,intf0=0x%02X,intf1=0x%02X,int_src=0x%02X,fifo_bytes=%u,header=0x%02X\n",
      readRegister(IimRegister::WHO_AM_I), readRegister(IimRegister::ACCEL_CONFIG0),
      readRegister(IimRegister::PWR_MGMT0), readRegister(IimRegister::FIFO_CONFIG),
      readRegister(IimRegister::FIFO_CONFIG1), readRegister(0x4c),
      readRegister(IimRegister::INTF_CONFIG1), readRegister(IimRegister::INT_SOURCE0),
      lastFifoBytes_, lastHeader_);
}
