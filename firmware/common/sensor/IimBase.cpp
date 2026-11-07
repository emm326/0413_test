#include "IimBase.h"

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
  for (unsigned i = 0; i < 20; ++i) {
    if (readRegister(IimRegister::WHO_AM_I) == identity_) {
      found = true;
      break;
    }
    delay(10);
  }
  if (!found)
    return false;
  writeRegister(IimRegister::DEVICE_CONFIG, 1);
  delay(50);
  writeRegister(IimRegister::REG_BANK_SEL, 0);
  if (readRegister(IimRegister::WHO_AM_I) != identity_)
    return false;
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
  return configure(100, 4) && (readRegister(IimRegister::INTF_CONFIG1) & 4);
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
  uint8_t b[2];
  readBytes(IimRegister::FIFO_COUNTH, b, 2);
  return ((uint16_t(b[0]) << 8) | b[1]) / 8;
}
bool IimBase::read(AccelSample& s) {
  uint8_t b[8];
  readBytes(IimRegister::FIFO_DATA, b, 8);
  if ((b[0] & 0x80) || !(b[0] & 0x40) || (b[0] & 0x20))
    return false;
  s.x = int16_t((uint16_t(b[1]) << 8) | b[2]);
  s.y = int16_t((uint16_t(b[3]) << 8) | b[4]);
  s.z = int16_t((uint16_t(b[5]) << 8) | b[6]);
  return s.x != INT16_MIN && s.y != INT16_MIN && s.z != INT16_MIN;
}
