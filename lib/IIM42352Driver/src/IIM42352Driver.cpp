#include "IIM42352Driver.h"

IIM42352Driver::IIM42352Driver(SPIClass& spi,
                               int8_t sckPin,
                               int8_t misoPin,
                               int8_t mosiPin,
                               int8_t csPin,
                               uint32_t spiClockHz)
    : spi_(spi),
      settings_(spiClockHz, MSBFIRST, SPI_MODE0),
      sckPin_(sckPin),
      misoPin_(misoPin),
      mosiPin_(mosiPin),
      csPin_(csPin),
      lastDeviceId_(0x00),
      range_(Range::Range4G),
      dataRate_(DataRate::Hz100) {}

bool IIM42352Driver::begin() {
  pinMode(csPin_, OUTPUT);
  deselect();
  spi_.begin(sckPin_, misoPin_, mosiPin_, csPin_);
  delay(5);

  writeRegister(REG_BANK_SEL, 0x00);
  if (!checkDeviceId()) {
    return false;
  }

  writeRegister(REG_DEVICE_CONFIG, 0x01);
  delay(20);
  writeRegister(REG_BANK_SEL, 0x00);
  if (!checkDeviceId()) {
    return false;
  }

  writeRegister(REG_PWR_MGMT0, 0x00);
  delay(2);
  writeRegister(REG_FIFO_CONFIG, 0x40);
  writeRegister(REG_FIFO_CONFIG1, 0x01);
  writeRegister(REG_FIFO_CONFIG2, 0x80);
  writeRegister(REG_FIFO_CONFIG3, 0x00);
  writeRegister(REG_INT_CONFIG, 0x18);
  configureExternalClockAndDataReady();
  setRange(range_);
  setDataRate(dataRate_);
  writeRegister(REG_PWR_MGMT0, 0x03);
  delay(20);
  return true;
}

bool IIM42352Driver::checkDeviceId() {
  lastDeviceId_ = readRegister(REG_WHO_AM_I);
  return lastDeviceId_ == WHO_AM_I_EXPECTED;
}

uint8_t IIM42352Driver::lastDeviceId() const { return lastDeviceId_; }

uint8_t IIM42352Driver::expectedDeviceId() const { return WHO_AM_I_EXPECTED; }

uint8_t IIM42352Driver::regDataFormat() const { return REG_ACCEL_CONFIG0; }

uint8_t IIM42352Driver::regPowerCtl() const { return REG_PWR_MGMT0; }

uint8_t IIM42352Driver::regFifoStatus() const { return REG_INT_STATUS; }

uint8_t IIM42352Driver::regIntConfig() const { return REG_INT_CONFIG; }

uint8_t IIM42352Driver::regIntStatus() const { return REG_INT_STATUS; }

uint8_t IIM42352Driver::regIntConfig1() const { return REG_INT_CONFIG1; }

uint8_t IIM42352Driver::regIntSource0() const { return REG_INT_SOURCE0; }

uint8_t IIM42352Driver::regIntfConfig1() const { return REG_INTF_CONFIG1; }

uint8_t IIM42352Driver::regIntfConfig5() const { return REG_BANK1_INTF_CONFIG5; }

void IIM42352Driver::setRange(Range range) {
  range_ = range;
  const uint8_t value = (static_cast<uint8_t>(range_) << 5) |
                        (static_cast<uint8_t>(dataRate_) & 0x0F);
  writeRegister(REG_ACCEL_CONFIG0, value);
}

uint8_t IIM42352Driver::accelConfig0() { return readRegister(REG_ACCEL_CONFIG0); }

bool IIM42352Driver::rangeConfigured(Range range) {
  const uint8_t expected =
      static_cast<uint8_t>(static_cast<uint8_t>(range) << 5) & MASK_ACCEL_FS_SEL;
  return (accelConfig0() & MASK_ACCEL_FS_SEL) == expected;
}

void IIM42352Driver::setFullResolution(bool) {}

void IIM42352Driver::setDataRate(DataRate rate) {
  dataRate_ = rate;
  const uint8_t value = (static_cast<uint8_t>(range_) << 5) |
                        (static_cast<uint8_t>(dataRate_) & 0x0F);
  writeRegister(REG_ACCEL_CONFIG0, value);
}

bool IIM42352Driver::configureExternalClockAndDataReady() {
  uint8_t intConfig1 = readRegister(REG_INT_CONFIG1);
  intConfig1 &= static_cast<uint8_t>(~BIT_INT_ASYNC_RESET);
  writeRegister(REG_INT_CONFIG1, intConfig1);

  uint8_t intfConfig1 = readRegister(REG_INTF_CONFIG1);
  intfConfig1 |= BIT_RTC_MODE;
  writeRegister(REG_INTF_CONFIG1, intfConfig1);

  uint8_t intfConfig5 = readBankedRegister(1, REG_BANK1_INTF_CONFIG5);
  intfConfig5 &= static_cast<uint8_t>(~MASK_PIN9_FUNCTION);
  intfConfig5 |= PIN9_FUNCTION_CLKIN;
  writeBankedRegister(1, REG_BANK1_INTF_CONFIG5, intfConfig5);

  uint8_t intSource0 = readRegister(REG_INT_SOURCE0);
  intSource0 |= BIT_UI_DRDY_INT1_EN;
  writeRegister(REG_INT_SOURCE0, intSource0);

  return externalClockConfigured() && dataReadyInt1Configured();
}

bool IIM42352Driver::restartAccel() {
  writeRegister(REG_PWR_MGMT0, 0x00);
  delay(2);
  configureExternalClockAndDataReady();
  setRange(range_);
  setDataRate(dataRate_);
  writeRegister(REG_PWR_MGMT0, 0x03);
  delay(20);
  return externalClockConfigured() && dataReadyInt1Configured();
}

bool IIM42352Driver::externalClockConfigured() {
  const uint8_t intfConfig1 = readRegister(REG_INTF_CONFIG1);
  const uint8_t intfConfig5 = readBankedRegister(1, REG_BANK1_INTF_CONFIG5);
  return (intfConfig1 & BIT_RTC_MODE) != 0 &&
         (intfConfig5 & MASK_PIN9_FUNCTION) == PIN9_FUNCTION_CLKIN;
}

bool IIM42352Driver::dataReadyInt1Configured() {
  const uint8_t intConfig1 = readRegister(REG_INT_CONFIG1);
  const uint8_t intSource0 = readRegister(REG_INT_SOURCE0);
  return (intConfig1 & BIT_INT_ASYNC_RESET) == 0 &&
         (intSource0 & BIT_UI_DRDY_INT1_EN) != 0;
}

bool IIM42352Driver::readXYZRaw(int16_t& x, int16_t& y, int16_t& z) {
  uint8_t raw[6] = {0};
  if (!readMulti(REG_ACCEL_DATA_X1, raw, sizeof(raw))) {
    return false;
  }

  x = static_cast<int16_t>((static_cast<uint16_t>(raw[0]) << 8) | raw[1]);
  y = static_cast<int16_t>((static_cast<uint16_t>(raw[2]) << 8) | raw[3]);
  z = static_cast<int16_t>((static_cast<uint16_t>(raw[4]) << 8) | raw[5]);
  return true;
}

uint8_t IIM42352Driver::fifoAvailable() {
  const uint16_t bytes = fifoByteCount();
  const uint16_t packets = bytes / FIFO_PACKET_SIZE;
  return packets > 255 ? 255 : static_cast<uint8_t>(packets);
}

size_t IIM42352Driver::readFifoBurst(RawSample* outSamples, size_t maxSamples) {
  if (outSamples == nullptr || maxSamples == 0) {
    return 0;
  }

  size_t actual = 0;
  const size_t available = fifoAvailable();
  const size_t toRead = min(maxSamples, available);
  for (size_t i = 0; i < toRead; ++i) {
    RawSample sample = {};
    if (!readFifoPacket(sample)) {
      break;
    }
    outSamples[actual++] = sample;
  }
  return actual;
}

uint8_t IIM42352Driver::readRegister(uint8_t reg) {
  spi_.beginTransaction(settings_);
  select();
  spi_.transfer(reg | 0x80);
  const uint8_t value = spi_.transfer(0x00);
  deselect();
  spi_.endTransaction();
  return value;
}

void IIM42352Driver::writeRegister(uint8_t reg, uint8_t value) {
  spi_.beginTransaction(settings_);
  select();
  spi_.transfer(reg & 0x7F);
  spi_.transfer(value);
  deselect();
  spi_.endTransaction();
}

uint8_t IIM42352Driver::readBankedRegister(uint8_t bank, uint8_t reg) {
  writeRegister(REG_BANK_SEL, bank);
  const uint8_t value = readRegister(reg);
  writeRegister(REG_BANK_SEL, 0x00);
  return value;
}

void IIM42352Driver::writeBankedRegister(uint8_t bank, uint8_t reg, uint8_t value) {
  writeRegister(REG_BANK_SEL, bank);
  writeRegister(reg, value);
  writeRegister(REG_BANK_SEL, 0x00);
}

bool IIM42352Driver::readMulti(uint8_t startReg, uint8_t* buffer, size_t length) {
  if (buffer == nullptr || length == 0) {
    return false;
  }

  spi_.beginTransaction(settings_);
  select();
  spi_.transfer(startReg | 0x80);
  for (size_t i = 0; i < length; ++i) {
    buffer[i] = spi_.transfer(0x00);
  }
  deselect();
  spi_.endTransaction();
  return true;
}

bool IIM42352Driver::readFifoPacket(RawSample& outSample) {
  uint8_t raw[FIFO_PACKET_SIZE] = {0};
  if (!readMulti(REG_FIFO_DATA, raw, sizeof(raw))) {
    return false;
  }

  const uint8_t header = raw[0];
  if ((header & FIFO_HEADER_EMPTY) != 0 || (header & FIFO_HEADER_ACCEL) == 0) {
    return false;
  }

  outSample.x = static_cast<int16_t>((static_cast<uint16_t>(raw[1]) << 8) | raw[2]);
  outSample.y = static_cast<int16_t>((static_cast<uint16_t>(raw[3]) << 8) | raw[4]);
  outSample.z = static_cast<int16_t>((static_cast<uint16_t>(raw[5]) << 8) | raw[6]);
  return true;
}

uint16_t IIM42352Driver::fifoByteCount() {
  uint8_t raw[2] = {0};
  if (!readMulti(REG_FIFO_COUNTH, raw, sizeof(raw))) {
    return 0;
  }
  return static_cast<uint16_t>((static_cast<uint16_t>(raw[0]) << 8) | raw[1]);
}

void IIM42352Driver::select() { digitalWrite(csPin_, LOW); }

void IIM42352Driver::deselect() { digitalWrite(csPin_, HIGH); }
