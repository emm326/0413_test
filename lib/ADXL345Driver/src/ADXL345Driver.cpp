#include "ADXL345Driver.h"

ADXL345Driver::ADXL345Driver(TwoWire& wire,
                             uint8_t address,
                             int8_t sdaPin,
                             int8_t sclPin,
                             uint32_t wireClockHz)
    : wire_(wire),
      address_(address),
      sdaPin_(sdaPin),
      sclPin_(sclPin),
      wireClockHz_(wireClockHz),
      lastDeviceId_(0x00) {}

bool ADXL345Driver::begin() {
  constexpr uint8_t kDefaultFifoWatermark = 16;

  wire_.begin(sdaPin_, sclPin_, wireClockHz_);

  if (!checkDeviceId()) {
    return false;
  }

  writeRegister(REG_POWER_CTL, 0x00);
  writeRegister(REG_INT_ENABLE, 0x00);
  setRange(Range::Range4G);
  setFullResolution(true);
  setDataRate(DataRate::Hz100);
  writeRegister(REG_FIFO_CTL, FIFO_MODE_STREAM | (kDefaultFifoWatermark & 0x1F));
  writeRegister(REG_POWER_CTL, POWER_CTL_MEASURE);
  delay(10);
  return true;
}

bool ADXL345Driver::checkDeviceId() {
  lastDeviceId_ = readRegister(REG_DEVID);
  return lastDeviceId_ == DEVID_EXPECTED;
}

uint8_t ADXL345Driver::lastDeviceId() const { return lastDeviceId_; }

uint8_t ADXL345Driver::expectedDeviceId() const { return DEVID_EXPECTED; }

uint8_t ADXL345Driver::regDataFormat() const { return REG_DATA_FORMAT; }

uint8_t ADXL345Driver::regPowerCtl() const { return REG_POWER_CTL; }

uint8_t ADXL345Driver::regFifoStatus() const { return REG_FIFO_STATUS; }

void ADXL345Driver::setRange(Range range) {
  uint8_t value = readRegister(REG_DATA_FORMAT);
  value &= ~0x03;
  value |= static_cast<uint8_t>(range) & 0x03;
  writeRegister(REG_DATA_FORMAT, value);
}

void ADXL345Driver::setFullResolution(bool en) {
  uint8_t value = readRegister(REG_DATA_FORMAT);
  if (en) {
    value |= DATA_FORMAT_FULL_RES;
  } else {
    value &= ~DATA_FORMAT_FULL_RES;
  }
  writeRegister(REG_DATA_FORMAT, value);
}

void ADXL345Driver::setDataRate(DataRate rate) {
  writeRegister(REG_BW_RATE, static_cast<uint8_t>(rate));
}

bool ADXL345Driver::readXYZRaw(int16_t& x, int16_t& y, int16_t& z) {
  uint8_t raw[6] = {0};
  if (!readMulti(REG_DATAX0, raw, sizeof(raw))) {
    return false;
  }

  x = static_cast<int16_t>((static_cast<uint16_t>(raw[1]) << 8) | raw[0]);
  y = static_cast<int16_t>((static_cast<uint16_t>(raw[3]) << 8) | raw[2]);
  z = static_cast<int16_t>((static_cast<uint16_t>(raw[5]) << 8) | raw[4]);
  return true;
}

uint8_t ADXL345Driver::fifoAvailable() { return readRegister(REG_FIFO_STATUS) & 0x3F; }

size_t ADXL345Driver::readFifoBurst(RawSample* outSamples, size_t maxSamples) {
  if (outSamples == nullptr || maxSamples == 0) {
    return 0;
  }

  size_t available = fifoAvailable();
  size_t toRead = min(maxSamples, available);
  size_t actual = 0;

  for (size_t i = 0; i < toRead; ++i) {
    int16_t x = 0;
    int16_t y = 0;
    int16_t z = 0;
    if (!readXYZRaw(x, y, z)) {
      break;
    }

    outSamples[actual++] = {x, y, z};
  }

  return actual;
}

uint8_t ADXL345Driver::readRegister(uint8_t reg) {
  wire_.beginTransmission(address_);
  wire_.write(reg);
  if (wire_.endTransmission(false) != 0) {
    return 0x00;
  }

  const uint8_t requested = wire_.requestFrom(static_cast<int>(address_), 1, 1);
  if (requested != 1 || !wire_.available()) {
    return 0x00;
  }

  return wire_.read();
}

void ADXL345Driver::writeRegister(uint8_t reg, uint8_t value) {
  wire_.beginTransmission(address_);
  wire_.write(reg);
  wire_.write(value);
  wire_.endTransmission(true);
}

bool ADXL345Driver::readMulti(uint8_t startReg, uint8_t* buffer, size_t length) {
  if (buffer == nullptr || length == 0) {
    return false;
  }

  wire_.beginTransmission(address_);
  wire_.write(startReg);
  if (wire_.endTransmission(false) != 0) {
    return false;
  }

  const uint8_t requested =
      wire_.requestFrom(static_cast<int>(address_), static_cast<int>(length), 1);
  if (requested != length) {
    return false;
  }

  for (size_t i = 0; i < length; ++i) {
    if (!wire_.available()) {
      return false;
    }
    buffer[i] = wire_.read();
  }

  return true;
}
