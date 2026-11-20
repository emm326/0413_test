#include "IimBase.h"

#include <Wire.h>

#include "SelectedBoard.h"
#include "sensor/Registers.h"
IimBase::IimBase(const char* name, uint8_t identity)
    : spi_(HSPI), name_(name), identity_(identity) {}
void IimBase::writeRegister(uint8_t address, uint8_t value) {
  spi_.beginTransaction(SPISettings(spiHz_, MSBFIRST, spiMode_));
  digitalWrite(Board::sensorCs, LOW);
  delayMicroseconds(2);
  spi_.transfer(address & 0x7f);
  spi_.transfer(value);
  delayMicroseconds(1);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.endTransaction();
  delayMicroseconds(20);
}
void IimBase::readBytes(uint8_t address, uint8_t* data, size_t count) {
  spi_.beginTransaction(SPISettings(spiHz_, MSBFIRST, spiMode_));
  digitalWrite(Board::sensorCs, LOW);
  delayMicroseconds(2);
  spi_.transfer(address | 0x80);
  for (size_t i = 0; i < count; ++i)
    data[i] = spi_.transfer(0);
  delayMicroseconds(1);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.endTransaction();
  delayMicroseconds(20);
}
uint8_t IimBase::readRegister(uint8_t address) {
  uint8_t value;
  readBytes(address, &value, 1);
  return value;
}
bool IimBase::writeVerified(uint8_t address, uint8_t value, uint8_t mask,
                            const char* stage) {
  for (uint8_t attempt = 0; attempt < 3; ++attempt) {
    writeRegister(address, value);
    delayMicroseconds(250);
    const uint8_t first = readRegister(address);
    const uint8_t second = readRegister(address);
    const bool ok = (first & mask) == (value & mask) &&
                    (second & mask) == (value & mask);
    Serial.printf("SENSOR_WRITE,stage=%s,addr=0x%02X,expected=0x%02X,mask=0x%02X,"
                  "read1=0x%02X,read2=0x%02X,attempt=%u,ok=%u\n", stage,
                  address, value, mask, first, second, attempt + 1, ok);
    if (ok)
      return true;
  }
  return false;
}
bool IimBase::selectBank(uint8_t bank) {
  return writeVerified(IimRegister::REG_BANK_SEL, bank, 0x07, "bank");
}
bool IimBase::stableIdentity(uint8_t& observedIdentity, uint8_t attempts) {
  uint8_t consecutive = 0;
  for (uint8_t i = 0; i < attempts; ++i) {
    observedIdentity = readRegister(IimRegister::WHO_AM_I);
    consecutive = observedIdentity == identity_ ? consecutive + 1 : 0;
    if (consecutive >= 3)
      return true;
    delay(2);
  }
  return false;
}
bool IimBase::selectTransport(uint8_t& observedIdentity) {
  for (uint8_t mode : {uint8_t(SPI_MODE0), uint8_t(SPI_MODE3)}) {
    for (uint32_t hz : {1000000UL, 100000UL}) {
      spiMode_ = mode;
      spiHz_ = hz;
      writeRegister(IimRegister::REG_BANK_SEL, 0);
      const bool stable = stableIdentity(observedIdentity);
      Serial.printf("SENSOR_PROBE,mode=%u,hz=%lu,identity=0x%02X,stable=%u\n", mode,
                    (unsigned long)hz, observedIdentity, stable);
      if (stable)
        return true;
    }
  }
  return false;
}
bool IimBase::begin(bool resetDevice, bool externalClock) {
  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  // SPIClass::begin() alone does not reattach an already initialized bus.
  spi_.end();
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi, Board::sensorCs);
  lastFifoBytes_ = 0;
  lastFifoCountRaw_ = 0;
  lastFifoCountAttempts_ = 0;
  lastFifoCountOk_ = true;
  lastFifoReadOk_ = true;
  fifoConsecutiveFailures_ = 0;
  lastFifoErrorReason_ = "none";
  lastHeader_ = 0;
  delay(5);
  uint8_t observedIdentity = 0xff;
  const bool found = selectTransport(observedIdentity);
  Serial.printf("SENSOR_ID,name=%s,expected=0x%02X,actual=0x%02X,ok=%u\n", name_,
                identity_, observedIdentity, found);
  if (!found) {
    Serial.println("SENSOR_ERROR,stage=identity");
    return false;
  }
  if (resetDevice) {
    writeRegister(IimRegister::DEVICE_CONFIG, 1);
    delay(50);
    writeRegister(IimRegister::REG_BANK_SEL, 0);
    const bool resetReady = stableIdentity(observedIdentity);
    Serial.printf("SENSOR_RESET,identity=0x%02X,ok=%u\n", observedIdentity, resetReady);
    if (!resetReady) {
      Serial.println("SENSOR_ERROR,stage=identity_after_reset");
      diagnostic();
      return false;
    }
  } else {
    Serial.printf("SENSOR_RESET,skipped=1,identity=0x%02X\n", observedIdentity);
  }
  // DS-000440 12.9/13.6: stop both sensors before any interface/FIFO changes.
  if (!selectBank(0) ||
      !writeVerified(IimRegister::PWR_MGMT0, standbyPower(), 0x1f, "stop_before_config")) {
    Serial.println("SENSOR_ERROR,stage=stop_before_config");
    return false;
  }
  delayMicroseconds(250);
  if (!configureInterface()) {
    Serial.println("SENSOR_ERROR,stage=spi_interface_readback");
    return false;
  }
  flush();
  // Enforce byte counts and big-endian data when reusing a non-reset device.
  const uint8_t interfaceConfig = readRegister(IimRegister::INTF_CONFIG0);
  writeRegister(IimRegister::INTF_CONFIG0, (interfaceConfig & ~0x70) | 0x30);
  writeRegister(IimRegister::FIFO_CONFIG, 0x40);
  configureFifo();
  writeRegister(IimRegister::FIFO_CONFIG2, 8);
  writeRegister(IimRegister::FIFO_CONFIG3, 0);
  writeRegister(IimRegister::INT_CONFIG, 0x03);
  writeRegister(IimRegister::INT_CONFIG1, readRegister(IimRegister::INT_CONFIG1) & ~0x10);
  bool pinOk = true;
  if (externalClock) {
    // Pin function must be ready before selecting the external reference.
    if (!selectBank(1)) {
      selectBank(0);
      Serial.println("SENSOR_ERROR,stage=clkin_bank_readback");
      return false;
    }
    const uint8_t pin9 = readRegister(IimRegister::INTF_CONFIG5);
    pinOk = writeVerified(IimRegister::INTF_CONFIG5, (pin9 & ~0x06) | 0x04,
                          0x06, "clkin_function");
    const bool bank0Ok = selectBank(0);
    if (!pinOk || !bank0Ok) {
      Serial.println("SENSOR_ERROR,stage=clkin_pin_readback");
      return false;
    }
  }
  const uint8_t clockBits = externalClock ? 0x05 : 0x01;
  const uint8_t clockBefore = readRegister(IimRegister::INTF_CONFIG1);
  const bool clockWritten = writeVerified(IimRegister::INTF_CONFIG1,
      (clockBefore & ~0x07) | clockBits, 0x07, "clock_source");
  writeRegister(IimRegister::INT_SOURCE0, 0x08);
  const bool accelOk = configure(100, 4);
  const uint8_t clockConfig = readRegister(IimRegister::INTF_CONFIG1);
  const bool interfaceOk = (readRegister(IimRegister::INTF_CONFIG0) & 0x70) == 0x30;
  const bool fifoOk = (readRegister(IimRegister::FIFO_CONFIG) & 0xc0) == 0x40 &&
                      (readRegister(IimRegister::FIFO_CONFIG1) & 0x7f) == 0x41 &&
                      readRegister(IimRegister::FIFO_CONFIG2) == 8 &&
                      readRegister(IimRegister::FIFO_CONFIG3) == 0;
  const bool interruptOk = readRegister(IimRegister::INT_SOURCE0) == 0x08 &&
                           (readRegister(IimRegister::INT_CONFIG) & 0x07) == 0x03 &&
                           !(readRegister(IimRegister::INT_CONFIG1) & 0x10);
  const bool identityOk = stableIdentity(observedIdentity);
  const bool clockOk = clockWritten && (clockConfig & 0x07) == clockBits;
  const bool configured = accelOk && clockOk && interfaceOk && fifoOk &&
                          interruptOk && identityOk;
  Serial.printf("SENSOR_CONFIG,accel_ok=%u,clock_select_ok=%u,pin_ok=%u,interface_ok=%u,"
                "fifo_ok=%u,interrupt_ok=%u,identity_ok=%u,ok=%u,external_clock=%u\n",
                accelOk, clockOk, pinOk, interfaceOk, fifoOk, interruptOk,
                identityOk, configured, externalClock);
  if (!configured)
    Serial.println("SENSOR_ERROR,stage=configuration_readback");
  return configured;
}

bool IimBase::configure(uint16_t rate, uint8_t range) {
  if ((rate != 50 && rate != 100) || (range != 2 && range != 4 && range != 8 && range != 16))
    return false;
  uint8_t fs = range == 2 ? 3 : range == 4 ? 2 : range == 8 ? 1 : 0;
  uint8_t value = (fs << 5) | (rate == 50 ? 9 : 8);
  return writeVerified(IimRegister::ACCEL_CONFIG0, value, 0xff, "accel");
}
void IimBase::start() {
  writeRegister(IimRegister::PWR_MGMT0, 0x03);
  // DS-000440 PWR_MGMT0: no register writes for 200us after OFF -> LN.
  delayMicroseconds(250);
}  // Accel LN, gyro OFF on both parts.
void IimBase::standby() {
  writeRegister(IimRegister::PWR_MGMT0, standbyPower());
  delayMicroseconds(250);
  flush();
}
void IimBase::flush() {
  writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
}
uint16_t IimBase::available() {
  // COUNTL latches both bytes. Read it again after COUNTH and retry if the
  // two low-byte observations disagree, which avoids a carry crossing the
  // high/low read boundary.
  lastFifoCountOk_ = false;
  lastFifoReadOk_ = true;
  lastFifoErrorReason_ = "none";
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    lastFifoCountAttempts_ = attempt;
    const uint8_t lowBefore = readRegister(IimRegister::FIFO_COUNTL);
    const uint8_t high = readRegister(IimRegister::FIFO_COUNTH);
    const uint8_t lowAfter = readRegister(IimRegister::FIFO_COUNTL);
    if (lowBefore != lowAfter) {
      lastFifoErrorReason_ = "count_unstable";
      continue;
    }
    lastFifoCountRaw_ = (uint16_t(high) << 8) | lowAfter;
    lastFifoBytes_ = lastFifoCountRaw_;
    if (!fifoCountValid()) {
      lastFifoErrorReason_ = "count_range";
      break;
    }
    lastFifoCountOk_ = true;
    fifoConsecutiveFailures_ = 0;
    return lastFifoBytes_ / fifoPacketBytes();
  }
  ++fifoErrorTotal_;
  ++fifoConsecutiveFailures_;
  lastFifoBytes_ = 0xffff;
  lastFifoReadOk_ = false;
  return 0;
}
bool IimBase::read(AccelSample& s) {
  uint8_t b[8];
  readBytes(IimRegister::FIFO_DATA, b, 8);
  lastHeader_ = b[0];
  lastFifoReadOk_ = false;
  if ((b[0] & 0x80) || !(b[0] & 0x40) || (b[0] & 0x20)) {
    lastFifoErrorReason_ = "packet_header";
    ++fifoErrorTotal_;
    ++fifoConsecutiveFailures_;
    return false;
  }
  s.x = int16_t((uint16_t(b[1]) << 8) | b[2]);
  s.y = int16_t((uint16_t(b[3]) << 8) | b[4]);
  s.z = int16_t((uint16_t(b[5]) << 8) | b[6]);
  if (s.x == INT16_MIN || s.y == INT16_MIN || s.z == INT16_MIN) {
    lastFifoErrorReason_ = "invalid_sample";
    ++fifoErrorTotal_;
    ++fifoConsecutiveFailures_;
    return false;
  }
  lastFifoReadOk_ = true;
  lastFifoErrorReason_ = "none";
  fifoConsecutiveFailures_ = 0;
  return true;
}

bool IimBase::read(FifoSample& sample) {
  sample = {};
  sample.packetBytes = fifoPacketBytes();
  sample.timed = false;
  const bool valid = read(sample.accel);
  sample.header = lastHeader_;
  return valid;
}
bool IimBase::readDirect(AccelSample& sample) {
  if (!(readRegister(IimRegister::INT_STATUS) & 0x08))
    return false;
  if (readRegister(IimRegister::WHO_AM_I) != identity_)
    return false;
  uint8_t raw[6];
  readBytes(IimRegister::ACCEL_DATA_X1, raw, sizeof(raw));
  AccelSample observed = {
      int16_t((uint16_t(raw[0]) << 8) | raw[1]),
      int16_t((uint16_t(raw[2]) << 8) | raw[3]),
      int16_t((uint16_t(raw[4]) << 8) | raw[5])};
  if (observed.x == INT16_MIN || observed.y == INT16_MIN || observed.z == INT16_MIN ||
      readRegister(IimRegister::WHO_AM_I) != identity_)
    return false;
  sample = observed;
  return true;
}

void IimBase::diagnostic() {
  // Observe the selected link without changing pin pulls or scanning other modes.
  const uint8_t identity = readRegister(IimRegister::WHO_AM_I);
  Serial.printf("SENSOR_LINK,mode=%u,hz=%lu,who=0x%02X\n", spiMode_,
                (unsigned long)spiHz_, identity);
  Serial.printf("SENSOR_DIAG,who=0x%02X,accel=0x%02X,power=0x%02X,fifo_cfg=0x%02X,"
                "fifo1=0x%02X,intf0=0x%02X,intf1=0x%02X,int_src=0x%02X,"
                "fifo_bytes=%u,header=0x%02X\n",
                identity, readRegister(IimRegister::ACCEL_CONFIG0),
                readRegister(IimRegister::PWR_MGMT0), readRegister(IimRegister::FIFO_CONFIG),
                readRegister(IimRegister::FIFO_CONFIG1), readRegister(IimRegister::INTF_CONFIG0),
                readRegister(IimRegister::INTF_CONFIG1), readRegister(IimRegister::INT_SOURCE0),
                lastFifoBytes_, lastHeader_);
}

void IimBase::linkTest() {
  // Read WHO_AM_I independently of the SPI peripheral; preserve the sensor state.
  const uint8_t hardwareBefore = readRegister(IimRegister::WHO_AM_I);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.end();

  struct Wiring {
    const char* label;
    int sck;
    int miso;
    int mosi;
    int cs;
  };
  // Try the documented wiring first. The swapped case catches modules whose
  // SDI/SDO labels were connected to the opposite board pins.
  const Wiring wirings[] = {
      {"standard", Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
       Board::sensorCs},
      {"miso_mosi_swapped", Board::sensorSck, Board::sensorMosi,
       Board::sensorMiso, Board::sensorCs},
  };

  for (const Wiring& wiring : wirings) {
    pinMode(wiring.cs, OUTPUT);
    digitalWrite(wiring.cs, HIGH);
    pinMode(wiring.sck, OUTPUT);
    pinMode(wiring.mosi, OUTPUT);
    pinMode(wiring.miso, INPUT);
    Serial.printf(
        "SENSOR_PIN_STATE,wiring=%s,sck=%d,miso=%d,mosi=%d,cs=%d,"
        "miso_level=%d,int1=%d\n",
        wiring.label, wiring.sck, wiring.miso, wiring.mosi, wiring.cs,
        digitalRead(wiring.miso), digitalRead(Board::sensorInt));

    for (uint8_t mode : {uint8_t(SPI_MODE0), uint8_t(SPI_MODE3)}) {
      const bool idleHigh = mode == SPI_MODE3;
      for (uint8_t halfPeriodUs : {uint8_t(5), uint8_t(20)}) {
        auto transfer = [&](uint8_t output) {
          uint8_t input = 0;
          for (uint8_t mask = 0x80; mask; mask >>= 1) {
            if (idleHigh)
              digitalWrite(wiring.sck, LOW);
            digitalWrite(wiring.mosi, output & mask ? HIGH : LOW);
            delayMicroseconds(halfPeriodUs);
            digitalWrite(wiring.sck, HIGH);
            delayMicroseconds(halfPeriodUs);
            input = (input << 1) | (digitalRead(wiring.miso) ? 1 : 0);
            if (!idleHigh)
              digitalWrite(wiring.sck, LOW);
          }
          return input;
        };
        uint8_t matched = 0;
        uint8_t last = 0;
        for (uint8_t attempt = 0; attempt < 4; ++attempt) {
          digitalWrite(wiring.sck, idleHigh ? HIGH : LOW);
          delayMicroseconds(20);
          digitalWrite(wiring.cs, LOW);
          delayMicroseconds(20);
          transfer(IimRegister::WHO_AM_I | 0x80);
          last = transfer(0);
          delayMicroseconds(20);
          digitalWrite(wiring.cs, HIGH);
          delayMicroseconds(20);
          matched += last == identity_;
          Serial.printf(
              "SENSOR_SOFT_SPI,wiring=%s,mode=%u,half_period_us=%u,attempt=%u,"
              "who=0x%02X\n",
              wiring.label, mode, halfPeriodUs, attempt + 1, last);
        }
        Serial.printf(
            "SENSOR_SOFT_RESULT,wiring=%s,mode=%u,half_period_us=%u,matches=%u,"
            "total=4\n",
            wiring.label, mode, halfPeriodUs, matched);
      }
    }
  }

  digitalWrite(Board::sensorCs, HIGH);
  pinMode(Board::sensorCs, OUTPUT);
  pinMode(Board::sensorSck, OUTPUT);
  pinMode(Board::sensorMosi, OUTPUT);
  pinMode(Board::sensorMiso, INPUT);
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi, Board::sensorCs);
  const uint8_t hardwareAfter = readRegister(IimRegister::WHO_AM_I);
  Serial.printf("SENSOR_LINK_TEST,hardware_before=0x%02X,hardware_after=0x%02X,"
                "spi_restored=1,sensor_reset=0\n", hardwareBefore, hardwareAfter);
}

void IimBase::i2cScan() {
  // Diagnostic only: CSB high selects I2C. SDA uses the module SDI pin and
  // SCL uses SCLK. Restore the SPI bus before returning.
  digitalWrite(Board::sensorCs, HIGH);
  spi_.end();
  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  pinMode(Board::sensorMosi, INPUT_PULLUP);
  pinMode(Board::sensorSck, INPUT_PULLUP);
  Wire.begin(Board::sensorMosi, Board::sensorSck, 100000U);

  uint8_t found = 0;
  for (uint8_t address = 0x08; address <= 0x77; ++address) {
    Wire.beginTransmission(address);
    const uint8_t result = Wire.endTransmission();
    if (result == 0) {
      ++found;
      uint8_t who = 0xFF;
      Wire.beginTransmission(address);
      Wire.write(IimRegister::WHO_AM_I);
      if (Wire.endTransmission(false) == 0 && Wire.requestFrom(address, uint8_t(1)) == 1)
        who = Wire.read();
      Serial.printf("SENSOR_I2C_DEVICE,address=0x%02X,who=0x%02X\n", address, who);
    }
  }
  if (!found)
    Serial.println("SENSOR_I2C_SCAN,found=0,sda=48,scl=47,cs=34");

  Wire.end();
  pinMode(Board::sensorSck, OUTPUT);
  pinMode(Board::sensorMosi, OUTPUT);
  pinMode(Board::sensorMiso, INPUT);
  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi, Board::sensorCs);
  Serial.printf("SENSOR_I2C_SCAN_DONE,found=%u,spi_restored=1\n", found);
}
