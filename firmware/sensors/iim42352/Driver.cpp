#include "Driver.h"

#include <Wire.h>
#include <string.h>

#include "SelectedBoard.h"
#include "sensor/Registers.h"

namespace {
constexpr uint8_t kWhoAmI = 0x6d;
constexpr uint8_t kPwrAccelModeMask = 0x03;
constexpr uint8_t kPwrAccelModeLowNoise = 0x03;
constexpr uint8_t kPowerModeMask = 0x0f;
constexpr uint8_t kTimestampConfigWanted = 0x29;
constexpr uint8_t kTmstConfig = 0x54;
constexpr uint8_t kBitTmstEnable = 0x01;
constexpr uint8_t kBitTmstResol = 0x08;
constexpr uint8_t kBitRtcMode = 0x04;
constexpr uint8_t kFifoConfigStopOnFull = 0x80;
// IIM-42352 FIFO_PACKET_EN[1:0] selects a packet format, not gyro enable.
// Packet 2 has six reserved bytes and a timestamp at bytes 14..15 (16 total).
constexpr uint8_t kFifoConfig1Base = 0x64;
constexpr uint8_t kFifoPacketMask = 0x03;
constexpr uint8_t kFifoPacketAccel = 0x01;
constexpr uint8_t kFifoPacketTimed = 0x03;
constexpr uint8_t kIntfConfig0Mask = 0x70;
constexpr uint8_t kIntfConfig0RecordBig = 0x50;
constexpr uint8_t kIntConfigWanted = 0x03;
constexpr uint8_t kIntSource0Fifo = 0x04;

constexpr uint8_t kFifoHeaderMsg = 0x80;
constexpr uint8_t kFifoHeaderFormatMask = 0x70;
constexpr uint8_t kFifoHeaderPacket1 = 0x40;
constexpr uint8_t kFifoHeaderPacket2 = 0x60;
constexpr uint8_t kFifoHeaderPacket3 = 0x70;
constexpr uint8_t kFifoHeaderTmst = 0x08;
constexpr uint8_t kFifoHeaderFsync = 0x04;

uint8_t fsSelFor(uint8_t range) {
  switch (range) {
    case 2:
      return 0x03;
    case 8:
      return 0x01;
    case 16:
      return 0x00;
    default:
      return 0x02;
  }
}

uint8_t odrCodeFor(uint16_t rate) {
  return rate == 100 ? 0x08 : 0x09;
}
}  // namespace

Iim42352::Iim42352() : spi_(HSPI) {}

void Iim42352::writeRegister(uint8_t address, uint8_t value) {
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

bool Iim42352::readRegisters(uint8_t address, uint8_t* data, size_t count) {
  if (data == nullptr || count == 0) {
    return false;
  }
  spi_.beginTransaction(SPISettings(spiHz_, MSBFIRST, spiMode_));
  digitalWrite(Board::sensorCs, LOW);
  delayMicroseconds(2);
  spi_.transfer(address | 0x80);
  for (size_t i = 0; i < count; ++i) {
    data[i] = spi_.transfer(0);
  }
  delayMicroseconds(1);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.endTransaction();
  delayMicroseconds(20);
  return true;
}

uint8_t Iim42352::readRegisterRaw(uint8_t address) {
  uint8_t value = 0;
  readRegisters(address, &value, 1);
  return value;
}

bool Iim42352::setBank(uint8_t bank) {
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    writeRegister(IimRegister::REG_BANK_SEL, bank);
    delayMicroseconds(10);
    const uint8_t applied = readRegisterRaw(IimRegister::REG_BANK_SEL);
    if ((applied & 0x07) == (bank & 0x07)) {
      return true;
    }
    delayMicroseconds(10);
  }
  Serial.printf("SENSOR_BANK_ERROR,requested=%u,ok=0\n", bank);
  return false;
}

bool Iim42352::writeVerified(uint8_t address, uint8_t value, uint8_t mask,
                             const char* stage) {
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    writeRegister(address, value);
    delayMicroseconds(250);
    const uint8_t first = readRegisterRaw(address);
    const uint8_t second = readRegisterRaw(address);
    const bool ok = (first & mask) == (value & mask) &&
                    (second & mask) == (value & mask);
    Serial.printf("SENSOR_WRITE,stage=%s,addr=0x%02X,expected=0x%02X,"
                  "mask=0x%02X,read1=0x%02X,read2=0x%02X,attempt=%u,ok=%u\n",
                  stage, address, value, mask, first, second, attempt, ok ? 1 : 0);
    if (ok) {
      return true;
    }
  }
  return false;
}

bool Iim42352::stableIdentity(uint8_t& observedIdentity, uint8_t attempts) {
  uint8_t consecutive = 0;
  for (uint8_t i = 0; i < attempts; ++i) {
    observedIdentity = readRegisterRaw(IimRegister::WHO_AM_I);
    consecutive = observedIdentity == kWhoAmI ? consecutive + 1 : 0;
    if (consecutive >= 3) {
      return true;
    }
    delay(2);
  }
  return false;
}

bool Iim42352::selectTransport(uint8_t& observedIdentity) {
  for (uint8_t mode : {uint8_t(SPI_MODE0), uint8_t(SPI_MODE3)}) {
    for (uint32_t hz : {1000000UL, 100000UL}) {
      spiMode_ = mode;
      spiHz_ = hz;
      writeRegister(IimRegister::REG_BANK_SEL, 0);
      const bool stable = stableIdentity(observedIdentity);
      Serial.printf("SENSOR_PROBE,mode=%u,hz=%lu,identity=0x%02X,stable=%u\n",
                    mode, (unsigned long)hz, observedIdentity, stable ? 1 : 0);
      if (stable) {
        return true;
      }
    }
  }
  return false;
}

bool Iim42352::resetDeviceAndWait() {
  writeRegister(IimRegister::DEVICE_CONFIG, 0x01);
  const uint32_t started = millis();
  uint8_t status = 0;
  bool resetDone = false;
  while (millis() - started < 100UL) {
    delay(2);
    setBank(0);
    if (readRegisters(IimRegister::INT_STATUS, &status, 1) &&
        (status & 0x10) != 0) {
      resetDone = true;
      break;
    }
  }
  uint8_t who = 0;
  bool identityOk = false;
  for (uint8_t i = 0; i < 20 && !identityOk; ++i) {
    delay(2);
    setBank(0);
    who = readRegisterRaw(IimRegister::WHO_AM_I);
    identityOk = who == kWhoAmI;
  }
  Serial.printf("SENSOR_RESET,identity=0x%02X,reset_done=%u,ok=%u\n", who,
                resetDone ? 1 : 0, (resetDone && identityOk) ? 1 : 0);
  return resetDone && identityOk;
}

bool Iim42352::configureInterface() {
  uint8_t before = 0;
  if (!setBank(0) || !readRegisters(IimRegister::INTF_CONFIG0, &before, 1)) {
    return false;
  }
  const uint8_t wanted =
      static_cast<uint8_t>((before & ~kIntfConfig0Mask) | kIntfConfig0RecordBig);
  const bool ok = writeVerified(IimRegister::INTF_CONFIG0, wanted,
                                kIntfConfig0Mask, "interface_config");
  Serial.printf("SENSOR_INTERFACE,intf0_before=0x%02X,intf0_after=0x%02X,"
                "record_count=1,data_big_endian=1,ok=%u\n",
                before, readRegisterRaw(IimRegister::INTF_CONFIG0), ok ? 1 : 0);
  return ok;
}

bool Iim42352::configureClock(bool externalClock) {
  bool pinOk = true;
  if (externalClock) {
    if (!setBank(1)) {
      return false;
    }
    const uint8_t pad = readRegisterRaw(IimRegister::INTF_CONFIG5);
    pinOk = writeVerified(IimRegister::INTF_CONFIG5,
                          static_cast<uint8_t>((pad & ~0x06) | 0x04), 0x06,
                          "clkin_function");
    if (!setBank(0)) {
      return false;
    }
  }
  uint8_t current = 0;
  if (!setBank(0) || !readRegisters(IimRegister::INTF_CONFIG1, &current, 1)) {
    return false;
  }
  // CLKSEL=00 selects the RC oscillator; 01/10 are reserved on IIM-42352.
  const uint8_t bits = externalClock ? 0x04 : 0x00;
  const bool clockOk = writeVerified(
      IimRegister::INTF_CONFIG1, static_cast<uint8_t>((current & ~0x07) | bits),
      0x07, "clock_source");
  const uint8_t applied = readRegisterRaw(IimRegister::INTF_CONFIG1);
  const bool rtcMode = (applied & kBitRtcMode) != 0;
  const bool ok = pinOk && clockOk && (externalClock ? rtcMode : !rtcMode);
  Serial.printf("SENSOR_CLOCK,requested=%u,intf1=0x%02X,rtc_mode=%u,"
                "clkin_pin=%u,ok=%u\n",
                externalClock ? 1 : 0, applied, rtcMode ? 1 : 0,
                externalClock ? 1 : 0, ok ? 1 : 0);
  return ok;
}

bool Iim42352::configureTimestamp() {
  timestampRtcMode_ = false;
  if (!setBank(0)) {
    timestampSupported_ = false;
    return false;
  }
  uint8_t intf1 = 0;
  if (setBank(0) && readRegisters(IimRegister::INTF_CONFIG1, &intf1, 1)) {
    timestampRtcMode_ = (intf1 & kBitRtcMode) != 0;
  }
  uint8_t applied = readRegisterRaw(kTmstConfig);
  bool ok = false;
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    writeRegister(kTmstConfig, kTimestampConfigWanted);
    delay(1);
    applied = readRegisterRaw(kTmstConfig);
    if (applied == kTimestampConfigWanted) {
      ok = true;
      break;
    }
    delay(1);
  }
  timestampConfig_ = applied;
  timestampSupported_ = ok && (applied & kBitTmstEnable) != 0;
  Serial.printf("SENSOR_TIMESTAMP,config=0x%02X,rtc_mode=%u,enabled=%u,"
                "resolution_bit=%u,resolution_us_x1000=%lu,supported=%u,ok=%u\n",
                applied, timestampRtcMode_ ? 1 : 0,
                (applied & kBitTmstEnable) != 0 ? 1 : 0,
                (applied & kBitTmstResol) != 0 ? 1 : 0,
                (unsigned long)(1000000000UL / timestampTicksPerSecond()),
                timestampSupported_ ? 1 : 0, ok ? 1 : 0);
  return ok;
}

bool Iim42352::configureFifo() {
  if (!setBank(0)) {
    return false;
  }
  const uint8_t fifo1 = static_cast<uint8_t>(
      kFifoConfig1Base | (timestampSupported_ ? kFifoPacketTimed : kFifoPacketAccel));
  const bool modeOk =
      writeVerified(IimRegister::FIFO_CONFIG, kFifoConfigStopOnFull, 0xc0,
                    "fifo_mode");
  const bool featuresOk =
      writeVerified(IimRegister::FIFO_CONFIG1, fifo1, 0x7f, "fifo_features");
  const bool wmLowOk =
      writeVerified(IimRegister::FIFO_CONFIG2, 1, 0xff, "fifo_wm_low");
  const bool wmHighOk =
      writeVerified(IimRegister::FIFO_CONFIG3, 0, 0xff, "fifo_wm_high");
  const bool ok = modeOk && featuresOk && wmLowOk && wmHighOk;
  Serial.printf("SENSOR_FIFO_CONFIG,fifo=0x%02X,fifo1=0x%02X,fifo2=0x%02X,"
                "fifo3=0x%02X,timestamp=%u,ok=%u\n",
                readRegisterRaw(IimRegister::FIFO_CONFIG),
                readRegisterRaw(IimRegister::FIFO_CONFIG1),
                readRegisterRaw(IimRegister::FIFO_CONFIG2),
                readRegisterRaw(IimRegister::FIFO_CONFIG3),
                timestampSupported_ ? 1 : 0, ok ? 1 : 0);
  return ok;
}

bool Iim42352::forceSixteenBitFifo() {
  uint8_t fifo1 = 0;
  if (!setBank(0) || !readRegisters(IimRegister::FIFO_CONFIG1, &fifo1, 1)) {
    return false;
  }
  const uint8_t wanted = static_cast<uint8_t>((fifo1 | 0x40) & ~0x10);
  const bool ok =
      writeVerified(IimRegister::FIFO_CONFIG1, wanted, 0x50, "fifo_16bit");
  Serial.printf("SENSOR_FIFO_CONFIG1,applied=0x%02X,hires=0,resume=1,ok=%u\n",
                readRegisterRaw(IimRegister::FIFO_CONFIG1), ok ? 1 : 0);
  return ok;
}

bool Iim42352::configureInterrupt() {
  if (!setBank(0)) {
    return false;
  }
  uint8_t intCfg1 = 0;
  bool asyncOk = false;
  if (setBank(0) && readRegisters(IimRegister::INT_CONFIG1, &intCfg1, 1)) {
    asyncOk = writeVerified(IimRegister::INT_CONFIG1,
                            static_cast<uint8_t>(intCfg1 & ~0x10), 0x10,
                            "int_async_reset");
  }
  const bool intOk = writeVerified(IimRegister::INT_CONFIG, kIntConfigWanted,
                                   0x03, "int_config");
  const bool sourceOk = writeVerified(IimRegister::INT_SOURCE0,
                                      kIntSource0Fifo, 0x0c, "int_source");
  Serial.printf("SENSOR_INT_CONFIG,int_cfg=0x%02X,int_src=0x%02X,"
                "async_ok=%u,ok=%u\n",
                readRegisterRaw(IimRegister::INT_CONFIG),
                readRegisterRaw(IimRegister::INT_SOURCE0), asyncOk ? 1 : 0,
                (asyncOk && intOk && sourceOk) ? 1 : 0);
  return asyncOk && intOk && sourceOk;
}

bool Iim42352::applyAccelConfig(uint16_t rate, uint8_t range) {
  if (!setBank(0)) {
    return false;
  }
  const uint8_t value =
      static_cast<uint8_t>((fsSelFor(range) << 5) | odrCodeFor(rate));
  return writeVerified(IimRegister::ACCEL_CONFIG0, value, 0xff, "accel");
}

bool Iim42352::configure(uint16_t rate, uint8_t range) {
  if (!initialized_ || (rate != 50 && rate != 100) ||
      (range != 2 && range != 4 && range != 8 && range != 16)) {
    return false;
  }
  if (running_) {
    standby();
  }

  bool ok = applyAccelConfig(rate, range);
  (void)configureTimestamp();
  ok = configureFifo() && ok;
  ok = forceSixteenBitFifo() && ok;
  ok = configureInterrupt() && ok;

  uint8_t power = 0;
  const bool powerRead =
      setBank(0) && readRegisters(IimRegister::PWR_MGMT0, &power, 1);
  const bool powerOff =
      powerRead && writeVerified(IimRegister::PWR_MGMT0,
                                 static_cast<uint8_t>(power & ~kPowerModeMask),
                                 kPowerModeMask, "power_off");
  ok = powerOff && ok;

  if (ok) {
    rate_ = rate;
    range_ = range;
    clearQueue();
    lastFifoBytes_ = 0;
    lastHeader_ = 0;
    overflow_ = false;
    fifoBatchBytes_ = 0;
    fifoParseOffset_ = 0;
    if (setBank(0)) {
      writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
    }
    updateFifoPacketSize();
  }

  const uint8_t accel = readRegisterRaw(IimRegister::ACCEL_CONFIG0);
  const uint8_t fifo = readRegisterRaw(IimRegister::FIFO_CONFIG);
  const uint8_t fifo1 = readRegisterRaw(IimRegister::FIFO_CONFIG1);
  const bool odrOk = (accel & 0x0f) == odrCodeFor(rate);
  const bool fsrOk = (accel >> 5) == fsSelFor(range);
  const bool fifoModeOk = (fifo & 0xc0) == kFifoConfigStopOnFull;
  const bool hiresOk = (fifo1 & 0x10) == 0;
  ok = ok && odrOk && fsrOk && fifoModeOk && hiresOk;
  Serial.printf("SENSOR_CONFIGURE,rate=%u,range=%u,power=0x%02X,accel=0x%02X,"
                "fifo=0x%02X,fifo1=0x%02X,odr_ok=%u,fsr_ok=%u,fifo_mode_ok=%u,"
                "hires_ok=%u,packet_bytes=%u,ok=%u\n",
                rate, range, readRegisterRaw(IimRegister::PWR_MGMT0), accel,
                fifo, fifo1, odrOk ? 1 : 0, fsrOk ? 1 : 0, fifoModeOk ? 1 : 0,
                hiresOk ? 1 : 0, packetBytes_, ok ? 1 : 0);
  return ok;
}

void Iim42352::start() {
  if (!initialized_) {
    return;
  }
  clearQueue();
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  fifoBatchBytes_ = 0;
  fifoParseOffset_ = 0;
  if (!setBank(0)) {
    running_ = false;
    Serial.println("SENSOR_START,running=0,ok=0,reason=bank");
    return;
  }
  writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
  const bool ok = writeVerified(IimRegister::PWR_MGMT0,
                                kPwrAccelModeLowNoise, kPowerModeMask,
                                "accel_start");
  delayMicroseconds(250);
  running_ = ok;
  if (running_ && setBank(0)) {
    writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
    updateFifoPacketSize();
  }
  const uint8_t power = readRegisterRaw(IimRegister::PWR_MGMT0);
  Serial.printf("SENSOR_START,rate=%u,range=%u,running=%u,power=0x%02X,"
                "accel_low_noise=%u,packet_bytes=%u\n",
                rate_, range_, running_ ? 1 : 0, power,
                (power & kPwrAccelModeMask) == kPwrAccelModeLowNoise ? 1 : 0,
                packetBytes_);
}

void Iim42352::standby() {
  if (!initialized_) {
    return;
  }
  const bool ok =
      setBank(0) &&
      writeVerified(IimRegister::PWR_MGMT0, 0, kPowerModeMask,
                    "accel_stop");
  running_ = false;
  clearQueue();
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  fifoBatchBytes_ = 0;
  fifoParseOffset_ = 0;
  if (setBank(0)) {
    writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
  }
  Serial.printf("SENSOR_STANDBY,ok=%u\n", ok ? 1 : 0);
}

void Iim42352::flush() {
  clearQueue();
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  fifoBatchBytes_ = 0;
  fifoParseOffset_ = 0;
  if (initialized_ && setBank(0)) {
    writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
  }
}

void Iim42352::clearQueue() {
  head_ = 0;
  tail_ = 0;
  count_ = 0;
}

bool Iim42352::begin(bool resetDevice, bool externalClock) {
  initialized_ = false;
  running_ = false;
  clearQueue();
  packetBytes_ = 16;
  lastFifoBytes_ = 0;
  lastFifoCountRaw_ = 0;
  lastFifoCountAttempts_ = 0;
  lastFifoCountOk_ = true;
  lastFifoReadOk_ = true;
  fifoConsecutiveFailures_ = 0;
  lastFifoIdentity_ = 0;
  fifoRecoveryTotal_ = 0;
  fifoErrorTotal_ = 0;
  lastFifoErrorReason_ = "none";
  lastHeader_ = 0;
  lastFifoTimestamp_ = 0;
  lastFifoTimestampValid_ = false;
  timestampConfig_ = 0;
  timestampRtcMode_ = false;
  timestampSupported_ = false;
  overflow_ = false;
  fifoBatchBytes_ = 0;
  fifoParseOffset_ = 0;

  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.end();
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
             Board::sensorCs);
  delay(5);

  uint8_t observedIdentity = 0xff;
  if (!selectTransport(observedIdentity)) {
    Serial.printf("SENSOR_ID,name=%s,expected=0x%02X,actual=0x%02X,ok=0\n",
                  name(), expectedIdentity(), observedIdentity);
    Serial.println("SENSOR_ERROR,stage=identity");
    return false;
  }
  Serial.printf("SENSOR_ID,name=%s,expected=0x%02X,actual=0x%02X,ok=1\n",
                name(), expectedIdentity(), observedIdentity);

  if (resetDevice) {
    if (!resetDeviceAndWait()) {
      Serial.println("SENSOR_ERROR,stage=identity_after_reset");
      return false;
    }
  } else {
    Serial.printf("SENSOR_RESET,skipped=1,identity=0x%02X\n",
                  observedIdentity);
  }

  if (!setBank(0) ||
      !writeVerified(IimRegister::PWR_MGMT0, 0, kPowerModeMask,
                     "stop_before_config")) {
    Serial.println("SENSOR_ERROR,stage=stop_before_config");
    return false;
  }
  delayMicroseconds(250);

  if (!configureInterface()) {
    Serial.println("SENSOR_ERROR,stage=spi_interface_readback");
    return false;
  }

  initialized_ = true;
  flush();
  if (!configureClock(externalClock)) {
    Serial.println("SENSOR_ERROR,stage=clock");
    initialized_ = false;
    return false;
  }

  bool configured = false;
  for (uint8_t attempt = 1; attempt <= 3 && !configured; ++attempt) {
    configured = configure(50, 4);
    if (!configured && attempt < 3) {
      delay(10);
    }
  }
  if (!configured) {
    Serial.println("SENSOR_ERROR,stage=configure");
    initialized_ = false;
    return false;
  }
  return true;
}

void Iim42352::updateFifoPacketSize() {
  uint8_t fifo1 = 0;
  if (!setBank(0) ||
      !readRegisters(IimRegister::FIFO_CONFIG1, &fifo1, 1)) {
    return;
  }
  switch (fifo1 & kFifoPacketMask) {
    case kFifoPacketAccel:
      packetBytes_ = 8;
      break;
    case kFifoPacketTimed:
      packetBytes_ = (fifo1 & 0x10) != 0 ? 20 : 16;
      break;
    default:
      packetBytes_ = 0;
      break;
  }
}

uint16_t Iim42352::maxFifoPackets() const {
  const uint16_t size = packetBytes_ == 0 ? 1 : packetBytes_;
  return static_cast<uint16_t>(kFifoCapacityBytes / size);
}

uint16_t Iim42352::available() {
  if (!initialized_ || !running_) {
    return count_;
  }

  uint8_t countBytes[2] = {};
  uint16_t packets = 0;
  bool countOk = false;
  lastFifoCountAttempts_ = 0;
  for (uint8_t attempt = 1; attempt <= 3 && !countOk; ++attempt) {
    lastFifoCountAttempts_ = attempt;
    if (setBank(0) &&
        readRegisters(IimRegister::FIFO_COUNTH, countBytes,
                      sizeof(countBytes))) {
      packets = static_cast<uint16_t>(countBytes[0]) |
                (static_cast<uint16_t>(countBytes[1]) << 8);
      countOk = packets <= maxFifoPackets();
    }
    if (!countOk) {
      delay(1);
    }
  }
  lastFifoCountRaw_ = packets;
  lastFifoCountOk_ = countOk;

  if (!countOk) {
    ++fifoConsecutiveFailures_;
    ++fifoErrorTotal_;
    lastFifoReadOk_ = false;
    lastFifoErrorReason_ = "fifo_count_invalid";
    lastFifoBytes_ = 0;
    clearQueue();
    const bool recovered =
        fifoConsecutiveFailures_ < 3 && recoverFifoState();
    overflow_ = fifoConsecutiveFailures_ >= 3;
    Serial.printf("SENSOR_FIFO_ERROR,index=0,count_raw=%u,attempts=%u,"
                  "buffered=0,header=0x%02X,packet_bytes=%u,consecutive=%u,"
                  "total=%u,recoveries=%u,reason=%s,recovered=%u,fatal=%u,"
                  "batch_bytes=%u\n",
                  lastFifoCountRaw_, lastFifoCountAttempts_, lastHeader_,
                  packetBytes_, fifoConsecutiveFailures_, fifoErrorTotal_,
                  fifoRecoveryTotal_, lastFifoErrorReason(),
                  recovered ? 1 : 0, overflow_ ? 1 : 0, fifoBatchBytes_);
    return count_;
  }

  lastFifoBytes_ = static_cast<uint16_t>(packets * packetBytes_);
  if (packets == 0) {
    fifoBatchBytes_ = 0;
    fifoParseOffset_ = 0;
    lastFifoReadOk_ = true;
    if (!overflow_) {
      fifoConsecutiveFailures_ = 0;
      lastFifoErrorReason_ = "none";
    }
    return count_;
  }

  uint32_t bytes = static_cast<uint32_t>(packets) * packetBytes_;
  if (bytes > kFifoBatchCapacityBytes) {
    bytes = kFifoBatchCapacityBytes;
  }
  fifoBatchBytes_ = static_cast<uint16_t>(bytes);
  fifoParseOffset_ = 0;
  if (!setBank(0) ||
      !readRegisters(IimRegister::FIFO_DATA, fifoBatch_, fifoBatchBytes_)) {
    fifoBatchBytes_ = 0;
    ++fifoConsecutiveFailures_;
    ++fifoErrorTotal_;
    lastFifoReadOk_ = false;
    lastFifoErrorReason_ = "fifo_batch_read_failed";
    const bool recovered =
        fifoConsecutiveFailures_ < 3 && recoverFifoState();
    overflow_ = fifoConsecutiveFailures_ >= 3;
    Serial.printf("SENSOR_FIFO_ERROR,index=0,count_raw=%u,attempts=%u,"
                  "buffered=%u,header=0x%02X,packet_bytes=%u,consecutive=%u,"
                  "total=%u,recoveries=%u,reason=%s,recovered=%u,fatal=%u,"
                  "batch_bytes=%u\n",
                  lastFifoCountRaw_, lastFifoCountAttempts_, count_, lastHeader_,
                  packetBytes_, fifoConsecutiveFailures_, fifoErrorTotal_,
                  fifoRecoveryTotal_, lastFifoErrorReason(),
                  recovered ? 1 : 0, overflow_ ? 1 : 0, fifoBatchBytes_);
    return count_;
  }

  for (uint16_t i = 0; i < packets; ++i) {
    if (!readFifoPacket()) {
      ++fifoConsecutiveFailures_;
      ++fifoErrorTotal_;
      lastFifoReadOk_ = false;
      const bool recovered =
          fifoConsecutiveFailures_ < 3 && recoverFifoState();
      overflow_ = fifoConsecutiveFailures_ >= 3;
      Serial.printf("SENSOR_FIFO_ERROR,index=%u,count_raw=%u,attempts=%u,"
                    "buffered=%u,header=0x%02X,packet_bytes=%u,consecutive=%u,"
                    "total=%u,recoveries=%u,reason=%s,recovered=%u,fatal=%u,"
                    "batch_bytes=%u\n",
                    i, lastFifoCountRaw_, lastFifoCountAttempts_, count_,
                    lastHeader_, packetBytes_, fifoConsecutiveFailures_,
                    fifoErrorTotal_, fifoRecoveryTotal_, lastFifoErrorReason(),
                    recovered ? 1 : 0, overflow_ ? 1 : 0, fifoBatchBytes_);
      Serial.printf("FIFO_RECOVER,reason=%s,available=%u,count_raw=%u,"
                    "attempts=%u,consecutive=%u,total=%u,header=0x%02X\n",
                    lastFifoErrorReason(), count_, lastFifoCountRaw_,
                    lastFifoCountAttempts_, fifoConsecutiveFailures_,
                    fifoErrorTotal_, lastHeader_);
      return count_;
    }
  }
  fifoConsecutiveFailures_ = 0;
  lastFifoReadOk_ = true;
  if (!overflow_) {
    lastFifoErrorReason_ = "none";
  }
  return count_;
}
uint16_t Iim42352::fifoPacketBytesForHeader(uint8_t header) {
  switch (header & kFifoHeaderFormatMask) {
    case kFifoHeaderPacket1:
      // Packet 1 never has a timestamp, even if FIFO_TMST_FSYNC_EN is set.
      return (header & (kFifoHeaderTmst | kFifoHeaderFsync)) == 0 ? 8 : 0;
    case kFifoHeaderPacket2:
      return 16;
    default:
      return 0;
  }
}

bool Iim42352::readFifoPacket() {
  if (fifoParseOffset_ >= fifoBatchBytes_) {
    lastFifoErrorReason_ = "fifo_batch_offset_exhausted";
    return false;
  }
  uint8_t header = fifoBatch_[fifoParseOffset_++];
  lastHeader_ = header;
  if ((header & kFifoHeaderMsg) != 0) {
    lastFifoErrorReason_ = "fifo_message_header";
    return false;
  }
  const uint16_t packetBytes = fifoPacketBytesForHeader(header);
  if (packetBytes == 0 || packetBytes != packetBytes_) {
    lastFifoErrorReason_ =
        (header & kFifoHeaderFormatMask) == kFifoHeaderPacket3
            ? "unexpected_20bit_header" : "unexpected_packet_format";
    return false;
  }
  const uint16_t bytes = packetBytes - kFifoHeaderBytes;
  if (static_cast<uint32_t>(fifoParseOffset_) + bytes > fifoBatchBytes_) {
    lastFifoErrorReason_ = "fifo_batch_packet_truncated";
    return false;
  }
  memcpy(fifoPayload_, &fifoBatch_[fifoParseOffset_], bytes);
  fifoParseOffset_ = static_cast<uint16_t>(fifoParseOffset_ + bytes);

  lastFifoTimestamp_ = 0;
  lastFifoTimestampValid_ = false;
  if (packetBytes == 16) {
    // Payload excludes the header: reserved bytes occupy offsets 6..11,
    // temperature is at 12, and the timestamp is at 13..14.
    lastFifoTimestamp_ =
        static_cast<uint16_t>((fifoPayload_[13] << 8) | fifoPayload_[14]);
    lastFifoTimestampValid_ =
        (header & (kFifoHeaderTmst | kFifoHeaderFsync)) == kFifoHeaderTmst;
  }

  auto decode = [](const uint8_t* p) -> int16_t {
    return static_cast<int16_t>(static_cast<uint16_t>((p[0] << 8) | p[1]));
  };
  const AccelSample sample = {
      decode(&fifoPayload_[0]), decode(&fifoPayload_[2]),
      decode(&fifoPayload_[4])};
  if (sample.x == INT16_MIN || sample.y == INT16_MIN ||
      sample.z == INT16_MIN) {
    return true;
  }
  if (count_ >= kSampleCapacity) {
    overflow_ = true;
    lastFifoErrorReason_ = "sample_queue_overflow";
    return true;
  }
  samples_[tail_] = {sample, lastFifoTimestamp_, lastHeader_, packetBytes_,
                     lastFifoTimestampValid_};
  tail_ = static_cast<uint16_t>((tail_ + 1) % kSampleCapacity);
  ++count_;
  return true;
}

bool Iim42352::read(AccelSample& sample) {
  FifoSample item = {};
  if (!read(item)) {
    return false;
  }
  sample = item.accel;
  return true;
}

bool Iim42352::read(AccelSample& sample, uint16_t& fifoTimestamp, bool& timed) {
  FifoSample item = {};
  if (!read(item)) {
    return false;
  }
  sample = item.accel;
  fifoTimestamp = item.timestamp;
  timed = item.timed;
  return true;
}

bool Iim42352::read(FifoSample& sample) {
  if (count_ == 0) {
    return false;
  }
  sample = samples_[head_];
  head_ = static_cast<uint16_t>((head_ + 1) % kSampleCapacity);
  --count_;
  return true;
}

bool Iim42352::readDirect(AccelSample& sample) {
  if (!initialized_) {
    return false;
  }
  uint8_t who = 0;
  uint8_t status = 0;
  uint8_t raw[6] = {};
  if (!setBank(0) ||
      !readRegisters(IimRegister::WHO_AM_I, &who, 1) || who != kWhoAmI ||
      !readRegisters(IimRegister::INT_STATUS, &status, 1) ||
      (status & 0x0e) == 0 ||
      !readRegisters(IimRegister::ACCEL_DATA_X1, raw, sizeof(raw))) {
    return false;
  }
  auto decode = [](const uint8_t* p) -> int16_t {
    return static_cast<int16_t>(static_cast<uint16_t>((p[0] << 8) | p[1]));
  };
  const AccelSample observed = {
      decode(&raw[0]), decode(&raw[2]), decode(&raw[4])};
  if (observed.x == INT16_MIN || observed.y == INT16_MIN ||
      observed.z == INT16_MIN) {
    return false;
  }
  sample = observed;
  return true;
}

bool Iim42352::fifoCountValid() const {
  return !overflow_ && lastFifoBytes_ <= kFifoCapacityBytes;
}

bool Iim42352::recoverFifoState() {
  ++fifoRecoveryTotal_;
  uint8_t who = 0;
  if (!setBank(0) || !readRegisters(IimRegister::WHO_AM_I, &who, 1) ||
      who != kWhoAmI) {
    lastFifoIdentity_ = who;
    lastFifoErrorReason_ = "recovery_identity_failed";
    return false;
  }
  lastFifoIdentity_ = who;

  if (setBank(0)) {
    writeRegister(IimRegister::SIGNAL_PATH_RESET, 0x02);
  }
  delay(2);
  (void)configureTimestamp();
  if (!configureFifo() || !forceSixteenBitFifo()) {
    lastFifoErrorReason_ = "recovery_configure_failed";
    return false;
  }

  uint8_t power = 0;
  if (!setBank(0) ||
      !readRegisters(IimRegister::PWR_MGMT0, &power, 1)) {
    lastFifoErrorReason_ = "recovery_power_read_failed";
    return false;
  }
  if (running_ && (power & kPwrAccelModeMask) != kPwrAccelModeLowNoise) {
    if (!writeVerified(IimRegister::PWR_MGMT0, kPwrAccelModeLowNoise,
                       kPowerModeMask, "recovery_accel_start")) {
      lastFifoErrorReason_ = "recovery_accel_start_failed";
      return false;
    }
  }
  updateFifoPacketSize();
  delay(5);

  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    uint8_t countBytes[2] = {};
    uint8_t checkWho = 0;
    if (setBank(0) &&
        readRegisters(IimRegister::WHO_AM_I, &checkWho, 1) &&
        checkWho == kWhoAmI &&
        readRegisters(IimRegister::FIFO_COUNTH, countBytes,
                      sizeof(countBytes))) {
      const uint16_t packets = static_cast<uint16_t>(countBytes[0]) |
                               (static_cast<uint16_t>(countBytes[1]) << 8);
      lastFifoCountRaw_ = packets;
      if (packets <= maxFifoPackets()) {
        lastFifoBytes_ = 0;
        overflow_ = false;
        lastFifoReadOk_ = false;
        lastFifoErrorReason_ = "recovered";
        fifoBatchBytes_ = 0;
        fifoParseOffset_ = 0;
        Serial.printf("SENSOR_FIFO_RESYNC,who=0x%02X,packet_bytes=%u,"
                      "count_raw=%u,attempt=%u,ok=1\n",
                      checkWho, packetBytes_, packets, attempt);
        return true;
      }
      lastFifoErrorReason_ = "recovery_count_invalid";
    } else {
      lastFifoErrorReason_ = "recovery_count_read_failed";
    }
    delay(1);
  }
  Serial.printf("SENSOR_FIFO_RESYNC,who=0x%02X,packet_bytes=%u,ok=0\n",
                lastFifoIdentity_, packetBytes_);
  return false;
}

uint8_t Iim42352::readRegister(uint8_t address) {
  if (!initialized_) {
    return 0;
  }
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    uint8_t value = 0;
    if (setBank(0) && readRegisters(address, &value, 1)) {
      if (address != IimRegister::WHO_AM_I || value == kWhoAmI) {
        return value;
      }
    }
    delayMicroseconds(20);
  }
  return 0;
}

uint32_t Iim42352::timestampTicksPerSecond() const {
  // DS-000436 section 12.7: nominal timestamp intervals must be scaled by
  // 32.768/30 with CLKIN, or by 32/30 with the internal oscillator.
  const uint32_t resolution = (timestampConfig_ & kBitTmstResol) != 0 ? 16 : 1;
  const uint32_t scale = timestampRtcMode_ ? 32768U : 32000U;
  const uint32_t divisor = resolution * scale;
  return static_cast<uint32_t>((30000000000ULL + divisor / 2U) / divisor);
}

uint32_t Iim42352::timestampUs(uint16_t ticks) const {
  const uint32_t resolution = (timestampConfig_ & kBitTmstResol) != 0 ? 16 : 1;
  const uint32_t scale = timestampRtcMode_ ? 32768U : 32000U;
  return static_cast<uint32_t>(
      (static_cast<uint64_t>(ticks) * resolution * scale + 15000ULL) / 30000ULL);
}

void Iim42352::diagnostic() {
  if (!initialized_) {
    Serial.println("SENSOR_DIAG,initialized=0");
    return;
  }
  uint8_t who = 0;
  uint8_t accel = 0;
  uint8_t power = 0;
  uint8_t fifo = 0;
  uint8_t fifo1 = 0;
  uint8_t fifo2 = 0;
  uint8_t fifo3 = 0;
  uint8_t intf0 = 0;
  uint8_t intf1 = 0;
  uint8_t intSource = 0;
  uint8_t intConfig = 0;
  uint8_t status = 0;
  setBank(0);
  readRegisters(IimRegister::WHO_AM_I, &who, 1);
  readRegisters(IimRegister::ACCEL_CONFIG0, &accel, 1);
  readRegisters(IimRegister::PWR_MGMT0, &power, 1);
  readRegisters(IimRegister::FIFO_CONFIG, &fifo, 1);
  readRegisters(IimRegister::FIFO_CONFIG1, &fifo1, 1);
  readRegisters(IimRegister::FIFO_CONFIG2, &fifo2, 1);
  readRegisters(IimRegister::FIFO_CONFIG3, &fifo3, 1);
  readRegisters(IimRegister::INTF_CONFIG0, &intf0, 1);
  readRegisters(IimRegister::INTF_CONFIG1, &intf1, 1);
  readRegisters(IimRegister::INT_SOURCE0, &intSource, 1);
  readRegisters(IimRegister::INT_CONFIG, &intConfig, 1);
  readRegisters(IimRegister::INT_STATUS, &status, 1);
  uint8_t intf5 = 0;
  if (setBank(1)) {
    readRegisters(IimRegister::INTF_CONFIG5, &intf5, 1);
  }
  setBank(0);

  Serial.printf("SENSOR_DIAG,who=0x%02X,accel=0x%02X,power=0x%02X,"
                "fifo_cfg=0x%02X,fifo1=0x%02X,fifo2=0x%02X,fifo3=0x%02X,"
                "intf0=0x%02X,intf1=0x%02X,intf5=0x%02X,int_src=0x%02X,"
                "int_cfg=0x%02X,int_status=0x%02X,fifo_bytes=%u,"
                "packet_bytes=%u,header=0x%02X,timestamp_supported=%u\n",
                who, accel, power, fifo, fifo1, fifo2, fifo3, intf0, intf1,
                intf5, intSource, intConfig, status, lastFifoBytes_,
                packetBytes_, lastHeader_, timestampSupported_ ? 1 : 0);
}

void Iim42352::linkTest() {
  const uint8_t hardwareBefore = setBank(0) ? readRegisterRaw(0x75) : 0;
  digitalWrite(Board::sensorCs, HIGH);
  spi_.end();

  struct Wiring {
    const char* label;
    int sck;
    int miso;
    int mosi;
    int cs;
  };
  const Wiring wirings[] = {
      {"standard", Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
       Board::sensorCs},
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

    // Weak pulls distinguish an undriven MISO from a line held low/high.
    // Never drive the confirmed sensor MISO as an output.
    pinMode(wiring.miso, INPUT_PULLUP);
    delayMicroseconds(20);
    const int misoPullUp = digitalRead(wiring.miso);
    pinMode(wiring.miso, INPUT_PULLDOWN);
    delayMicroseconds(20);
    const int misoPullDown = digitalRead(wiring.miso);
    pinMode(wiring.miso, INPUT);
    Serial.printf("SENSOR_MISO_IDLE,cs_high=%d,pull_up=%d,pull_down=%d\n",
                  digitalRead(wiring.cs), misoPullUp, misoPullDown);

    for (uint8_t mode : {uint8_t(SPI_MODE0), uint8_t(SPI_MODE3)}) {
      const bool idleHigh = mode == SPI_MODE3;
      for (uint8_t halfPeriodUs : {uint8_t(5), uint8_t(20)}) {
        auto transfer = [&](uint8_t output) {
          uint8_t input = 0;
          for (uint8_t mask = 0x80; mask; mask >>= 1) {
            if (idleHigh) {
              digitalWrite(wiring.sck, LOW);
            }
            digitalWrite(wiring.mosi, output & mask ? HIGH : LOW);
            delayMicroseconds(halfPeriodUs);
            digitalWrite(wiring.sck, HIGH);
            delayMicroseconds(halfPeriodUs);
            input = static_cast<uint8_t>((input << 1) |
                                         (digitalRead(wiring.miso) ? 1 : 0));
            if (!idleHigh) {
              digitalWrite(wiring.sck, LOW);
            }
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
          matched += last == kWhoAmI;
          Serial.printf(
              "SENSOR_SOFT_SPI,wiring=%s,mode=%u,half_period_us=%u,"
              "attempt=%u,who=0x%02X\n",
              wiring.label, mode, halfPeriodUs, attempt + 1, last);
        }
        Serial.printf("SENSOR_SOFT_RESULT,wiring=%s,mode=%u,"
                      "half_period_us=%u,matches=%u,total=4\n",
                      wiring.label, mode, halfPeriodUs, matched);
      }
    }
  }

  digitalWrite(Board::sensorCs, HIGH);
  pinMode(Board::sensorCs, OUTPUT);
  pinMode(Board::sensorSck, OUTPUT);
  pinMode(Board::sensorMosi, OUTPUT);
  pinMode(Board::sensorMiso, INPUT);
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
             Board::sensorCs);
  setBank(0);
  const uint8_t hardwareAfter = readRegisterRaw(0x75);
  Serial.printf("SENSOR_LINK_TEST,hardware_before=0x%02X,"
                "hardware_after=0x%02X,spi_restored=1,sensor_reset=0\n",
                hardwareBefore, hardwareAfter);
}

void Iim42352::i2cScan() {
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
      if (Wire.endTransmission(false) == 0 &&
          Wire.requestFrom(address, uint8_t(1)) == 1) {
        who = static_cast<uint8_t>(Wire.read());
      }
      Serial.printf("SENSOR_I2C_DEVICE,address=0x%02X,who=0x%02X\n",
                    address, who);
    }
  }
  if (!found) {
    Serial.printf("SENSOR_I2C_SCAN,found=0,sda=%d,scl=%d,cs=%d\n",
                  Board::sensorMosi, Board::sensorSck, Board::sensorCs);
  }

  Wire.end();
  pinMode(Board::sensorSck, OUTPUT);
  pinMode(Board::sensorMosi, OUTPUT);
  pinMode(Board::sensorMiso, INPUT);
  pinMode(Board::sensorCs, OUTPUT);
  digitalWrite(Board::sensorCs, HIGH);
  spi_.begin(Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
             Board::sensorCs);
  Serial.printf("SENSOR_I2C_SCAN_DONE,found=%u,spi_restored=1\n", found);
}