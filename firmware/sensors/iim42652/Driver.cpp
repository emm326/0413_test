#include "Driver.h"

#include <esp_timer.h>

#include "SelectedBoard.h"

namespace {
constexpr uint8_t kWhoAmI = 0x6f;
// PWR_MGMT0 ACCEL_MODE occupies bits [1:0]: 0=off, 1=low power, 2=low noise.
constexpr uint8_t kPwrAccelModeMask = 0x03;
constexpr uint8_t kPwrAccelModeLowNoise = 0x02;

IXM42XXX_ACCEL_CONFIG0_ODR_t odrFor(uint16_t rate) {
  return rate == 100 ? IXM42XXX_ACCEL_CONFIG0_ODR_100_HZ
                     : IXM42XXX_ACCEL_CONFIG0_ODR_50_HZ;
}

IXM42XXX_ACCEL_CONFIG0_FS_SEL_t fsrFor(uint8_t range) {
  switch (range) {
    case 2:
      return IXM42XXX_ACCEL_CONFIG0_FS_SEL_2g;
    case 8:
      return IXM42XXX_ACCEL_CONFIG0_FS_SEL_8g;
    case 16:
      return IXM42XXX_ACCEL_CONFIG0_FS_SEL_16g;
    default:
      return IXM42XXX_ACCEL_CONFIG0_FS_SEL_4g;
  }
}
}  // namespace

// Hooks required by the unmodified InvenSense high-level driver.
extern "C" void inv_ixm42xxx_sleep_us(uint32_t us) {
  if (us >= 1000U) {
    delay(us / 1000U);
    us %= 1000U;
  }
  if (us > 0U) {
    delayMicroseconds(us);
  }
}

extern "C" uint64_t inv_ixm42xxx_get_time_us(void) {
  return static_cast<uint64_t>(esp_timer_get_time());
}

Iim42652* Iim42652::active_ = nullptr;

Iim42652::Iim42652()
    : transport_(Board::sensorSck, Board::sensorMiso, Board::sensorMosi,
                 Board::sensorCs) {}

bool Iim42652::begin(bool resetDevice, bool externalClock) {
  active_ = this;
  initialized_ = false;
  running_ = false;
  head_ = tail_ = count_ = 0;
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  packetBytes_ = 11;

  if (!transport_.begin()) {
    Serial.println("SENSOR_VENDOR_INIT,rc=-1,stage=transport");
    return false;
  }

  serif_ = {};
  serif_.context = &transport_;
  serif_.read_reg = &SensorSpiTransport::readThunk;
  serif_.write_reg = &SensorSpiTransport::writeThunk;
  serif_.configure = nullptr;
  serif_.max_read = IXM42XXX_FIFO_MIRRORING_SIZE;
  serif_.max_write = 16;
  serif_.serif_type = IXM42XXX_UI_SPI4;

  const int rc = inv_ixm42xxx_init(&driver_, &serif_, &Iim42652::eventThunk);
  Serial.printf(
      "SENSOR_VENDOR_INIT,rc=%d,reset_requested=%u,soft_reset=1,"
      "external_clock=%u\n",
      rc, resetDevice ? 1 : 0, externalClock ? 1 : 0);
  if (rc != INV_ERROR_SUCCESS) {
    transport_.end();
    return false;
  }

  uint8_t who = 0;
  for (uint8_t attempt = 0; attempt < 3; ++attempt) {
    if (!setBank(0) || !directRead(0x75, who)) {
      who = 0;
      break;
    }
    if (who == kWhoAmI) {
      break;
    }
    delay(2);
  }
  Serial.printf("SENSOR_ID,name=%s,expected=0x%02X,observed=0x%02X,ok=%u\n",
                name(), expectedIdentity(), who, who == kWhoAmI ? 1 : 0);
  if (who != kWhoAmI) {
    transport_.end();
    return false;
  }

  initialized_ = true;
  if (!configureClock(externalClock)) {
    initialized_ = false;
    transport_.end();
    return false;
  }
  if (!configure(50, 4)) {
    initialized_ = false;
    transport_.end();
    return false;
  }
  return true;
}

bool Iim42652::configureClock(bool enabled) {
  const int rc = inv_ixm42xxx_enable_clkin_rtc(&driver_, enabled ? 1 : 0);
  if (rc != INV_ERROR_SUCCESS) {
    Serial.printf("SENSOR_CLOCK_SET,enabled=%u,rc=%d\n", enabled ? 1 : 0, rc);
    return false;
  }
  return verifyClock(enabled);
}

bool Iim42652::verifyClock(bool enabled) {
  uint8_t intf1 = 0;
  uint8_t intf5 = 0;
  const bool readOk = setBank(0) && directRead(MPUREG_INTF_CONFIG1, intf1);
  const bool readPad = setBank(1) && directRead(MPUREG_INTF_CONFIG5_B1, intf5);
  setBank(0);

  const bool rtcMode = (intf1 & BIT_RTC_MODE_MASK) != 0;
  const uint8_t pad = (intf5 & BIT_INTF_CONFIG5_GPIO_PAD_SEL_MASK) >>
                      BIT_INTF_CONFIG5_GPIO_PAD_SEL_POS;
  const bool clkinPin = pad == 2;
  const bool ok = readOk && readPad &&
                  (enabled ? (rtcMode && clkinPin) : !rtcMode);
  Serial.printf("SENSOR_CLOCK,requested=%u,intf1=0x%02X,intf5=0x%02X,"
                "rtc_mode=%u,clkin_pin=%u,physical_read=%u,ok=%u\n",
                enabled ? 1 : 0, intf1, intf5, rtcMode ? 1 : 0,
                clkinPin ? 1 : 0, (readOk && readPad) ? 1 : 0, ok ? 1 : 0);
  return ok;
}

bool Iim42652::configure(uint16_t rate, uint8_t range) {
  if (!initialized_ || (rate != 50 && rate != 100) ||
      (range != 2 && range != 4 && range != 8 && range != 16)) {
    return false;
  }
  if (running_) {
    standby();
  }

  int rc = inv_ixm42xxx_set_accel_frequency(&driver_, odrFor(rate));
  rc |= inv_ixm42xxx_set_accel_fsr(&driver_, fsrFor(range));
  rc |= inv_ixm42xxx_configure_fifo(&driver_, INV_IXM42XXX_FIFO_ENABLED);
  rc |= inv_ixm42xxx_configure_fifo_wm(&driver_, 1);
  rc |= configureInterrupt() ? 0 : INV_ERROR;
  if (rc != INV_ERROR_SUCCESS) {
    Serial.printf("SENSOR_CONFIGURE,rate=%u,range=%u,rc=%d,ok=0\n", rate, range, rc);
    return false;
  }

  rate_ = rate;
  range_ = range;
  overflow_ = false;
  head_ = tail_ = count_ = 0;
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  if (setBank(0)) {
    inv_ixm42xxx_reset_fifo(&driver_);
  }
  updateFifoPacketSize();

  uint8_t power = 0;
  uint8_t accel = 0;
  uint8_t fifo = 0;
  uint8_t fifo1 = 0;
  const bool readback = setBank(0) && directRead(MPUREG_PWR_MGMT_0, power) &&
                        directRead(MPUREG_ACCEL_CONFIG0, accel) &&
                        directRead(MPUREG_FIFO_CONFIG, fifo) &&
                        directRead(MPUREG_FIFO_CONFIG1, fifo1);
  const bool odrOk = (accel & BIT_ACCEL_CONFIG0_ODR_MASK) ==
                     static_cast<uint8_t>(odrFor(rate));
  const bool fsrOk = (accel & BIT_ACCEL_CONFIG0_FS_SEL_MASK) ==
                     static_cast<uint8_t>(fsrFor(range));
  const bool fifoOk = (fifo & BIT_FIFO_CONFIG_MODE_MASK) ==
                      static_cast<uint8_t>(IXM42XXX_FIFO_CONFIG_MODE_STOP_ON_FULL);
  const bool ok = readback && odrOk && fsrOk && fifoOk;
  Serial.printf("SENSOR_CONFIGURE,rate=%u,range=%u,power=0x%02X,accel=0x%02X,"
                "fifo=0x%02X,fifo1=0x%02X,readback=%u,odr_ok=%u,fsr_ok=%u,"
                "fifo_mode_ok=%u,packet_bytes=%u,ok=%u\n",
                rate_, range_, power, accel, fifo, fifo1, readback ? 1 : 0,
                odrOk ? 1 : 0, fsrOk ? 1 : 0, fifoOk ? 1 : 0, packetBytes_,
                ok ? 1 : 0);
  return ok;
}

bool Iim42652::configureInterrupt() {
  inv_ixm42xxx_interrupt_parameter_t config = {};
  config.INV_IXM42XXX_FIFO_THS = INV_IXM42XXX_ENABLE;
  const int rc = inv_ixm42xxx_set_config_int1(&driver_, &config);
  if (rc != INV_ERROR_SUCCESS) {
    return false;
  }

  uint8_t intConfig = 0;
  if (!setBank(0) || !readRaw(MPUREG_INT_CONFIG, intConfig)) {
    return false;
  }
  intConfig &= static_cast<uint8_t>(
      ~(BIT_INT_CONFIG_INT1_DRIVE_CIRCUIT_MASK | BIT_INT_CONFIG_INT1_POLARITY_MASK));
  intConfig |= static_cast<uint8_t>(IXM42XXX_INT_CONFIG_INT1_DRIVE_CIRCUIT_PP);
  intConfig |= static_cast<uint8_t>(IXM42XXX_INT_CONFIG_INT1_POLARITY_HIGH);
  return writeRaw(MPUREG_INT_CONFIG, intConfig);
}

bool Iim42652::setBank(uint8_t bank) {
  return inv_ixm42xxx_set_reg_bank(&driver_, bank) == INV_ERROR_SUCCESS;
}

bool Iim42652::readRaw(uint8_t address, uint8_t& value) {
  return readRaw(address, &value, 1);
}

bool Iim42652::readRaw(uint8_t address, uint8_t* buffer, uint32_t length) {
  return buffer != nullptr && length > 0 &&
         inv_ixm42xxx_read_reg(&driver_, address, length, buffer) ==
             INV_ERROR_SUCCESS;
}

bool Iim42652::writeRaw(uint8_t address, uint8_t value) {
  return inv_ixm42xxx_write_reg(&driver_, address, 1, &value) ==
         INV_ERROR_SUCCESS;
}

bool Iim42652::directRead(uint8_t address, uint8_t& value) {
  return directRead(address, &value, 1);
}

bool Iim42652::directRead(uint8_t address, uint8_t* buffer, uint32_t length) {
  return buffer != nullptr && length > 0 &&
         transport_.read(address, buffer, length) == 0;
}

void Iim42652::updateFifoPacketSize() {
  uint8_t fifo1 = 0;
  if (!setBank(0) || !directRead(MPUREG_FIFO_CONFIG1, fifo1)) {
    return;
  }
  const bool accel = (fifo1 & BIT_FIFO_CONFIG1_ACCEL_MASK) != 0;
  const bool gyro = (fifo1 & BIT_FIFO_CONFIG1_GYRO_MASK) != 0;
  const bool timed = (fifo1 & BIT_FIFO_CONFIG1_TMST_FSYNC_MASK) != 0;
  uint16_t size = kFifoHeaderBytes;
  if (accel) {
    size += kFifoAxisBytes;
  }
  if (gyro) {
    size += kFifoAxisBytes;
  }
  if (accel || gyro) {
    size += kFifoTempBytes;
  }
  if (timed) {
    size += kFifoTimeBytes;
  }
  packetBytes_ = size;
}

uint16_t Iim42652::maxFifoPackets() const {
  const uint16_t size = packetBytes_ == 0 ? 1 : packetBytes_;
  return static_cast<uint16_t>(kFifoCapacityBytes / size);
}

uint8_t Iim42652::readRegister(uint8_t address) {
  if (!initialized_) {
    return 0;
  }
  uint8_t value = 0;
  if (!setBank(0) || !directRead(address, value)) {
    return 0;
  }
  return value;
}

void Iim42652::start() {
  if (!initialized_) {
    return;
  }
  head_ = tail_ = count_ = 0;
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  if (setBank(0)) {
    inv_ixm42xxx_reset_fifo(&driver_);
  }
  const int rc = inv_ixm42xxx_enable_accel_low_noise_mode(&driver_);
  running_ = rc == INV_ERROR_SUCCESS;
  if (running_) {
    inv_ixm42xxx_reset_fifo(&driver_);
    // Enabling the accelerometer also enables accel and gyro in FIFO_CONFIG1,
    // so the packet size must be refreshed after the enable call.
    updateFifoPacketSize();
  }
  uint8_t power = 0;
  const bool powerRead = setBank(0) && directRead(MPUREG_PWR_MGMT_0, power);
  const bool lowNoise =
      powerRead && (power & kPwrAccelModeMask) == kPwrAccelModeLowNoise;
  Serial.printf("SENSOR_START,rate=%u,range=%u,rc=%d,running=%u,power=0x%02X,"
                "accel_low_noise=%u,packet_bytes=%u\n",
                rate_, range_, rc, running_ ? 1 : 0, power,
                lowNoise ? 1 : 0, packetBytes_);
}

void Iim42652::standby() {
  if (!initialized_) {
    return;
  }
  const int rc = inv_ixm42xxx_disable_accel(&driver_);
  running_ = false;
  head_ = tail_ = count_ = 0;
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  if (setBank(0)) {
    inv_ixm42xxx_reset_fifo(&driver_);
  }
  Serial.printf("SENSOR_STANDBY,rc=%d\n", rc);
}

void Iim42652::flush() {
  head_ = tail_ = count_ = 0;
  lastFifoBytes_ = 0;
  lastHeader_ = 0;
  overflow_ = false;
  if (initialized_ && setBank(0)) {
    inv_ixm42xxx_reset_fifo(&driver_);
  }
}

uint16_t Iim42652::available() {
  if (!initialized_ || !running_) {
    return count_;
  }

  uint8_t countBytes[2] = {};
  if (!setBank(0) || !readRaw(MPUREG_FIFO_COUNTH, countBytes, 2)) {
    overflow_ = true;
    lastFifoBytes_ = 0xffff;
    return count_;
  }
  const uint16_t packets = static_cast<uint16_t>(countBytes[0]) |
                           (static_cast<uint16_t>(countBytes[1]) << 8);
  if (packets > maxFifoPackets()) {
    overflow_ = true;
    lastFifoBytes_ = 0xffff;
    head_ = tail_ = count_ = 0;
    inv_ixm42xxx_reset_fifo(&driver_);
    return 0;
  }

  lastFifoBytes_ = static_cast<uint16_t>(packets * packetBytes_);
  if (packets == 0) {
    return count_;
  }

  const int rc = inv_ixm42xxx_get_data_from_fifo(&driver_);
  if (rc != INV_ERROR_SUCCESS) {
    overflow_ = true;
    lastFifoBytes_ = 0xffff;
    return count_;
  }
  lastHeader_ = driver_.fifo_data[0];
  return count_;
}

bool Iim42652::read(AccelSample& sample) {
  if (count_ == 0) {
    return false;
  }
  sample = samples_[head_];
  head_ = static_cast<uint16_t>((head_ + 1) % kSampleCapacity);
  --count_;
  return true;
}

bool Iim42652::readDirect(AccelSample& sample) {
  if (!initialized_) {
    return false;
  }
  uint8_t who = 0;
  uint8_t status = 0;
  uint8_t raw[6] = {};
  if (!setBank(0) || !directRead(MPUREG_WHO_AM_I, who) || who != kWhoAmI ||
      !directRead(MPUREG_INT_STATUS, status) ||
      !(status & (BIT_INT_STATUS_DRDY | BIT_INT_STATUS_FIFO_THS |
                  BIT_INT_STATUS_FIFO_FULL)) ||
      !directRead(MPUREG_ACCEL_DATA_X1_UI, raw, sizeof(raw))) {
    return false;
  }

  const bool bigEndian =
      driver_.endianess_data == IXM42XXX_INTF_CONFIG0_DATA_BIG_ENDIAN;
  auto decode = [bigEndian](const uint8_t* p) -> int16_t {
    const uint16_t value = bigEndian
                               ? static_cast<uint16_t>((p[0] << 8) | p[1])
                               : static_cast<uint16_t>((p[1] << 8) | p[0]);
    return static_cast<int16_t>(value);
  };
  AccelSample observed = {decode(&raw[0]), decode(&raw[2]), decode(&raw[4])};
  if (observed.x == INT16_MIN || observed.y == INT16_MIN ||
      observed.z == INT16_MIN) {
    return false;
  }
  sample = observed;
  return true;
}

bool Iim42652::fifoCountValid() const {
  return !overflow_ && lastFifoBytes_ <= kFifoCapacityBytes;
}

void Iim42652::onEvent(const inv_ixm42xxx_sensor_event_t& event) {
  if ((event.sensor_mask & (1 << INV_IXM42XXX_SENSOR_ACCEL)) == 0) {
    return;
  }
  if (count_ >= kSampleCapacity) {
    overflow_ = true;
    return;
  }
  samples_[tail_] = {event.accel[0], event.accel[1], event.accel[2]};
  tail_ = static_cast<uint16_t>((tail_ + 1) % kSampleCapacity);
  ++count_;
}

void Iim42652::eventThunk(inv_ixm42xxx_sensor_event_t* event) {
  if (active_ != nullptr && event != nullptr) {
    active_->onEvent(*event);
  }
}

void Iim42652::diagnostic() {
  if (!initialized_) {
    Serial.println("SENSOR_DIAG,initialized=0");
    return;
  }
  setBank(0);
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
  uint8_t status = 0;
  directRead(MPUREG_WHO_AM_I, who);
  directRead(MPUREG_ACCEL_CONFIG0, accel);
  directRead(MPUREG_PWR_MGMT_0, power);
  directRead(MPUREG_FIFO_CONFIG, fifo);
  directRead(MPUREG_FIFO_CONFIG1, fifo1);
  directRead(MPUREG_FIFO_CONFIG2, fifo2);
  directRead(MPUREG_FIFO_CONFIG3, fifo3);
  directRead(MPUREG_INTF_CONFIG0, intf0);
  directRead(MPUREG_INTF_CONFIG1, intf1);
  directRead(MPUREG_INT_SOURCE0, intSource);
  directRead(MPUREG_INT_STATUS, status);
  setBank(1);
  uint8_t intf5 = 0;
  directRead(MPUREG_INTF_CONFIG5_B1, intf5);
  setBank(0);

  Serial.printf("SENSOR_DIAG,who=0x%02X,accel=0x%02X,power=0x%02X,fifo_cfg=0x%02X,"
                "fifo1=0x%02X,fifo2=0x%02X,fifo3=0x%02X,intf0=0x%02X,"
                "intf1=0x%02X,intf5=0x%02X,int_src=0x%02X,int_status=0x%02X,"
                "fifo_bytes=%u,packet_bytes=%u,header=0x%02X\n",
                who, accel, power, fifo, fifo1, fifo2, fifo3, intf0, intf1,
                intf5, intSource, status, lastFifoBytes_, packetBytes_,
                lastHeader_);
}

void Iim42652::linkTest() {
  const uint8_t before = readRegister(0x75);
  transport_.end();
  const bool rebound = transport_.begin();
  const uint8_t after = rebound ? readRegister(0x75) : 0;
  Serial.printf("SENSOR_LINK_TEST,hardware_before=0x%02X,hardware_after=0x%02X,"
                "spi_restored=%u,sensor_reset=0\n",
                before, after, rebound ? 1 : 0);
}
