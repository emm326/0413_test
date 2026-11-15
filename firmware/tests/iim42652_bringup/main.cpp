#include <Arduino.h>
#include <cmath>
#include <esp_timer.h>

#include "boards/eora_s3_400/Board.h"
#include "Ixm42xxxDefs.h"
#include "Ixm42xxxDriver_HL.h"
#include "Ixm42xxxTransport.h"
#include "SensorSpiTransport.h"

namespace {

constexpr uint8_t kExpectedWhoAmI = 0x6F;
constexpr float kAccelLsbPerG = 8192.0f;  // DS-000440: +/-4 g = 8192 LSB/g.
constexpr uint32_t kSampleWindowUs = 5000000;
constexpr uint32_t kPollPeriodUs = 1000;

struct SampleStats {
  uint32_t count = 0;
  int16_t minX = 0;
  int16_t maxX = 0;
  int16_t minY = 0;
  int16_t maxY = 0;
  int16_t minZ = 0;
  int16_t maxZ = 0;
  int64_t sumX = 0;
  int64_t sumY = 0;
  int64_t sumZ = 0;
  double sumMagnitudeG = 0.0;
};

SensorSpiTransport transport(Board::sensorSck, Board::sensorMiso,
                             Board::sensorMosi, Board::sensorCs);
inv_ixm42xxx driver;
SampleStats stats;
int64_t samplingStartUs = 0;
bool samplingActive = false;

void printResult(const char* step, int rc) {
  Serial.printf("DRIVER_STEP,%s,rc=%d,%s\n", step, rc,
                rc == INV_ERROR_SUCCESS ? "OK" : "FAIL");
}

uint8_t readPhysicalRegister(uint8_t reg) {
  uint8_t value = 0xFF;
  const int rc = transport.read(reg, &value, 1);
  if (rc != 0) {
    return 0xFF;
  }
  return value;
}

void updateStats(int16_t x, int16_t y, int16_t z) {
  if (stats.count == 0) {
    stats.minX = stats.maxX = x;
    stats.minY = stats.maxY = y;
    stats.minZ = stats.maxZ = z;
  } else {
    stats.minX = min(stats.minX, x);
    stats.maxX = max(stats.maxX, x);
    stats.minY = min(stats.minY, y);
    stats.maxY = max(stats.maxY, y);
    stats.minZ = min(stats.minZ, z);
    stats.maxZ = max(stats.maxZ, z);
  }
  stats.sumX += x;
  stats.sumY += y;
  stats.sumZ += z;
  const double magnitude = std::sqrt(
      static_cast<double>(x) * x + static_cast<double>(y) * y +
      static_cast<double>(z) * z);
  stats.sumMagnitudeG += magnitude / kAccelLsbPerG;
  ++stats.count;
}

void handleSensorEvent(inv_ixm42xxx_sensor_event_t* event) {
  if (!samplingActive || event == nullptr) {
    return;
  }

  const int16_t x = event->accel[0];
  const int16_t y = event->accel[1];
  const int16_t z = event->accel[2];
  updateStats(x, y, z);

  if (stats.count <= 10) {
    const uint32_t dtUs =
        static_cast<uint32_t>(esp_timer_get_time() - samplingStartUs);
    const double magnitudeG =
        std::sqrt(static_cast<double>(x) * x + static_cast<double>(y) * y +
                  static_cast<double>(z) * z) /
        kAccelLsbPerG;
    Serial.printf(
        "SAMPLE,index=%lu,dt_us=%lu,x=%d,y=%d,z=%d,temp_raw=%d,"
        "magnitude_g=%.4f\n",
        static_cast<unsigned long>(stats.count),
        static_cast<unsigned long>(dtUs), x, y, z, event->temperature,
        magnitudeG);
  }
}

bool configureSensor() {
  int rc = inv_ixm42xxx_enable_clkin_rtc(&driver, false);
  printResult("disable_clkin", rc);
  if (rc != INV_ERROR_SUCCESS) {
    return false;
  }

  rc = inv_ixm42xxx_set_accel_fsr(
      &driver, IXM42XXX_ACCEL_CONFIG0_FS_SEL_4g);
  printResult("set_accel_fsr_4g", rc);
  if (rc != INV_ERROR_SUCCESS) {
    return false;
  }

  rc = inv_ixm42xxx_set_accel_frequency(
      &driver, IXM42XXX_ACCEL_CONFIG0_ODR_50_HZ);
  printResult("set_accel_odr_50hz", rc);
  if (rc != INV_ERROR_SUCCESS) {
    return false;
  }

  rc = inv_ixm42xxx_enable_accel_low_noise_mode(&driver);
  printResult("enable_accel_low_noise", rc);
  if (rc != INV_ERROR_SUCCESS) {
    return false;
  }

  delay(IXM42XXX_ACC_STARTUP_TIME_US / 1000U);
  return true;
}

void runSampling() {
  stats = SampleStats();
  samplingStartUs = esp_timer_get_time();
  samplingActive = true;

  uint32_t transportErrors = 0;
  uint32_t nextPollUs = 0;
  while (esp_timer_get_time() - samplingStartUs <
         static_cast<int64_t>(kSampleWindowUs)) {
    const int rc = inv_ixm42xxx_get_data_from_registers(&driver);
    if (rc != INV_ERROR_SUCCESS) {
      ++transportErrors;
    }

    nextPollUs += kPollPeriodUs;
    const int64_t elapsedUs = esp_timer_get_time() - samplingStartUs;
    if (nextPollUs > static_cast<uint32_t>(elapsedUs)) {
      delayMicroseconds(nextPollUs - static_cast<uint32_t>(elapsedUs));
    }
  }

  samplingActive = false;
  const uint32_t elapsedUs =
      static_cast<uint32_t>(esp_timer_get_time() - samplingStartUs);
  const double sampleRateHz =
      elapsedUs == 0 ? 0.0
                     : static_cast<double>(stats.count) * 1000000.0 / elapsedUs;

  const double meanMagG =
      stats.count == 0 ? 0.0 : stats.sumMagnitudeG / stats.count;
  const bool rateOk = sampleRateHz >= 40.0 && sampleRateHz <= 60.0;
  const bool countOk = stats.count >= 180;
  const bool transportOk = transportErrors == 0;
  const bool signalPlausible = meanMagG > 0.5 && meanMagG < 1.5;

  Serial.printf(
      "SAMPLING,samples=%lu,elapsed_us=%lu,rate_hz=%.3f,spi_errors=%lu,"
      "mean_x_raw=%.1f,mean_y_raw=%.1f,mean_z_raw=%.1f,mean_g=%.4f,"
      "min_x=%d,max_x=%d,min_y=%d,max_y=%d,min_z=%d,max_z=%d\n",
      static_cast<unsigned long>(stats.count),
      static_cast<unsigned long>(elapsedUs), sampleRateHz,
      static_cast<unsigned long>(transportErrors),
      stats.count == 0 ? 0.0 : static_cast<double>(stats.sumX) / stats.count,
      stats.count == 0 ? 0.0 : static_cast<double>(stats.sumY) / stats.count,
      stats.count == 0 ? 0.0 : static_cast<double>(stats.sumZ) / stats.count,
      meanMagG, stats.minX, stats.maxX, stats.minY, stats.maxY, stats.minZ,
      stats.maxZ);
  Serial.printf(
      "SAMPLING_CHECK,rate_ok=%u,count_ok=%u,transport_ok=%u,"
      "signal_plausible=%u\n",
      rateOk ? 1 : 0, countOk ? 1 : 0, transportOk ? 1 : 0,
      signalPlausible ? 1 : 0);

  const int disableRc = inv_ixm42xxx_disable_accel(&driver);
  printResult("disable_accel", disableRc);
  delay(5);
  const uint8_t power = readPhysicalRegister(MPUREG_PWR_MGMT_0);
  const bool accelOff = (power & BIT_PWR_MGMT_0_ACCEL_MODE_MASK) ==
                        IXM42XXX_PWR_MGMT_0_ACCEL_MODE_OFF;
  Serial.printf("SAFE_STOP,pwr_mgmt0=0x%02X,accel_off=%u\n", power,
                accelOff ? 1 : 0);

  const bool passed = rateOk && countOk && transportOk && accelOff &&
                      disableRc == INV_ERROR_SUCCESS;
  Serial.printf(
      "BRINGUP_RESULT,result=%s,samples=%lu,rate_hz=%.3f,mean_g=%.4f\n",
      passed ? "PASS" : "FAIL", static_cast<unsigned long>(stats.count),
      sampleRateHz, meanMagG);
}

}  // namespace

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

bool preflightTransport() {
  const SensorSpiTransport::Config candidates[] = {
      {"hspi_mode0_100k", SPI_MODE0, 100000, false, false},
      {"hspi_mode0_2m", SPI_MODE0, 2000000, false, false},
      {"hspi_mode3_100k", SPI_MODE3, 100000, false, false},
      {"hspi_mode3_2m", SPI_MODE3, 2000000, false, false},
      {"fspi_mode0_100k", SPI_MODE0, 100000, false, true},
      {"fspi_mode0_2m", SPI_MODE0, 2000000, false, true},
      {"gpio_mode0_100k", SPI_MODE0, 100000, true, false},
  };

  for (const auto& candidate : candidates) {
    if (!transport.begin(candidate)) {
      Serial.printf("PREFLIGHT,config=%s,begin=FAIL\n", candidate.name);
      continue;
    }

    uint8_t reads[3] = {0, 0, 0};
    const int rc0 = transport.read(MPUREG_WHO_AM_I, &reads[0], 1);
    delay(2);
    const int rc1 = transport.read(MPUREG_WHO_AM_I, &reads[1], 1);
    delay(2);
    const int rc2 = transport.read(MPUREG_WHO_AM_I, &reads[2], 1);

    const bool stable = rc0 == 0 && rc1 == 0 && rc2 == 0 &&
                        reads[0] == kExpectedWhoAmI &&
                        reads[1] == kExpectedWhoAmI &&
                        reads[2] == kExpectedWhoAmI;
    Serial.printf(
        "PREFLIGHT,config=%s,who1=0x%02X,who2=0x%02X,who3=0x%02X,"
        "stable=%u\n",
        candidate.name, reads[0], reads[1], reads[2], stable ? 1 : 0);

    if (stable) {
      Serial.printf("PREFLIGHT_SELECT,config=%s\n", candidate.name);
      return true;
    }
    transport.end();
  }

  return false;
}

void setup() {
  Serial.begin(460800);
  for (int secondsLeft = 5; secondsLeft > 0; --secondsLeft) {
    Serial.printf("BOOT_WAIT,%d\n", secondsLeft);
    delay(1000);
  }

  Serial.println();
  Serial.println("IIM42652_6WIRE_BRINGUP");
  Serial.println("WIRING,vcc=3.3V,gnd=GND,sck=GPIO47,mosi=GPIO48,"
                 "miso=GPIO39,csb=GPIO40");
  Serial.println("SCOPE,accel_only,fsr=4g,odr=50Hz,fifo=off,clkin=off");

  pinMode(Board::sensorMiso, INPUT);
  Serial.printf("PIN_IDLE,miso=GPIO%d,level=%d\n", Board::sensorMiso,
                digitalRead(Board::sensorMiso));

  if (!preflightTransport()) {
    Serial.println(
        "PREFLIGHT_RESULT,result=FAIL,reason=no_stable_who_am_i_6f");
    Serial.println("BRINGUP_RESULT,result=FAIL,stage=preflight");
    return;
  }

  inv_ixm42xxx_serif serif{};
  serif.context = &transport;
  serif.read_reg = SensorSpiTransport::readThunk;
  serif.write_reg = SensorSpiTransport::writeThunk;
  serif.configure = nullptr;
  serif.max_read = 64;
  serif.max_write = 64;
  serif.serif_type = IXM42XXX_UI_SPI4;

  int rc = inv_ixm42xxx_init(&driver, &serif, handleSensorEvent);
  printResult("vendor_init", rc);
  if (rc != INV_ERROR_SUCCESS) {
    Serial.printf("BRINGUP_RESULT,result=FAIL,stage=init,rc=%d\n", rc);
    return;
  }

  rc = inv_ixm42xxx_configure_fifo(&driver, INV_IXM42XXX_FIFO_DISABLED);
  printResult("configure_fifo_disabled", rc);
  if (rc != INV_ERROR_SUCCESS) {
    Serial.printf("BRINGUP_RESULT,result=FAIL,stage=fifo,rc=%d\n", rc);
    return;
  }

  uint8_t whoAmI = 0;
  rc = inv_ixm42xxx_get_who_am_i(&driver, &whoAmI);
  printResult("get_who_am_i", rc);
  const bool whoOk = rc == INV_ERROR_SUCCESS && whoAmI == kExpectedWhoAmI;
  Serial.printf("WHO_AM_I,value=0x%02X,expected=0x%02X,ok=%u\n", whoAmI,
                kExpectedWhoAmI, whoOk ? 1 : 0);
  if (!whoOk) {
    Serial.println("BRINGUP_RESULT,result=FAIL,stage=who_am_i");
    return;
  }

  if (!configureSensor()) {
    Serial.println("BRINGUP_RESULT,result=FAIL,stage=configure");
    return;
  }

  const uint8_t intf0 = readPhysicalRegister(MPUREG_INTF_CONFIG0);
  const uint8_t power = readPhysicalRegister(MPUREG_PWR_MGMT_0);
  const uint8_t accelCfg = readPhysicalRegister(MPUREG_ACCEL_CONFIG0);
  const uint8_t intStatus = readPhysicalRegister(MPUREG_INT_STATUS);
  Serial.printf(
      "PHYSICAL_CONFIG,intf_config0=0x%02X,pwr_mgmt0=0x%02X,"
      "accel_config0=0x%02X,int_status=0x%02X,drdy=%u\n",
      intf0, power, accelCfg, intStatus,
      (intStatus & BIT_INT_STATUS_DRDY) ? 1 : 0);

  runSampling();
}

void loop() {
  delay(1000);
}