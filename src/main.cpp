#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>
#include <esp_system.h>
#include <stdlib.h>
#include <string.h>

#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

#if defined(APP_SENSOR_IIM42352)
#include "Ds3231Clock.h"
#include "GpsTimebase.h"
#include "IIM42352Driver.h"
using SensorDriver = IIM42352Driver;
#else
#include "ADXL345Driver.h"
using SensorDriver = ADXL345Driver;
#endif
#include "board_pins.h"
#include "config.h"
#include "lora_transport.h"
#if defined(APP_LORAWAN_TX)
#include "lorawan_transport.h"
#endif
#include "protocol.h"

#if !defined(APP_IIM42352_INT1_INPUT_PULLUP_VALUE)
#define APP_IIM42352_INT1_INPUT_PULLUP_VALUE 0
#endif

#if !defined(APP_IIM42352_INT1_INTERRUPT_MODE_CHANGE) && \
    !defined(APP_IIM42352_INT1_INTERRUPT_MODE_FALLING) && \
    !defined(APP_IIM42352_INT1_INTERRUPT_MODE_RISING)
#define APP_IIM42352_INT1_INTERRUPT_MODE_RISING 1
#endif

namespace {

struct RateProfile {
  uint16_t sampleRateX10Hz;
  SensorDriver::DataRate sensorRate;
  bool experimental;
  const char* label;
};

enum class TxUiMode : uint8_t {
  Normal = 0,
  Select,
};

constexpr RateProfile kRateProfiles[] = {
    {500, SensorDriver::DataRate::Hz50, false, "50"},
    {1000, SensorDriver::DataRate::Hz100, false, "100"},
    {2000, SensorDriver::DataRate::Hz200, false, "200"},
    {4000, SensorDriver::DataRate::Hz400, true, "400"},
};
constexpr size_t kRateProfileCount = sizeof(kRateProfiles) / sizeof(kRateProfiles[0]);

TwoWire& sensorWire = Wire;
TwoWire displayWire(1);

#if defined(APP_SENSOR_IIM42352)
SPIClass iimSpi(HSPI);
SensorDriver adxl(iimSpi,
                  PIN_IIM42352_SCK,
                  PIN_IIM42352_MISO,
                  PIN_IIM42352_MOSI,
                  PIN_IIM42352_CS,
                  AppConfig::IIM42352_SPI_CLOCK_HZ);
TwoWire& rtcWire = Wire;
HardwareSerial gpsSerial(1);
Ds3231Clock ds3231(rtcWire,
                   DS3231_I2C_ADDRESS,
                   PIN_DS3231_SDA,
                   PIN_DS3231_SCL,
                   AppConfig::DS3231_I2C_CLOCK_HZ);
GpsTimebase gpsTimebase(gpsSerial);
#else
SensorDriver adxl(sensorWire,
                  ADXL345_I2C_ADDRESS,
                  PIN_ADXL345_SDA,
                  PIN_ADXL345_SCL,
                  AppConfig::ADXL345_I2C_CLOCK_HZ);
#endif

Adafruit_SSD1306 oled(AppConfig::OLED_WIDTH, AppConfig::OLED_HEIGHT, &displayWire, PIN_OLED_RST);

Protocol::AccelRawSample sampleBuffer[AppConfig::MAX_SAMPLES_PER_PACKET] = {};
SensorDriver::RawSample fifoBurstBuffer[AppConfig::SENSOR_FIFO_WATERMARK] = {};
uint8_t txPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint8_t rxPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint8_t controlPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};

struct QueuedTxPacket {
  uint8_t data[Protocol::MAX_PACKET_SIZE];
  size_t length;
  uint16_t seq;
  uint32_t t0Us;
  uint8_t count;
  uint8_t retries;
};

QueuedTxPacket txQueue[AppConfig::TX_QUEUE_PACKET_CAPACITY] = {};
uint8_t txQueueHead = 0;
uint8_t txQueueCount = 0;

uint16_t packetSeq = 0;
uint32_t samplePeriodUs = 10000UL;
uint32_t currentBatchStartUs = 0;
uint8_t pendingSamples = 0;
int16_t lastRxRadioStateLogged = 0;
uint32_t queueOverflowCount = 0;
uint32_t txUsbPackets = 0;
uint32_t txUsbSamples = 0;
uint32_t txUsbFifoReadFail = 0;
uint32_t txUsbInt1Missing = 0;
char txSerialLine[96] = {};
size_t txSerialLineLen = 0;

#if defined(APP_SENSOR_IIM42352)
bool ds3231Ready = false;
bool ds3231_32k_valid = false;
uint32_t lastGpsWaitLogMs = 0;
constexpr uint8_t kIimInt1TimestampCapacity = 64;
volatile uint32_t iimInt1Timestamps[kIimInt1TimestampCapacity] = {};
volatile uint8_t iimInt1TimestampHead = 0;
volatile uint8_t iimInt1TimestampCount = 0;
volatile uint32_t iimInt1TimestampOverflow = 0;
volatile uint32_t iimInt1RawEdges = 0;
volatile uint32_t iimInt1LastEdgeUs = 0;
uint32_t lastIimInt1TimestampUs = 0;
uint32_t lastIimInt1ReportMs = 0;
#endif

bool txRunArmed = false;
bool txCaptureStarted = false;
uint16_t txRunId = 0;
uint64_t txRunStartEpochUs = 0;
uint32_t txRunStartLocalUs = 0;
bool txWaitingAck = false;
uint16_t txWaitingAckSeq = 0;
uint32_t txAckDeadlineMs = 0;

bool rxRunArmed = false;
bool rxRunStarted = false;
uint16_t rxRunId = 0;
uint64_t rxRunStartEpochUs = 0;
uint32_t rxRunStartLocalUs = 0;
uint16_t rxRunFsHzX10 = AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
uint32_t rxLastStartBroadcastMs = 0;
uint8_t rxPollIndex = 0;
bool rxPollWaiting = false;
uint16_t rxPollTargetDevId = 0;
uint32_t rxPollDeadlineMs = 0;
uint8_t rxPollBurstPackets = 0;
uint16_t rxLastSeq[AppConfig::P2P_NODE_COUNT] = {};
bool rxLastSeqValid[AppConfig::P2P_NODE_COUNT] = {};

char rxSerialLine[96] = {};
size_t rxSerialLineLen = 0;

size_t activeRateIndex = 0;
size_t candidateRateIndex = 0;
TxUiMode txUiMode = TxUiMode::Normal;

bool oledReady = false;
uint8_t oledAddress = 0;
bool displayDirty = true;
uint32_t lastDisplayRefreshMs = 0;
uint32_t lastUiInteractionMs = 0;

bool buttonRawPressed = false;
bool buttonStablePressed = false;
uint32_t buttonRawChangedMs = 0;
uint32_t buttonPressedAtMs = 0;
bool buttonLongPressHandled = false;

uint32_t overloadDisplayUntilMs = 0;
uint32_t lastOverloadLogMs = 0;
uint32_t overloadEventCount = 0;

constexpr uint32_t kBootScreenMs = 1500;

#if defined(APP_SENSOR_IIM42352)
bool txTimebaseReady();
#endif

float sampleRateHzFloat(uint16_t sampleRateX10Hz) { return sampleRateX10Hz / 10.0f; }

uint32_t actualSampleRateX1000Hz(uint16_t sampleRateX10Hz) {
#if defined(APP_USE_DS3231_32K_TIMESTAMPS)
  switch (sampleRateX10Hz) {
    case 500:
      return 51200UL;
    case 1000:
      return 102400UL;
    case 2000:
      return 204800UL;
    case 4000:
      return 409600UL;
    default:
      break;
  }
#endif
  return static_cast<uint32_t>(sampleRateX10Hz) * 100UL;
}

float actualSampleRateHzFloat(uint16_t sampleRateX10Hz) {
  return actualSampleRateX1000Hz(sampleRateX10Hz) / 1000.0f;
}

uint32_t samplePeriodUsForRate(uint16_t sampleRateX10Hz) {
  const uint32_t rateX1000 = actualSampleRateX1000Hz(sampleRateX10Hz);
  return static_cast<uint32_t>((1000000000ULL + rateX1000 / 2ULL) / rateX1000);
}

uint32_t sampleIntervalMsRounded(uint8_t sampleIndex, uint16_t sampleRateX10Hz) {
  const uint32_t rateX1000 = actualSampleRateX1000Hz(sampleRateX10Hz);
  return static_cast<uint32_t>((static_cast<uint64_t>(sampleIndex) * 1000000ULL +
                                rateX1000 / 2ULL) /
                               rateX1000);
}

uint32_t sampleIntervalUsRounded(uint8_t sampleIndex, uint16_t sampleRateX10Hz) {
  const uint32_t rateX1000 = actualSampleRateX1000Hz(sampleRateX10Hz);
  return static_cast<uint32_t>((static_cast<uint64_t>(sampleIndex) * 1000000000ULL +
                                rateX1000 / 2ULL) /
                               rateX1000);
}

bool timeReachedMs(uint32_t deadlineMs) {
  return static_cast<int32_t>(millis() - deadlineMs) >= 0;
}

bool timeReachedUs(uint32_t deadlineUs) {
  return static_cast<int32_t>(micros() - deadlineUs) >= 0;
}

uint32_t randomDelayMs(uint32_t maxDelayMs) {
  if (maxDelayMs == 0) {
    return 0;
  }
  return static_cast<uint32_t>(random(static_cast<long>(maxDelayMs) + 1L));
}

uint16_t p2pDeviceIdAt(uint8_t index) {
  return static_cast<uint16_t>(AppConfig::P2P_FIRST_DEVICE_ID + index);
}

int p2pDeviceIndex(uint16_t devId) {
  if (devId < AppConfig::P2P_FIRST_DEVICE_ID) {
    return -1;
  }

  const uint16_t offset = devId - AppConfig::P2P_FIRST_DEVICE_ID;
  if (offset >= AppConfig::P2P_NODE_COUNT) {
    return -1;
  }
  return static_cast<int>(offset);
}

uint8_t p2pBurstPacketsPerNode() {
  return AppConfig::P2P_BURST_PACKETS_PER_NODE == 0
             ? 1
             : AppConfig::P2P_BURST_PACKETS_PER_NODE;
}

void seedTxRandom() {
  const uint32_t seed = static_cast<uint32_t>(esp_random()) ^
                        static_cast<uint32_t>(micros()) ^
                        (static_cast<uint32_t>(AppConfig::DEVICE_ID) << 8);
  randomSeed(seed);
}

size_t defaultRateIndex() {
  for (size_t i = 0; i < kRateProfileCount; ++i) {
    if (kRateProfiles[i].sampleRateX10Hz == AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ) {
      return i;
    }
  }
  return 0;
}

bool findRateIndex(uint16_t sampleRateX10Hz, size_t& outIndex) {
  for (size_t i = 0; i < kRateProfileCount; ++i) {
    if (kRateProfiles[i].sampleRateX10Hz == sampleRateX10Hz) {
      outIndex = i;
      return true;
    }
  }
  return false;
}

const RateProfile& activeRateProfile() { return kRateProfiles[activeRateIndex]; }

const RateProfile& candidateRateProfile() { return kRateProfiles[candidateRateIndex]; }

const char* experimentalTag(const RateProfile& profile) {
  return profile.experimental ? " EXP" : "";
}

bool overloadActive() {
  return millis() < overloadDisplayUntilMs;
}

void markDisplayDirty() { displayDirty = true; }

void printRoleInfo() {
#if defined(ROLE_TX)
#if defined(APP_LORAWAN_TX)
  Serial.println("ROLE=TX_LORAWAN");
#elif defined(APP_TX_USB_STREAM_TEST)
  Serial.println("ROLE=TX_USB_STREAM_TEST");
#else
  Serial.println("ROLE=TX");
#endif
#elif defined(ROLE_RX)
  Serial.println("ROLE=RX");
#endif
#if defined(APP_TX_USB_STREAM_TEST)
  Serial.printf("USB stream cfg: packet_samples=%u,compact_header=%u,compact_packet=%u\r\n",
                AppConfig::MAX_SAMPLES_PER_PACKET,
                static_cast<unsigned>(sizeof(Protocol::TxUsbCompactHeader)),
                static_cast<unsigned>(
                    Protocol::calcTxUsbCompactPacketSize(AppConfig::MAX_SAMPLES_PER_PACKET)));
#elif !defined(APP_LORAWAN_TX)
  Serial.printf("P2P cfg: nodes=%u,burst=%u,queue=%u,packet_samples=%u\r\n",
                AppConfig::P2P_NODE_COUNT,
                p2pBurstPacketsPerNode(),
                AppConfig::TX_QUEUE_PACKET_CAPACITY,
                AppConfig::MAX_SAMPLES_PER_PACKET);
#endif
}

void clearPendingBatch() {
  pendingSamples = 0;
  currentBatchStartUs = 0;
}

#if defined(APP_SENSOR_IIM42352)
void IRAM_ATTR onIimInt1() {
  const uint32_t nowUs = micros();
  ++iimInt1RawEdges;
  iimInt1LastEdgeUs = nowUs;
  if (iimInt1TimestampCount >= kIimInt1TimestampCapacity) {
    ++iimInt1TimestampOverflow;
    return;
  }

  const uint8_t tail = static_cast<uint8_t>(
      (iimInt1TimestampHead + iimInt1TimestampCount) % kIimInt1TimestampCapacity);
  iimInt1Timestamps[tail] = nowUs;
  ++iimInt1TimestampCount;
}

void clearIimInt1Timestamps() {
  noInterrupts();
  iimInt1TimestampHead = 0;
  iimInt1TimestampCount = 0;
  iimInt1TimestampOverflow = 0;
  iimInt1RawEdges = 0;
  iimInt1LastEdgeUs = 0;
  interrupts();
  lastIimInt1TimestampUs = 0;
}

bool popIimInt1Timestamp(uint32_t& outTimestampUs) {
  bool ok = false;
  noInterrupts();
  if (iimInt1TimestampCount > 0) {
    outTimestampUs = iimInt1Timestamps[iimInt1TimestampHead];
    iimInt1TimestampHead = static_cast<uint8_t>(
        (iimInt1TimestampHead + 1U) % kIimInt1TimestampCapacity);
    --iimInt1TimestampCount;
    ok = true;
  }
  interrupts();
  return ok;
}

uint8_t iimInt1QueuedTimestamps() {
  uint8_t count = 0;
  noInterrupts();
  count = iimInt1TimestampCount;
  interrupts();
  return count;
}

uint32_t iimInt1OverflowCount() {
  uint32_t count = 0;
  noInterrupts();
  count = iimInt1TimestampOverflow;
  interrupts();
  return count;
}

uint32_t iimInt1RawEdgeCount() {
  uint32_t count = 0;
  noInterrupts();
  count = iimInt1RawEdges;
  interrupts();
  return count;
}

uint32_t iimInt1LastEdgeTimestampUs() {
  uint32_t timestampUs = 0;
  noInterrupts();
  timestampUs = iimInt1LastEdgeUs;
  interrupts();
  return timestampUs;
}

int iimInt1PinMode() {
#if APP_IIM42352_INT1_INPUT_PULLUP_VALUE
  return INPUT_PULLUP;
#else
  return INPUT;
#endif
}

int iimInt1InterruptMode() {
#if defined(APP_IIM42352_INT1_INTERRUPT_MODE_FALLING)
  return FALLING;
#elif defined(APP_IIM42352_INT1_INTERRUPT_MODE_RISING)
  return RISING;
#else
  return CHANGE;
#endif
}

const char* iimInt1InterruptModeName() {
#if defined(APP_IIM42352_INT1_INTERRUPT_MODE_FALLING)
  return "FALLING";
#elif defined(APP_IIM42352_INT1_INTERRUPT_MODE_RISING)
  return "RISING";
#else
  return "CHANGE";
#endif
}
#endif

void clearTxQueue() {
  txQueueHead = 0;
  txQueueCount = 0;
  txWaitingAck = false;
  txWaitingAckSeq = 0;
}

bool txQueueEmpty() { return txQueueCount == 0; }

QueuedTxPacket& txQueueFront() { return txQueue[txQueueHead]; }

void popTxQueueFront() {
  if (txQueueCount == 0) {
    return;
  }
  txQueueHead = static_cast<uint8_t>((txQueueHead + 1U) % AppConfig::TX_QUEUE_PACKET_CAPACITY);
  --txQueueCount;
}

bool pushTxQueue(const uint8_t* data,
                 size_t length,
                 uint16_t seq,
                 uint32_t t0Us,
                 uint8_t count) {
  if (data == nullptr || length == 0 || length > Protocol::MAX_PACKET_SIZE) {
    return false;
  }

  if (txQueueCount >= AppConfig::TX_QUEUE_PACKET_CAPACITY) {
    ++queueOverflowCount;
    Serial.printf("TX queue full: dev=%u,drop_seq=%u,overflow=%lu\r\n",
                  AppConfig::DEVICE_ID,
                  seq,
                  static_cast<unsigned long>(queueOverflowCount));
    return false;
  }

  const uint8_t tail =
      static_cast<uint8_t>((txQueueHead + txQueueCount) % AppConfig::TX_QUEUE_PACKET_CAPACITY);
  QueuedTxPacket& item = txQueue[tail];
  memcpy(item.data, data, length);
  item.length = length;
  item.seq = seq;
  item.t0Us = t0Us;
  item.count = count;
  item.retries = 0;
  ++txQueueCount;
  return true;
}

void discardSensorFifo(bool printLog) {
  size_t flushed = 0;
  while (true) {
    const size_t count = adxl.readFifoBurst(
        fifoBurstBuffer, sizeof(fifoBurstBuffer) / sizeof(fifoBurstBuffer[0]));
    if (count == 0) {
      break;
    }
    flushed += count;
    if (count < sizeof(fifoBurstBuffer) / sizeof(fifoBurstBuffer[0])) {
      break;
    }
  }

  if (printLog && flushed > 0) {
    Serial.printf("sensor fifo flushed: %u samples\r\n", static_cast<unsigned>(flushed));
  }
}

void noteOverload(uint8_t fifoDepth) {
  ++overloadEventCount;
  overloadDisplayUntilMs = millis() + 1500UL;

  if (millis() - lastOverloadLogMs < 1000UL) {
    return;
  }

  lastOverloadLogMs = millis();
  Serial.printf("TX overload: fs=%.1f%s,fifo=%u,events=%lu\r\n",
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()),
                fifoDepth,
                static_cast<unsigned long>(overloadEventCount));
  markDisplayDirty();
}

void drawTxScreen() {
  if (!oledReady) {
    return;
  }

  oled.clearDisplay();
  oled.setTextSize(1);
  oled.setTextColor(SSD1306_WHITE);
  oled.setCursor(0, 0);

  if (txUiMode == TxUiMode::Select) {
    const RateProfile& profile = candidateRateProfile();
    oled.printf("RATE SELECT\r\n");
    oled.printf("> %s Hz%s\r\n", profile.label, experimentalTag(profile));
    oled.printf("NOW %s Hz%s\r\n",
                activeRateProfile().label,
                experimentalTag(activeRateProfile()));
    oled.printf("DEV %u\r\n", AppConfig::DEVICE_ID);
    oled.printf("Click: Next\r\n");
    oled.printf("Hold : Apply\r\n");
    oled.printf("Back %lus\r\n",
                static_cast<unsigned long>(AppConfig::UI_SELECT_TIMEOUT_MS / 1000UL));
  } else {
    const RateProfile& profile = activeRateProfile();
    oled.printf("TX RUN DEV %u\r\n", AppConfig::DEVICE_ID);
    oled.printf("RATE %s Hz%s\r\n", profile.label, experimentalTag(profile));
    oled.printf("SEQ %u\r\n", packetSeq);
    oled.printf("Q %u/%u B %u/%u\r\n",
                txQueueCount,
                AppConfig::TX_QUEUE_PACKET_CAPACITY,
                pendingSamples,
                AppConfig::MAX_SAMPLES_PER_PACKET);
    if (overloadActive()) {
      oled.printf("OVERLOAD\r\n");
#if defined(APP_LORAWAN_TX)
    } else {
      oled.printf("RADIO READY\r\n");
    }
#else
#if defined(APP_SENSOR_IIM42352)
    } else if (!txTimebaseReady()) {
      oled.printf("WAIT GPS/PPS\r\n");
#endif
    } else if (!txRunArmed) {
      oled.printf("WAIT START\r\n");
    } else if (!txCaptureStarted) {
      oled.printf("ARMED\r\n");
    } else if (txWaitingAck) {
      oled.printf("WAIT ACK\r\n");
    } else {
      oled.printf("RADIO READY\r\n");
    }
#endif
    oled.printf("Click: Menu\r\n");
    oled.printf("Hold in menu\r\n");
  }

  oled.display();
  lastDisplayRefreshMs = millis();
  displayDirty = false;
}

void refreshTxScreen(uint32_t nowMs) {
  if (!oledReady) {
    return;
  }

  if (!displayDirty && (nowMs - lastDisplayRefreshMs) < AppConfig::UI_REFRESH_MS) {
    return;
  }

  drawTxScreen();
}

bool initOled() {
  pinMode(PIN_VEXT_CTRL, OUTPUT);
  digitalWrite(PIN_VEXT_CTRL, LOW);
  delay(10);

  pinMode(PIN_OLED_RST, OUTPUT);
  digitalWrite(PIN_OLED_RST, HIGH);
  delay(1);
  digitalWrite(PIN_OLED_RST, LOW);
  delay(20);
  digitalWrite(PIN_OLED_RST, HIGH);
  delay(20);

  displayWire.begin(PIN_OLED_SDA, PIN_OLED_SCL, AppConfig::OLED_I2C_CLOCK_HZ);

  if (oled.begin(SSD1306_SWITCHCAPVCC, AppConfig::OLED_I2C_ADDR_PRIMARY, true, false)) {
    oledAddress = AppConfig::OLED_I2C_ADDR_PRIMARY;
  } else if (oled.begin(SSD1306_SWITCHCAPVCC, AppConfig::OLED_I2C_ADDR_FALLBACK, true, false)) {
    oledAddress = AppConfig::OLED_I2C_ADDR_FALLBACK;
  } else {
    oledReady = false;
    Serial.println("OLED init failed: addr 0x3C/0x3D not found");
    return false;
  }

  oledReady = true;
  oled.clearDisplay();
  oled.setTextWrap(false);
  markDisplayDirty();
  Serial.printf("OLED init ok: SDA=%d SCL=%d RST=%d ADDR=0x%02X\r\n",
                PIN_OLED_SDA,
                PIN_OLED_SCL,
                PIN_OLED_RST,
                oledAddress);
  return true;
}

void showBootScreen() {
  if (!oledReady) {
    return;
  }

  oled.clearDisplay();
  oled.setTextSize(1);
  oled.setTextColor(SSD1306_WHITE);
  oled.setCursor(0, 0);
#if defined(APP_TX_USB_STREAM_TEST)
  oled.printf("TX USB TEST\r\n");
#elif defined(APP_SENSOR_IIM42352)
  oled.printf("TX IIM GPS PPS\r\n");
#else
  oled.printf("TX FIELD READY\r\n");
#endif
  oled.printf("DEV %u\r\n", AppConfig::DEVICE_ID);
  oled.printf("RATE %s Hz%s\r\n",
              activeRateProfile().label,
              experimentalTag(activeRateProfile()));
#if defined(APP_TX_USB_STREAM_TEST)
  oled.printf("USB STREAM\r\n");
#elif defined(APP_LORAWAN_TX)
  oled.printf("LoRaWAN CN470\r\n");
#else
  oled.printf("LoRa %.1f MHz\r\n", AppConfig::LORA_FREQUENCY_MHZ);
#endif
  oled.printf("Click: Menu\r\n");
  oled.printf("Hold : Apply\r\n");
  oled.display();
  delay(kBootScreenMs);
  markDisplayDirty();
}

const char* sensorName() {
#if defined(APP_SENSOR_IIM42352)
  return "IIM-42352";
#else
  return "ADXL345";
#endif
}

bool initAdxl() {
#if defined(APP_SENSOR_IIM42352)
  pinMode(PIN_LORA_NSS, OUTPUT);
  digitalWrite(PIN_LORA_NSS, HIGH);
  pinMode(PIN_IIM42352_CS, OUTPUT);
  digitalWrite(PIN_IIM42352_CS, HIGH);
  pinMode(PIN_IIM42352_INT1, iimInt1PinMode());
  clearIimInt1Timestamps();

  Serial.printf("IIM-42352 SPI pins: SCK=%d MOSI=%d MISO=%d CS=%d INT1=%d INT2=%d\r\n",
                PIN_IIM42352_SCK,
                PIN_IIM42352_MOSI,
                PIN_IIM42352_MISO,
                PIN_IIM42352_CS,
                PIN_IIM42352_INT1,
                PIN_IIM42352_INT2);

  if (!adxl.begin()) {
    Serial.printf("IIM-42352 init failed: WHO_AM_I read=0x%02X, expected=0x%02X\r\n",
                  adxl.lastDeviceId(),
                  adxl.expectedDeviceId());
    Serial.printf("IIM-42352 diag: WHO=0x%02X PWR=0x%02X ACCEL_CFG=0x%02X FIFO_STATUS=0x%02X\r\n",
                  adxl.readRegister(0x75),
                  adxl.readRegister(adxl.regPowerCtl()),
                  adxl.readRegister(adxl.regDataFormat()),
                  adxl.readRegister(adxl.regFifoStatus()));
    return false;
  }

  adxl.setRange(SensorDriver::Range::Range4G);
  adxl.setFullResolution(true);
  adxl.setDataRate(activeRateProfile().sensorRate);
  const bool iimClkinOk = adxl.externalClockConfigured();
  const bool iimInt1Ok = adxl.dataReadyInt1Configured();
  delay(5);
  discardSensorFifo(false);
  clearIimInt1Timestamps();
  attachInterrupt(digitalPinToInterrupt(PIN_IIM42352_INT1),
                  onIimInt1,
                  iimInt1InterruptMode());

  Serial.println("IIM-42352 init ok");
  Serial.printf("IIM-42352 who_am_i=0x%02X\r\n", adxl.readRegister(0x75));
  Serial.printf("IIM-42352 clk_in=%u,int1_drdy=%u,INTF_CONFIG1=0x%02X,INTF_CONFIG5=0x%02X,INT_SOURCE0=0x%02X,INT_CONFIG1=0x%02X,INT_CONFIG=0x%02X,INT_STATUS=0x%02X\r\n",
                iimClkinOk ? 1 : 0,
                iimInt1Ok ? 1 : 0,
                adxl.readRegister(adxl.regIntfConfig1()),
                adxl.readBankedRegister(1, adxl.regIntfConfig5()),
                adxl.readRegister(adxl.regIntSource0()),
                adxl.readRegister(adxl.regIntConfig1()),
                adxl.readRegister(adxl.regIntConfig()),
                adxl.readRegister(adxl.regIntStatus()));
  Serial.printf("IIM-42352 INT1 capture: pin=%d,pullup=%u,mode=%s,level=%u\r\n",
                PIN_IIM42352_INT1,
                APP_IIM42352_INT1_INPUT_PULLUP_VALUE ? 1 : 0,
                iimInt1InterruptModeName(),
                digitalRead(PIN_IIM42352_INT1) == HIGH ? 1 : 0);
  Serial.println("IIM-42352 range=+-4g, spi+fifo+DS3231 CLKIN+INT1 DRDY mode");
#else
  Serial.printf("ADXL345 I2C pins: SDA=%d SCL=%d ADDR=0x%02X INT1=%d\r\n",
                PIN_ADXL345_SDA,
                PIN_ADXL345_SCL,
                ADXL345_I2C_ADDRESS,
                PIN_ADXL345_INT1);

  if (!adxl.begin()) {
    Serial.printf("ADXL345 init failed: DEVID read=0x%02X, expected=0x%02X\r\n",
                  adxl.lastDeviceId(),
                  adxl.expectedDeviceId());
    Serial.printf("ADXL345 diag: reg[0x%02X]=0x%02X reg[0x%02X]=0x%02X reg[0x%02X]=0x%02X reg[0x%02X]=0x%02X\r\n",
                  0x00,
                  adxl.readRegister(0x00),
                  adxl.regDataFormat(),
                  adxl.readRegister(adxl.regDataFormat()),
                  adxl.regPowerCtl(),
                  adxl.readRegister(adxl.regPowerCtl()),
                  adxl.regFifoStatus(),
                  adxl.readRegister(adxl.regFifoStatus()));
    return false;
  }

  adxl.setRange(SensorDriver::Range::Range4G);
  adxl.setFullResolution(true);
  adxl.setDataRate(activeRateProfile().sensorRate);
  delay(5);
  discardSensorFifo(false);

  Serial.println("ADXL345 init ok");
  Serial.printf("ADXL345 devid=0x%02X\r\n", adxl.readRegister(0x00));
  Serial.println("ADXL345 range=+-4g, full-resolution=on");
#endif

  Serial.printf("%s sample_rate configured=%.1f Hz actual=%.3f Hz%s\r\n",
                sensorName(),
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                actualSampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()));
  return true;
}

#if defined(APP_SENSOR_IIM42352)
bool initDs3231() {
  Serial.printf("DS3231 I2C pins: SDA=%d SCL=%d ADDR=0x%02X\r\n",
                PIN_DS3231_SDA,
                PIN_DS3231_SCL,
                DS3231_I2C_ADDRESS);
  ds3231Ready = ds3231.begin();
  ds3231_32k_valid = ds3231Ready && ds3231.is32kHzEnabled();
  if (!ds3231Ready) {
    Serial.println("DS3231 init failed: TX will wait and reject START_AT until fixed");
    return false;
  }
  Serial.printf("DS3231 init ok: status=0x%02X,32k=%u,osf=%u\r\n",
                ds3231.lastStatus(),
                ds3231_32k_valid ? 1 : 0,
                ds3231.oscillatorStopped() ? 1 : 0);
  return ds3231_32k_valid;
}

bool initGpsTimebase() {
  Serial.printf("LC76G UART pins: RX=%d TX=%d PPS=%d baud=%lu\r\n",
                PIN_GPS_RX,
                PIN_GPS_TX,
                PIN_GPS_PPS,
                static_cast<unsigned long>(AppConfig::GPS_BAUD));
  return gpsTimebase.begin(PIN_GPS_RX, PIN_GPS_TX, PIN_GPS_PPS, AppConfig::GPS_BAUD);
}

void updateGpsTimebase() { gpsTimebase.update(); }

bool txTimebaseReady() {
  return ds3231Ready && ds3231_32k_valid && gpsTimebase.timeValid();
}

void logGpsWaitStatus(uint32_t nowMs) {
  if (nowMs - lastGpsWaitLogMs < 1000UL) {
    return;
  }
  lastGpsWaitLogMs = nowMs;
  Serial.printf("TX wait timebase: gps_fix=%u,pps=%u,ds3231=%u,32k=%u,pps_interval_us=%lu\r\n",
                gpsTimebase.fixValid() ? 1 : 0,
                gpsTimebase.ppsValid() ? 1 : 0,
                ds3231Ready ? 1 : 0,
                ds3231_32k_valid ? 1 : 0,
                static_cast<unsigned long>(gpsTimebase.ppsIntervalUs()));
}
#else
void updateGpsTimebase() {}
#endif

bool initLoRa() {
#if defined(APP_LORAWAN_TX)
  if (!LoRaWanTransport::begin()) {
    Serial.printf("LoRaWAN init failed: %s (%d)\r\n",
                  LoRaWanTransport::stateToString(LoRaWanTransport::lastState()),
                  LoRaWanTransport::lastState());
    return false;
  }

  Serial.println("LoRaWAN init ok");
  LoRaWanTransport::printConfig(Serial);
  return true;
#else
  if (!LoRaTransport::begin()) {
    Serial.println("LoRa init failed");
    return false;
  }

  Serial.println("LoRa init ok");
  LoRaTransport::printConfig(Serial);
  return true;
#endif
}

bool sendControlFrame(uint8_t frameType,
                      uint16_t devId,
                      uint16_t seq,
                      uint64_t epochUs,
                      uint32_t param,
                      uint16_t fsHzX10) {
  size_t length = 0;
  if (!Protocol::encodeControlFrame(frameType,
                                    devId,
                                    seq,
                                    epochUs,
                                    param,
                                    fsHzX10,
                                    controlPacketBuffer,
                                    sizeof(controlPacketBuffer),
                                    length)) {
    Serial.printf("control encode failed: type=%u,dev=%u,seq=%u\r\n",
                  frameType,
                  devId,
                  seq);
    return false;
  }

  if (!LoRaTransport::sendPacket(controlPacketBuffer, length)) {
    Serial.printf("control send failed: type=%u,dev=%u,state=%s (%d)\r\n",
                  frameType,
                  devId,
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
    (void)LoRaTransport::startReceive();
    return false;
  }

  (void)LoRaTransport::startReceive();
  return true;
}

uint16_t parseSampleRateX10(const char* text) {
  if (text == nullptr || *text == '\0') {
    return AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
  }

  const double value = strtod(text, nullptr);
  if (value <= 0.0) {
    return AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
  }

  if (value <= 400.0) {
    return static_cast<uint16_t>(value * 10.0 + 0.5);
  }
  return static_cast<uint16_t>(value + 0.5);
}

void applyRateProfile(size_t newIndex, bool printLog) {
  const RateProfile& previous = activeRateProfile();
  activeRateIndex = newIndex;
  candidateRateIndex = newIndex;
  samplePeriodUs = samplePeriodUsForRate(activeRateProfile().sampleRateX10Hz);

  adxl.setDataRate(activeRateProfile().sensorRate);
  delay(5);
  discardSensorFifo(true);
  clearPendingBatch();
  overloadDisplayUntilMs = 0;
  markDisplayDirty();

  if (printLog) {
    Serial.printf("TX rate applied: %.1f Hz%s -> %.1f Hz%s\r\n",
                  sampleRateHzFloat(previous.sampleRateX10Hz),
                  experimentalTag(previous),
                  sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                  experimentalTag(activeRateProfile()));
  }
}

void enterSelectMode(uint32_t nowMs) {
  txUiMode = TxUiMode::Select;
  candidateRateIndex = activeRateIndex;
  lastUiInteractionMs = nowMs;
  markDisplayDirty();
}

void handleShortPress(uint32_t nowMs) {
  if (!oledReady) {
    return;
  }

  if (txUiMode != TxUiMode::Select) {
    enterSelectMode(nowMs);
  }

  candidateRateIndex = (candidateRateIndex + 1U) % kRateProfileCount;
  lastUiInteractionMs = nowMs;
  markDisplayDirty();

  Serial.printf("TX rate select: %s Hz%s\r\n",
                candidateRateProfile().label,
                experimentalTag(candidateRateProfile()));
}

void handleLongPress(uint32_t nowMs) {
  if (!oledReady || txUiMode != TxUiMode::Select) {
    return;
  }

  lastUiInteractionMs = nowMs;
  txUiMode = TxUiMode::Normal;

  if (candidateRateIndex != activeRateIndex) {
    applyRateProfile(candidateRateIndex, true);
  }

  markDisplayDirty();
}

void updateTxButton(uint32_t nowMs) {
  if (!oledReady) {
    return;
  }

  const bool rawPressed = (digitalRead(PIN_USER_BUTTON) == LOW);
  if (rawPressed != buttonRawPressed) {
    buttonRawPressed = rawPressed;
    buttonRawChangedMs = nowMs;
  }

  if ((nowMs - buttonRawChangedMs) < AppConfig::UI_DEBOUNCE_MS) {
    return;
  }

  if (buttonStablePressed != rawPressed) {
    buttonStablePressed = rawPressed;
    if (buttonStablePressed) {
      buttonPressedAtMs = nowMs;
      buttonLongPressHandled = false;
    } else if (!buttonLongPressHandled) {
      handleShortPress(nowMs);
    }
    return;
  }

  if (buttonStablePressed && !buttonLongPressHandled &&
      (nowMs - buttonPressedAtMs) >= AppConfig::UI_LONG_PRESS_MS) {
    handleLongPress(nowMs);
    buttonLongPressHandled = true;
  }
}

bool sendCurrentBatch() {
  if (pendingSamples == 0) {
    return true;
  }

  size_t packetLength = 0;
#if defined(APP_LORAWAN_TX)
  const uint32_t batchStartUs = currentBatchStartUs;
#else
  const uint32_t batchStartUs = currentBatchStartUs - txRunStartLocalUs;
#endif
  const bool ok =
      Protocol::encodePacket(AppConfig::DEVICE_ID,
                             packetSeq,
                             batchStartUs,
                             activeRateProfile().sampleRateX10Hz,
                             sampleBuffer,
                             pendingSamples,
                             txPacketBuffer,
                             sizeof(txPacketBuffer),
                             packetLength);
  if (!ok) {
    Serial.println("TX encode failed");
    return false;
  }

#if defined(APP_TX_USB_STREAM_TEST)
  Serial.printf("TXH,%u,%u,%lu,%u,%u\r\n",
                AppConfig::DEVICE_ID,
                packetSeq,
                static_cast<unsigned long>(batchStartUs),
                activeRateProfile().sampleRateX10Hz,
                pendingSamples);
  for (uint8_t i = 0; i < pendingSamples; ++i) {
    const uint64_t sampleEpochUs =
        txRunStartEpochUs + static_cast<uint64_t>(batchStartUs) +
        sampleIntervalUsRounded(i, activeRateProfile().sampleRateX10Hz);
    Serial.printf("%u,%llu,%d,%d,%d\r\n",
                  AppConfig::DEVICE_ID,
                  static_cast<unsigned long long>(sampleEpochUs),
                  sampleBuffer[i].ax,
                  sampleBuffer[i].ay,
                  sampleBuffer[i].az);
  }
  ++txUsbPackets;
  txUsbSamples += pendingSamples;
  Serial.printf("TXS,%u,%lu,%lu,%lu,%lu,%lu,%lu,%u\r\n",
                AppConfig::DEVICE_ID,
                static_cast<unsigned long>(txUsbPackets),
                static_cast<unsigned long>(txUsbSamples),
                static_cast<unsigned long>(txUsbInt1Missing),
#if defined(APP_SENSOR_IIM42352)
                static_cast<unsigned long>(iimInt1OverflowCount()),
#else
                0UL,
#endif
                static_cast<unsigned long>(txUsbFifoReadFail),
#if defined(APP_SENSOR_IIM42352)
                static_cast<unsigned long>(iimInt1RawEdgeCount()),
                digitalRead(PIN_IIM42352_INT1) == HIGH ? 1 : 0
#else
                0UL,
                0
#endif
                );
  ++packetSeq;
  clearPendingBatch();
  markDisplayDirty();
  return true;
#elif defined(APP_LORAWAN_TX)
  const uint32_t packetJitterMs = randomDelayMs(AppConfig::TX_PACKET_JITTER_MAX_MS);
  if (packetJitterMs > 0) {
    delay(packetJitterMs);
  }

  bool sent = false;
#if defined(APP_LORAWAN_TX)
  sent = LoRaWanTransport::sendUplink(txPacketBuffer, packetLength);
#else
  sent = LoRaTransport::sendPacket(txPacketBuffer, packetLength);
#endif
  if (!sent) {
#if defined(APP_LORAWAN_TX)
    Serial.printf("TX LoRaWAN uplink failed: %s (%d)\r\n",
                  LoRaWanTransport::stateToString(LoRaWanTransport::lastState()),
                  LoRaWanTransport::lastState());
#else
    Serial.println("TX LoRa send failed");
#endif
    return false;
  }

#if defined(APP_LORAWAN_TX)
  const LoRaWanTransport::UplinkMeta& meta = LoRaWanTransport::lastUplinkMeta();
  Serial.printf("TX dev=%u,seq=%u,count=%u,fs=%.1f%s,t0=%lu,len=%u,jitter=%lu,fcnt=%lu,dr=%u,freq=%.3f,toa=%lu\r\n",
                AppConfig::DEVICE_ID,
                packetSeq,
                pendingSamples,
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()),
                static_cast<unsigned long>(batchStartUs),
                static_cast<unsigned>(packetLength),
                static_cast<unsigned long>(packetJitterMs),
                static_cast<unsigned long>(meta.frameCounter),
                meta.dataRate,
                meta.frequencyMHz,
                static_cast<unsigned long>(meta.timeOnAirMs));
#else
  Serial.printf("TX dev=%u,seq=%u,count=%u,fs=%.1f%s,t0=%lu,len=%u,jitter=%lu\r\n",
                AppConfig::DEVICE_ID,
                packetSeq,
                pendingSamples,
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()),
                static_cast<unsigned long>(batchStartUs),
                static_cast<unsigned>(packetLength),
                static_cast<unsigned long>(packetJitterMs));
#endif
  ++packetSeq;
  clearPendingBatch();
  markDisplayDirty();
  delay(AppConfig::TX_IDLE_DELAY_MS);
  return true;
#else
  const bool queued =
      pushTxQueue(txPacketBuffer, packetLength, packetSeq, batchStartUs, pendingSamples);
  if (queued) {
    Serial.printf("TX queued dev=%u,seq=%u,count=%u,fs=%.1f%s,t0_offset_us=%lu,len=%u,q=%u/%u\r\n",
                  AppConfig::DEVICE_ID,
                  packetSeq,
                  pendingSamples,
                  sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                  experimentalTag(activeRateProfile()),
                  static_cast<unsigned long>(batchStartUs),
                  static_cast<unsigned>(packetLength),
                  txQueueCount,
                  AppConfig::TX_QUEUE_PACKET_CAPACITY);
  }

  ++packetSeq;
  clearPendingBatch();
  markDisplayDirty();
  return queued;
#endif
}

void processFifoSamples() {
  if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
    return;
  }

  const uint8_t fifoDepth = adxl.fifoAvailable();
  if (fifoDepth == 0) {
    return;
  }

  if (activeRateProfile().experimental &&
      fifoDepth >= AppConfig::SENSOR_FIFO_OVERLOAD_LEVEL) {
    noteOverload(fifoDepth);
  }

  const size_t burstCapacity =
      sizeof(fifoBurstBuffer) / sizeof(fifoBurstBuffer[0]);
  const size_t requested = min(static_cast<size_t>(fifoDepth), burstCapacity);
  const size_t actual = adxl.readFifoBurst(fifoBurstBuffer, requested);
  if (actual == 0) {
#if defined(APP_TX_USB_STREAM_TEST)
    ++txUsbFifoReadFail;
#endif
    Serial.printf("%s readFifoBurst failed\r\n", sensorName());
    return;
  }

#if !defined(APP_SENSOR_IIM42352)
  const uint32_t burstEndUs = micros();
  const uint32_t burstStartUs =
      burstEndUs - static_cast<uint32_t>((static_cast<uint64_t>(actual) - 1ULL) * samplePeriodUs);
#endif

  for (size_t i = 0; i < actual; ++i) {
    if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
      if (!sendCurrentBatch()) {
        return;
      }
    }

#if defined(APP_SENSOR_IIM42352)
    uint32_t sampleTimestampUs = 0;
    if (popIimInt1Timestamp(sampleTimestampUs)) {
      lastIimInt1TimestampUs = sampleTimestampUs;
    } else if (lastIimInt1TimestampUs != 0) {
      sampleTimestampUs = lastIimInt1TimestampUs + samplePeriodUs;
      lastIimInt1TimestampUs = sampleTimestampUs;
#if defined(APP_TX_USB_STREAM_TEST)
      ++txUsbInt1Missing;
#endif
      const uint32_t nowMs = millis();
      if (nowMs - lastIimInt1ReportMs >= 1000UL) {
        lastIimInt1ReportMs = nowMs;
        Serial.printf("IIM INT1 timestamp missing: fifo=%u,queued=%u,overflow=%lu,pin_level=%u,raw_edges=%lu,last_edge_us=%lu\r\n",
                      fifoDepth,
                      iimInt1QueuedTimestamps(),
                      static_cast<unsigned long>(iimInt1OverflowCount()),
                      digitalRead(PIN_IIM42352_INT1) == HIGH ? 1 : 0,
                      static_cast<unsigned long>(iimInt1RawEdgeCount()),
                      static_cast<unsigned long>(iimInt1LastEdgeTimestampUs()));
      }
    } else {
      sampleTimestampUs = micros();
      lastIimInt1TimestampUs = sampleTimestampUs;
#if defined(APP_TX_USB_STREAM_TEST)
      ++txUsbInt1Missing;
#endif
      const uint32_t nowMs = millis();
      if (nowMs - lastIimInt1ReportMs >= 1000UL) {
        lastIimInt1ReportMs = nowMs;
        Serial.printf("IIM INT1 not seen yet: fifo=%u,queued=%u,overflow=%lu,pin_level=%u,raw_edges=%lu,last_edge_us=%lu\r\n",
                      fifoDepth,
                      iimInt1QueuedTimestamps(),
                      static_cast<unsigned long>(iimInt1OverflowCount()),
                      digitalRead(PIN_IIM42352_INT1) == HIGH ? 1 : 0,
                      static_cast<unsigned long>(iimInt1RawEdgeCount()),
                      static_cast<unsigned long>(iimInt1LastEdgeTimestampUs()));
      }
    }
#else
    const uint32_t sampleTimestampUs =
        burstStartUs + static_cast<uint32_t>(static_cast<uint64_t>(i) * samplePeriodUs);
#endif
    if (pendingSamples == 0) {
      currentBatchStartUs = sampleTimestampUs;
    }

    sampleBuffer[pendingSamples++] = {
        fifoBurstBuffer[i].x,
        fifoBurstBuffer[i].y,
        fifoBurstBuffer[i].z,
    };

    if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
      if (!sendCurrentBatch()) {
        return;
      }
    }
  }

  markDisplayDirty();
}

void armTxRun(const Protocol::ControlFrame& frame) {
  if (frame.epochUs == 0 || frame.fsHzX10 == 0) {
    Serial.println("TX ignored invalid START_AT frame");
    return;
  }

  size_t rateIndex = 0;
  if (!findRateIndex(frame.fsHzX10, rateIndex)) {
    Serial.printf("TX ignored START_AT unsupported fs_x10=%u\r\n", frame.fsHzX10);
    return;
  }

  uint32_t startLocalUs = micros() + frame.param * 1000UL;
#if defined(APP_REQUIRE_GPS_TIMEBASE)
  updateGpsTimebase();
  if (!gpsTimebase.localMicrosForEpoch(frame.epochUs, startLocalUs)) {
    Serial.printf("TX ignored START_AT waiting GPS/PPS: dev=%u,run=%u,epoch_us=%llu\r\n",
                  AppConfig::DEVICE_ID,
                  frame.seq,
                  static_cast<unsigned long long>(frame.epochUs));
    return;
  }
#endif

  const bool newRun = !txRunArmed || frame.seq != txRunId ||
                      frame.epochUs != txRunStartEpochUs;
  txRunId = frame.seq;
  txRunStartEpochUs = frame.epochUs;
  txRunStartLocalUs = startLocalUs;
  txRunArmed = true;

  if (newRun) {
    txCaptureStarted = false;
    packetSeq = 0;
    clearPendingBatch();
    clearTxQueue();
#if defined(APP_SENSOR_IIM42352)
    clearIimInt1Timestamps();
#endif
    applyRateProfile(rateIndex, true);
    discardSensorFifo(false);
    Serial.printf("TX START_AT armed: dev=%u,run=%u,epoch_us=%llu,delay_ms=%lu,fs=%.1f\r\n",
                  AppConfig::DEVICE_ID,
                  txRunId,
                  static_cast<unsigned long long>(txRunStartEpochUs),
                  static_cast<unsigned long>(frame.param),
                  sampleRateHzFloat(activeRateProfile().sampleRateX10Hz));
  } else if (!txCaptureStarted) {
    Serial.printf("TX START_AT refreshed: dev=%u,run=%u,delay_ms=%lu\r\n",
                  AppConfig::DEVICE_ID,
                  txRunId,
                  static_cast<unsigned long>(frame.param));
  }

  markDisplayDirty();
}

#if defined(APP_TX_USB_STREAM_TEST)
void armTxRunUsb(uint64_t startEpochUs, uint16_t sampleRateX10Hz, uint32_t delayMs) {
  if (startEpochUs == 0 || sampleRateX10Hz == 0) {
    Serial.println("TX USB START_AT invalid");
    return;
  }

  size_t rateIndex = 0;
  if (!findRateIndex(sampleRateX10Hz, rateIndex)) {
    Serial.printf("TX USB START_AT unsupported fs_x10=%u\r\n", sampleRateX10Hz);
    return;
  }

  ++txRunId;
  if (txRunId == 0) {
    ++txRunId;
  }
  txRunStartEpochUs = startEpochUs;
  txRunStartLocalUs = micros() + delayMs * 1000UL;
  txRunArmed = true;
  txCaptureStarted = false;
  txWaitingAck = false;
  txWaitingAckSeq = 0;
  packetSeq = 0;
  txUsbPackets = 0;
  txUsbSamples = 0;
  txUsbFifoReadFail = 0;
  txUsbInt1Missing = 0;
  clearPendingBatch();
  clearTxQueue();
#if defined(APP_SENSOR_IIM42352)
  clearIimInt1Timestamps();
#endif
  applyRateProfile(rateIndex, true);
  discardSensorFifo(false);

  Serial.printf("TX USB START_AT armed: dev=%u,run=%u,epoch_us=%llu,delay_ms=%lu,fs=%.1f,compact_packet=%u\r\n",
                AppConfig::DEVICE_ID,
                txRunId,
                static_cast<unsigned long long>(txRunStartEpochUs),
                static_cast<unsigned long>(delayMs),
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                static_cast<unsigned>(
                    Protocol::calcTxUsbCompactPacketSize(AppConfig::MAX_SAMPLES_PER_PACKET)));
  markDisplayDirty();
}

void handleTxSerialLine(char* line) {
  if (line == nullptr) {
    return;
  }

  char* command = strtok(line, " \t,");
  if (command == nullptr) {
    return;
  }

  if (strcmp(command, "START_AT") != 0) {
    Serial.printf("TX USB unknown serial command: %s\r\n", command);
    return;
  }

  char* epochText = strtok(nullptr, " \t,");
  char* rateText = strtok(nullptr, " \t,");
  char* delayText = strtok(nullptr, " \t,");
  if (epochText == nullptr || rateText == nullptr) {
    Serial.println("TX USB START_AT usage: START_AT epoch_us sample_rate_hz [delay_ms]");
    return;
  }

  const uint64_t startEpochUs = strtoull(epochText, nullptr, 10);
  const uint16_t sampleRateX10Hz = parseSampleRateX10(rateText);
  uint32_t delayMs = AppConfig::P2P_START_DEFAULT_DELAY_MS;
  if (delayText != nullptr) {
    const unsigned long parsed = strtoul(delayText, nullptr, 10);
    if (parsed > 0) {
      delayMs = static_cast<uint32_t>(parsed);
    }
  }

  armTxRunUsb(startEpochUs, sampleRateX10Hz, delayMs);
}

void handleTxSerial() {
  while (Serial.available() > 0) {
    const char c = static_cast<char>(Serial.read());
    if (c == '\r') {
      continue;
    }

    if (c == '\n') {
      txSerialLine[txSerialLineLen] = '\0';
      handleTxSerialLine(txSerialLine);
      txSerialLineLen = 0;
      txSerialLine[0] = '\0';
      continue;
    }

    if (txSerialLineLen + 1 < sizeof(txSerialLine)) {
      txSerialLine[txSerialLineLen++] = c;
    } else {
      txSerialLineLen = 0;
      txSerialLine[0] = '\0';
      Serial.println("TX USB serial command too long");
    }
  }
}
#endif

void startTxCaptureIfDue() {
#if !defined(APP_LORAWAN_TX)
  if (!txRunArmed || txCaptureStarted || !timeReachedUs(txRunStartLocalUs)) {
    return;
  }

  discardSensorFifo(false);
  clearPendingBatch();
#if defined(APP_SENSOR_IIM42352)
  clearIimInt1Timestamps();
#endif
  txCaptureStarted = true;
  Serial.printf("TX capture started: dev=%u,run=%u,epoch_us=%llu,fs=%.1f\r\n",
                AppConfig::DEVICE_ID,
                txRunId,
                static_cast<unsigned long long>(txRunStartEpochUs),
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz));
  markDisplayDirty();
#endif
}

bool sendTxQueueHead() {
  if (txQueueEmpty()) {
    return sendControlFrame(Protocol::FRAME_TYPE_NO_DATA,
                            AppConfig::DEVICE_ID,
                            packetSeq,
                            0,
                            0,
                            activeRateProfile().sampleRateX10Hz);
  }

  QueuedTxPacket& item = txQueueFront();
  if (!LoRaTransport::sendPacket(item.data, item.length)) {
    Serial.printf("TX data send failed: dev=%u,seq=%u,state=%s (%d)\r\n",
                  AppConfig::DEVICE_ID,
                  item.seq,
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
    (void)LoRaTransport::startReceive();
    return false;
  }

  ++item.retries;
  txWaitingAck = true;
  txWaitingAckSeq = item.seq;
  txAckDeadlineMs = millis() + AppConfig::P2P_TX_ACK_TIMEOUT_MS;
  (void)LoRaTransport::startReceive();
  Serial.printf("TX data sent: dev=%u,seq=%u,count=%u,retry=%u,q=%u/%u\r\n",
                AppConfig::DEVICE_ID,
                item.seq,
                item.count,
                item.retries,
                txQueueCount,
                AppConfig::TX_QUEUE_PACKET_CAPACITY);
  markDisplayDirty();
  return true;
}

void handleTxControlFrame(const Protocol::ControlFrame& frame) {
  if (frame.frameType == Protocol::FRAME_TYPE_START_AT) {
    armTxRun(frame);
    return;
  }

  if (frame.devId != AppConfig::DEVICE_ID) {
    return;
  }

  if (frame.frameType == Protocol::FRAME_TYPE_POLL) {
    if (!txCaptureStarted) {
      (void)sendControlFrame(Protocol::FRAME_TYPE_NO_DATA,
                             AppConfig::DEVICE_ID,
                             packetSeq,
                             0,
                             0,
                             activeRateProfile().sampleRateX10Hz);
      return;
    }

    if (!txWaitingAck) {
      (void)sendTxQueueHead();
    }
    return;
  }

  if (frame.frameType == Protocol::FRAME_TYPE_ACK && txWaitingAck &&
      !txQueueEmpty() && frame.seq == txQueueFront().seq) {
    const uint8_t qBefore = txQueueCount;
    const bool grantNext = frame.param > 0;
    popTxQueueFront();
    txWaitingAck = false;
    txWaitingAckSeq = 0;
    Serial.printf("TX acked: dev=%u,seq=%u,grant=%u,q_before=%u,q_after=%u\r\n",
                  AppConfig::DEVICE_ID,
                  frame.seq,
                  grantNext ? 1U : 0U,
                  qBefore,
                  txQueueCount);
    if (grantNext) {
      (void)sendTxQueueHead();
    }
    markDisplayDirty();
  }
}

void handleTxRadio() {
#if !defined(APP_LORAWAN_TX) && !defined(APP_TX_USB_STREAM_TEST)
  if (txWaitingAck && timeReachedMs(txAckDeadlineMs)) {
    Serial.printf("TX ack timeout: dev=%u,seq=%u,q=%u/%u\r\n",
                  AppConfig::DEVICE_ID,
                  txWaitingAckSeq,
                  txQueueCount,
                  AppConfig::TX_QUEUE_PACKET_CAPACITY);
    txWaitingAck = false;
    txWaitingAckSeq = 0;
    markDisplayDirty();
  }

  size_t packetLength = 0;
  LoRaTransport::ReceiveMeta meta = {};
  if (!LoRaTransport::receivePacket(rxPacketBuffer,
                                    sizeof(rxPacketBuffer),
                                    packetLength,
                                    meta)) {
    return;
  }

  uint8_t frameType = 0;
  if (!Protocol::peekFrameType(rxPacketBuffer, packetLength, frameType) ||
      !Protocol::isControlFrameType(frameType)) {
    return;
  }

  Protocol::ControlFrame frame = {};
  const Protocol::DecodeError err =
      Protocol::decodeControlFrame(rxPacketBuffer, packetLength, frame);
  if (err != Protocol::DecodeError::None) {
    Serial.printf("TX control error: %s,len=%u\r\n",
                  Protocol::decodeErrorToString(err),
                  static_cast<unsigned>(packetLength));
    return;
  }

  handleTxControlFrame(frame);
#endif
}

void txLoop() {
  const uint32_t nowMs = millis();
#if !defined(APP_TX_USB_STREAM_TEST)
  updateGpsTimebase();
#endif
#if defined(APP_SENSOR_IIM42352) && !defined(APP_TX_USB_STREAM_TEST)
  if (!txRunArmed) {
    logGpsWaitStatus(nowMs);
  }
#endif
#if defined(APP_TX_USB_STREAM_TEST)
  handleTxSerial();
#else
  handleTxRadio();
#endif
  startTxCaptureIfDue();

  if (!txRunArmed || !txCaptureStarted) {
    updateTxButton(nowMs);
  }

  if (txUiMode == TxUiMode::Select &&
      (nowMs - lastUiInteractionMs) >= AppConfig::UI_SELECT_TIMEOUT_MS) {
    txUiMode = TxUiMode::Normal;
    candidateRateIndex = activeRateIndex;
    markDisplayDirty();
  }

  if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
    (void)sendCurrentBatch();
  }

#if defined(APP_LORAWAN_TX)
  processFifoSamples();
#else
  if (txCaptureStarted) {
    processFifoSamples();
  }
#endif
  refreshTxScreen(nowMs);
}

bool isDuplicateRxPacket(uint16_t devId, uint16_t seq) {
  const int index = p2pDeviceIndex(devId);
  if (index < 0) {
    return false;
  }

  return rxLastSeqValid[index] && rxLastSeq[index] == seq;
}

void noteRxPacketSeq(uint16_t devId, uint16_t seq) {
  const int index = p2pDeviceIndex(devId);
  if (index < 0) {
    return;
  }

  rxLastSeq[index] = seq;
  rxLastSeqValid[index] = true;
}

void resetRxPacketSeqs() {
  for (uint8_t i = 0; i < AppConfig::P2P_NODE_COUNT; ++i) {
    rxLastSeq[i] = 0;
    rxLastSeqValid[i] = false;
  }
}

void printDecodedPacket(const Protocol::DecodedPacketView& packet, bool duplicate) {
  Serial.printf("dev_id=%u,seq=%u,count=%u,fs=%.1f,actual_fs=%.3f,t0_us=%lu\r\n",
                packet.header.devId,
                packet.header.seq,
                packet.header.count,
                sampleRateHzFloat(packet.header.fsHzX10),
                actualSampleRateHzFloat(packet.header.fsHzX10),
                static_cast<unsigned long>(packet.header.t0Us));

  if (duplicate) {
    Serial.printf("RX duplicate ignored: dev=%u,seq=%u\r\n",
                  packet.header.devId,
                  packet.header.seq);
    return;
  }

  for (uint8_t i = 0; i < packet.header.count; ++i) {
    uint64_t timestampValue = packet.header.t0Us + sampleIntervalUsRounded(i, packet.header.fsHzX10);
    if (rxRunStarted && rxRunStartEpochUs != 0) {
      timestampValue = rxRunStartEpochUs +
                       static_cast<uint64_t>(packet.header.t0Us) +
                       sampleIntervalUsRounded(i, packet.header.fsHzX10);
    }
    const auto& s = packet.samples[i];
    Serial.printf("%u,%llu,%d,%d,%d\r\n",
                  packet.header.devId,
                  static_cast<unsigned long long>(timestampValue),
                  s.ax,
                  s.ay,
                  s.az);
  }
}

void armRxRun(uint64_t startEpochUs, uint16_t sampleRateX10Hz, uint32_t delayMs) {
  if (startEpochUs == 0 || sampleRateX10Hz == 0) {
    Serial.println("RX START_AT invalid");
    return;
  }

  size_t rateIndex = 0;
  if (!findRateIndex(sampleRateX10Hz, rateIndex)) {
    Serial.printf("RX START_AT unsupported fs_x10=%u\r\n", sampleRateX10Hz);
    return;
  }

  ++rxRunId;
  if (rxRunId == 0) {
    ++rxRunId;
  }
  rxRunArmed = true;
  rxRunStarted = false;
  rxRunStartEpochUs = startEpochUs;
  rxRunStartLocalUs = micros() + delayMs * 1000UL;
  rxRunFsHzX10 = sampleRateX10Hz;
  rxLastStartBroadcastMs = 0;
  rxPollIndex = 0;
  rxPollWaiting = false;
  rxPollTargetDevId = 0;
  rxPollBurstPackets = 0;
  resetRxPacketSeqs();

  Serial.printf("RX START_AT armed: run=%u,epoch_us=%llu,delay_ms=%lu,fs=%.1f,nodes=%u,burst=%u\r\n",
                rxRunId,
                static_cast<unsigned long long>(rxRunStartEpochUs),
                static_cast<unsigned long>(delayMs),
                sampleRateHzFloat(kRateProfiles[rateIndex].sampleRateX10Hz),
                AppConfig::P2P_NODE_COUNT,
                p2pBurstPacketsPerNode());
}

void handleRxSerialLine(char* line) {
  if (line == nullptr) {
    return;
  }

  char* command = strtok(line, " \t,");
  if (command == nullptr) {
    return;
  }

  if (strcmp(command, "START_AT") != 0) {
    Serial.printf("RX unknown serial command: %s\r\n", command);
    return;
  }

  char* epochText = strtok(nullptr, " \t,");
  char* rateText = strtok(nullptr, " \t,");
  char* delayText = strtok(nullptr, " \t,");

  if (epochText == nullptr || rateText == nullptr) {
    Serial.println("RX START_AT usage: START_AT epoch_us sample_rate_hz [delay_ms]");
    return;
  }

  const uint64_t startEpochUs = strtoull(epochText, nullptr, 10);
  const uint16_t sampleRateX10Hz = parseSampleRateX10(rateText);
  uint32_t delayMs = AppConfig::P2P_START_DEFAULT_DELAY_MS;
  if (delayText != nullptr) {
    const unsigned long parsed = strtoul(delayText, nullptr, 10);
    if (parsed > 0) {
      delayMs = static_cast<uint32_t>(parsed);
    }
  }

  armRxRun(startEpochUs, sampleRateX10Hz, delayMs);
}

void handleRxSerial() {
  while (Serial.available() > 0) {
    const char c = static_cast<char>(Serial.read());
    if (c == '\r') {
      continue;
    }

    if (c == '\n') {
      rxSerialLine[rxSerialLineLen] = '\0';
      handleRxSerialLine(rxSerialLine);
      rxSerialLineLen = 0;
      rxSerialLine[0] = '\0';
      continue;
    }

    if (rxSerialLineLen + 1 < sizeof(rxSerialLine)) {
      rxSerialLine[rxSerialLineLen++] = c;
    } else {
      rxSerialLineLen = 0;
      rxSerialLine[0] = '\0';
      Serial.println("RX serial command too long");
    }
  }
}

void broadcastStartAtIfDue() {
  if (!rxRunArmed || rxRunStarted) {
    return;
  }

  const uint32_t nowMs = millis();
  if (rxLastStartBroadcastMs != 0 &&
      nowMs - rxLastStartBroadcastMs < AppConfig::P2P_START_BROADCAST_INTERVAL_MS) {
    return;
  }

  uint32_t delayMs = 0;
  if (!timeReachedUs(rxRunStartLocalUs)) {
    delayMs = (rxRunStartLocalUs - micros() + 999UL) / 1000UL;
  }

  if (sendControlFrame(Protocol::FRAME_TYPE_START_AT,
                       0,
                       rxRunId,
                       rxRunStartEpochUs,
                       delayMs,
                       rxRunFsHzX10)) {
    rxLastStartBroadcastMs = nowMs;
    Serial.printf("RX START_AT broadcast: run=%u,delay_ms=%lu\r\n",
                  rxRunId,
                  static_cast<unsigned long>(delayMs));
  }
}

void startRxRunIfDue() {
  if (!rxRunArmed || rxRunStarted || !timeReachedUs(rxRunStartLocalUs)) {
    return;
  }

  rxRunStarted = true;
  rxPollIndex = 0;
  rxPollWaiting = false;
  rxPollBurstPackets = 0;
  Serial.printf("RX polling started: run=%u,epoch_us=%llu\r\n",
                rxRunId,
                static_cast<unsigned long long>(rxRunStartEpochUs));
}

void advanceRxPoll() {
  rxPollWaiting = false;
  rxPollTargetDevId = 0;
  rxPollBurstPackets = 0;
  rxPollIndex = static_cast<uint8_t>((rxPollIndex + 1U) % AppConfig::P2P_NODE_COUNT);
}

void continueRxBurstWait() {
  rxPollWaiting = true;
  rxPollDeadlineMs = millis() + AppConfig::P2P_POLL_RESPONSE_TIMEOUT_MS;
}

void sendNextPollIfReady() {
  if (!rxRunStarted || rxPollWaiting) {
    return;
  }

  rxPollTargetDevId = p2pDeviceIdAt(rxPollIndex);
  if (sendControlFrame(Protocol::FRAME_TYPE_POLL,
                       rxPollTargetDevId,
                       rxRunId,
                       rxRunStartEpochUs,
                       p2pBurstPacketsPerNode(),
                       rxRunFsHzX10)) {
    rxPollWaiting = true;
    rxPollDeadlineMs = millis() + AppConfig::P2P_POLL_RESPONSE_TIMEOUT_MS;
  } else {
    advanceRxPoll();
  }
}

void handleRxControlFrame(const Protocol::ControlFrame& frame) {
  if (!rxPollWaiting || frame.devId != rxPollTargetDevId) {
    return;
  }

  if (frame.frameType == Protocol::FRAME_TYPE_NO_DATA) {
    Serial.printf("RX no data: dev=%u\r\n", frame.devId);
    advanceRxPoll();
  }
}

void handleRxDataPacket(size_t packetLength, const LoRaTransport::ReceiveMeta& meta) {
  Protocol::DecodedPacketView packet = {};
  const Protocol::DecodeError err =
      Protocol::decodePacket(rxPacketBuffer, packetLength, packet);
  if (err != Protocol::DecodeError::None) {
    Serial.printf("RX packet error: %s, len=%u\r\n",
                  Protocol::decodeErrorToString(err),
                  static_cast<unsigned>(packetLength));
    if (err == Protocol::DecodeError::CrcMismatch) {
      Serial.printf("RX crc expected=0x%04X actual=0x%04X\r\n",
                    packet.expectedCrc,
                    packet.actualCrc);
    }
    return;
  }

  if (rxPollWaiting && packet.header.devId != rxPollTargetDevId) {
    Serial.printf("RX unexpected dev: got=%u,want=%u,seq=%u\r\n",
                  packet.header.devId,
                  rxPollTargetDevId,
                  packet.header.seq);
    return;
  }

  const bool duplicate = isDuplicateRxPacket(packet.header.devId, packet.header.seq);
  if (!duplicate) {
    noteRxPacketSeq(packet.header.devId, packet.header.seq);
  }

  Serial.printf("RX ok: len=%u,rssi=%d,snr=%.1f\r\n",
                static_cast<unsigned>(packetLength),
                meta.rssi,
                meta.snr);
  printDecodedPacket(packet, duplicate);

  const uint8_t receivedPacketsInBurst =
      static_cast<uint8_t>(rxPollBurstPackets + (duplicate ? 0U : 1U));
  const bool grantNext = Protocol::shouldGrantBurstContinuation(
      rxPollWaiting,
      duplicate,
      receivedPacketsInBurst,
      p2pBurstPacketsPerNode());
  (void)sendControlFrame(Protocol::FRAME_TYPE_ACK,
                         packet.header.devId,
                         packet.header.seq,
                         0,
                         grantNext ? 1UL : 0UL,
                         packet.header.fsHzX10);
  if (grantNext) {
    rxPollBurstPackets = receivedPacketsInBurst;
    Serial.printf("RX burst grant: dev=%u,seq=%u,slot=%u/%u\r\n",
                  packet.header.devId,
                  packet.header.seq,
                  rxPollBurstPackets,
                  p2pBurstPacketsPerNode());
    continueRxBurstWait();
  } else {
    advanceRxPoll();
  }
}

void rxLoop() {
  handleRxSerial();
  broadcastStartAtIfDue();
  startRxRunIfDue();

  if (rxRunArmed && !rxRunStarted) {
    delay(AppConfig::RX_RESTART_DELAY_MS);
    return;
  }

  if (!rxRunStarted) {
    delay(AppConfig::RX_RESTART_DELAY_MS);
    return;
  }

  if (rxPollWaiting && timeReachedMs(rxPollDeadlineMs)) {
    Serial.printf("RX poll timeout: dev=%u\r\n", rxPollTargetDevId);
    advanceRxPoll();
  }

  sendNextPollIfReady();

  size_t packetLength = 0;
  LoRaTransport::ReceiveMeta meta = {};
  if (!LoRaTransport::receivePacket(rxPacketBuffer,
                                    sizeof(rxPacketBuffer),
                                    packetLength,
                                    meta)) {
    const int16_t radioState = LoRaTransport::lastState();
    if (radioState != 0 && radioState != lastRxRadioStateLogged) {
      Serial.printf("RX radio event: %s (%d)\r\n",
                    LoRaTransport::stateToString(radioState),
                    radioState);
      lastRxRadioStateLogged = radioState;
    } else if (radioState == 0) {
      lastRxRadioStateLogged = 0;
    }
    delay(AppConfig::RX_RESTART_DELAY_MS);
    return;
  }

  lastRxRadioStateLogged = 0;

  uint8_t frameType = 0;
  if (!Protocol::peekFrameType(rxPacketBuffer, packetLength, frameType)) {
    Serial.printf("RX frame error: bad header,len=%u\r\n", static_cast<unsigned>(packetLength));
    return;
  }

  if (Protocol::isControlFrameType(frameType)) {
    Protocol::ControlFrame frame = {};
    const Protocol::DecodeError err =
        Protocol::decodeControlFrame(rxPacketBuffer, packetLength, frame);
    if (err != Protocol::DecodeError::None) {
      Serial.printf("RX control error: %s,len=%u\r\n",
                    Protocol::decodeErrorToString(err),
                    static_cast<unsigned>(packetLength));
      return;
    }
    handleRxControlFrame(frame);
    return;
  }

  handleRxDataPacket(packetLength, meta);
}

}  // namespace

void setup() {
  Serial.begin(AppConfig::SERIAL_BAUD);
  delay(1200);

  activeRateIndex = defaultRateIndex();
  candidateRateIndex = activeRateIndex;
  samplePeriodUs = samplePeriodUsForRate(activeRateProfile().sampleRateX10Hz);

  printRoleInfo();
  Serial.printf("DEV_ID=%u (0x%04X)\r\n", AppConfig::DEVICE_ID, AppConfig::DEVICE_ID);

#if defined(ROLE_TX)
  seedTxRandom();
  pinMode(PIN_USER_BUTTON, INPUT_PULLUP);
  Serial.printf("TX user_button=%d, oled_sda=%d, oled_scl=%d, oled_rst=%d\r\n",
                PIN_USER_BUTTON,
                PIN_OLED_SDA,
                PIN_OLED_SCL,
                PIN_OLED_RST);

  const uint32_t startupJitterMs = randomDelayMs(AppConfig::TX_STARTUP_JITTER_MAX_MS);
  Serial.printf("TX startup_jitter=%lu ms, packet_jitter_max=%lu ms\r\n",
                static_cast<unsigned long>(startupJitterMs),
                static_cast<unsigned long>(AppConfig::TX_PACKET_JITTER_MAX_MS));
  if (startupJitterMs > 0) {
    delay(startupJitterMs);
  }

#if defined(APP_SENSOR_IIM42352)
  (void)initDs3231();
#if !defined(APP_TX_USB_STREAM_TEST)
  (void)initGpsTimebase();
#endif
#endif

  if (!initAdxl()) {
    while (true) {
      delay(1000);
    }
  }
#endif

#if !defined(APP_TX_USB_STREAM_TEST)
  if (!initLoRa()) {
    while (true) {
      delay(1000);
    }
  }
#endif

#if defined(ROLE_TX)
  (void)initOled();
  showBootScreen();
#if !defined(APP_LORAWAN_TX) && !defined(APP_TX_USB_STREAM_TEST)
  if (!LoRaTransport::startReceive()) {
    Serial.printf("LoRa TX listen arm failed: %s (%d)\r\n",
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
  } else {
    Serial.println("LoRa TX control listen armed");
  }
#elif defined(APP_TX_USB_STREAM_TEST)
  Serial.println("TX USB stream test waiting for START_AT");
#endif
  Serial.printf("TX active_rate=%.1f Hz%s\r\n",
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()));
  markDisplayDirty();
  refreshTxScreen(millis());
#endif

#if defined(ROLE_RX)
  if (!LoRaTransport::startReceive()) {
    Serial.printf("LoRa RX arm failed: %s (%d)\r\n",
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
    while (true) {
      delay(1000);
    }
  }
  Serial.println("LoRa RX mode armed");
#endif
}

void loop() {
#if defined(ROLE_TX)
  txLoop();
#elif defined(ROLE_RX)
  rxLoop();
#endif
}
