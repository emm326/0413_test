#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>
#include <esp_system.h>
#include <stdlib.h>
#include <string.h>

#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

#include "ADXL345Driver.h"
#include "board_pins.h"
#include "config.h"
#include "lora_transport.h"
#if defined(APP_LORAWAN_TX)
#include "lorawan_transport.h"
#endif
#include "protocol.h"

namespace {

struct RateProfile {
  uint16_t sampleRateX10Hz;
  ADXL345Driver::DataRate adxlRate;
  bool experimental;
  const char* label;
};

enum class TxUiMode : uint8_t {
  Normal = 0,
  Select,
};

constexpr RateProfile kRateProfiles[] = {
    {250, ADXL345Driver::DataRate::Hz25, false, "25"},
    {500, ADXL345Driver::DataRate::Hz50, false, "50"},
    {1000, ADXL345Driver::DataRate::Hz100, false, "100"},
    {2000, ADXL345Driver::DataRate::Hz200, true, "200"},
    {4000, ADXL345Driver::DataRate::Hz400, true, "400"},
};
constexpr size_t kRateProfileCount = sizeof(kRateProfiles) / sizeof(kRateProfiles[0]);

TwoWire& sensorWire = Wire;
TwoWire displayWire(1);

ADXL345Driver adxl(sensorWire,
                   ADXL345_I2C_ADDRESS,
                   PIN_ADXL345_SDA,
                   PIN_ADXL345_SCL,
                   AppConfig::ADXL345_I2C_CLOCK_HZ);

Adafruit_SSD1306 oled(AppConfig::OLED_WIDTH, AppConfig::OLED_HEIGHT, &displayWire, PIN_OLED_RST);

Protocol::AccelRawSample sampleBuffer[AppConfig::MAX_SAMPLES_PER_PACKET] = {};
ADXL345Driver::RawSample fifoBurstBuffer[AppConfig::ADXL345_FIFO_WATERMARK] = {};
uint8_t txPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint8_t rxPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint8_t controlPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};

struct QueuedTxPacket {
  uint8_t data[Protocol::MAX_PACKET_SIZE];
  size_t length;
  uint16_t seq;
  uint32_t t0Ms;
  uint8_t count;
  uint8_t retries;
};

QueuedTxPacket txQueue[AppConfig::TX_QUEUE_PACKET_CAPACITY] = {};
uint8_t txQueueHead = 0;
uint8_t txQueueCount = 0;

uint16_t packetSeq = 0;
uint32_t samplePeriodUs = 10000000UL / AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
uint32_t currentBatchStartUs = 0;
uint8_t pendingSamples = 0;
int16_t lastRxRadioStateLogged = 0;
uint32_t queueOverflowCount = 0;

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

float sampleRateHzFloat(uint16_t sampleRateX10Hz) { return sampleRateX10Hz / 10.0f; }

uint32_t sampleIntervalMsRounded(uint8_t sampleIndex, uint16_t sampleRateX10Hz) {
  const uint32_t numerator = static_cast<uint32_t>(sampleIndex) * 10000UL + sampleRateX10Hz / 2U;
  return numerator / sampleRateX10Hz;
}

uint32_t sampleIntervalUsRounded(uint8_t sampleIndex, uint16_t sampleRateX10Hz) {
  const uint32_t numerator =
      static_cast<uint32_t>(sampleIndex) * 10000000UL + sampleRateX10Hz / 2U;
  return numerator / sampleRateX10Hz;
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
#else
  Serial.println("ROLE=TX");
#endif
#elif defined(ROLE_RX)
  Serial.println("ROLE=RX");
#endif
}

void clearPendingBatch() {
  pendingSamples = 0;
  currentBatchStartUs = 0;
}

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
                 uint32_t t0Ms,
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
  item.t0Ms = t0Ms;
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
    Serial.printf("ADXL345 fifo flushed: %u samples\r\n", static_cast<unsigned>(flushed));
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
  oled.printf("TX FIELD READY\r\n");
  oled.printf("DEV %u\r\n", AppConfig::DEVICE_ID);
  oled.printf("RATE %s Hz%s\r\n",
              activeRateProfile().label,
              experimentalTag(activeRateProfile()));
#if defined(APP_LORAWAN_TX)
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

bool initAdxl() {
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

  adxl.setRange(ADXL345Driver::Range::Range4G);
  adxl.setFullResolution(true);
  adxl.setDataRate(activeRateProfile().adxlRate);
  delay(5);
  discardSensorFifo(false);

  Serial.println("ADXL345 init ok");
  Serial.printf("ADXL345 devid=0x%02X\r\n", adxl.readRegister(0x00));
  Serial.println("ADXL345 range=+-4g, full-resolution=on");
  Serial.printf("ADXL345 sample_rate=%.1f Hz%s\r\n",
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()));
  return true;
}

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
  samplePeriodUs = 10000000UL / activeRateProfile().sampleRateX10Hz;

  adxl.setDataRate(activeRateProfile().adxlRate);
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
  const uint32_t batchStartMs = (currentBatchStartUs + 500UL) / 1000UL;
#else
  const uint32_t batchStartElapsedUs = currentBatchStartUs - txRunStartLocalUs;
  const uint32_t batchStartMs = (batchStartElapsedUs + 500UL) / 1000UL;
#endif
  const bool ok =
      Protocol::encodePacket(AppConfig::DEVICE_ID,
                             packetSeq,
                             batchStartMs,
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

#if defined(APP_LORAWAN_TX)
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
                static_cast<unsigned long>(batchStartMs),
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
                static_cast<unsigned long>(batchStartMs),
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
      pushTxQueue(txPacketBuffer, packetLength, packetSeq, batchStartMs, pendingSamples);
  if (queued) {
    Serial.printf("TX queued dev=%u,seq=%u,count=%u,fs=%.1f%s,t0_offset_ms=%lu,len=%u,q=%u/%u\r\n",
                  AppConfig::DEVICE_ID,
                  packetSeq,
                  pendingSamples,
                  sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                  experimentalTag(activeRateProfile()),
                  static_cast<unsigned long>(batchStartMs),
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
      fifoDepth >= AppConfig::ADXL345_FIFO_OVERLOAD_LEVEL) {
    noteOverload(fifoDepth);
  }

  const size_t burstCapacity =
      sizeof(fifoBurstBuffer) / sizeof(fifoBurstBuffer[0]);
  const size_t requested = min(static_cast<size_t>(fifoDepth), burstCapacity);
  const uint32_t burstEndUs = micros();
  const size_t actual = adxl.readFifoBurst(fifoBurstBuffer, requested);
  if (actual == 0) {
    Serial.println("ADXL345 readFifoBurst failed");
    return;
  }

  const uint32_t burstStartUs =
      burstEndUs - static_cast<uint32_t>((static_cast<uint64_t>(actual) - 1ULL) * samplePeriodUs);

  for (size_t i = 0; i < actual; ++i) {
    if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
      if (!sendCurrentBatch()) {
        return;
      }
    }

    const uint32_t sampleTimestampUs =
        burstStartUs + static_cast<uint32_t>(static_cast<uint64_t>(i) * samplePeriodUs);
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

  const bool newRun = !txRunArmed || frame.seq != txRunId ||
                      frame.epochUs != txRunStartEpochUs;
  txRunId = frame.seq;
  txRunStartEpochUs = frame.epochUs;
  txRunStartLocalUs = micros() + frame.param * 1000UL;
  txRunArmed = true;

  if (newRun) {
    txCaptureStarted = false;
    packetSeq = 0;
    clearPendingBatch();
    clearTxQueue();
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

void startTxCaptureIfDue() {
#if !defined(APP_LORAWAN_TX)
  if (!txRunArmed || txCaptureStarted || !timeReachedUs(txRunStartLocalUs)) {
    return;
  }

  discardSensorFifo(false);
  clearPendingBatch();
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
    Serial.printf("TX acked: dev=%u,seq=%u,q_before=%u\r\n",
                  AppConfig::DEVICE_ID,
                  frame.seq,
                  txQueueCount);
    popTxQueueFront();
    txWaitingAck = false;
    txWaitingAckSeq = 0;
    markDisplayDirty();
  }
}

void handleTxRadio() {
#if !defined(APP_LORAWAN_TX)
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
  handleTxRadio();
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
  Serial.printf("dev_id=%u,seq=%u,count=%u,fs=%.1f,t0=%lu\r\n",
                packet.header.devId,
                packet.header.seq,
                packet.header.count,
                sampleRateHzFloat(packet.header.fsHzX10),
                static_cast<unsigned long>(packet.header.t0Ms));

  if (duplicate) {
    Serial.printf("RX duplicate ignored: dev=%u,seq=%u\r\n",
                  packet.header.devId,
                  packet.header.seq);
    return;
  }

  for (uint8_t i = 0; i < packet.header.count; ++i) {
    uint64_t timestampValue = packet.header.t0Ms + sampleIntervalMsRounded(i, packet.header.fsHzX10);
    if (rxRunStarted && rxRunStartEpochUs != 0) {
      timestampValue = rxRunStartEpochUs +
                       static_cast<uint64_t>(packet.header.t0Ms) * 1000ULL +
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
  resetRxPacketSeqs();

  Serial.printf("RX START_AT armed: run=%u,epoch_us=%llu,delay_ms=%lu,fs=%.1f,nodes=%u\r\n",
                rxRunId,
                static_cast<unsigned long long>(rxRunStartEpochUs),
                static_cast<unsigned long>(delayMs),
                sampleRateHzFloat(kRateProfiles[rateIndex].sampleRateX10Hz),
                AppConfig::P2P_NODE_COUNT);
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
  Serial.printf("RX polling started: run=%u,epoch_us=%llu\r\n",
                rxRunId,
                static_cast<unsigned long long>(rxRunStartEpochUs));
}

void advanceRxPoll() {
  rxPollWaiting = false;
  rxPollTargetDevId = 0;
  rxPollIndex = static_cast<uint8_t>((rxPollIndex + 1U) % AppConfig::P2P_NODE_COUNT);
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
                       0,
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

  (void)sendControlFrame(Protocol::FRAME_TYPE_ACK,
                         packet.header.devId,
                         packet.header.seq,
                         0,
                         0,
                         packet.header.fsHzX10);
  advanceRxPoll();
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
  samplePeriodUs = 10000000UL / activeRateProfile().sampleRateX10Hz;

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

  if (!initAdxl()) {
    while (true) {
      delay(1000);
    }
  }
#endif

  if (!initLoRa()) {
    while (true) {
      delay(1000);
    }
  }

#if defined(ROLE_TX)
  (void)initOled();
  showBootScreen();
#if !defined(APP_LORAWAN_TX)
  if (!LoRaTransport::startReceive()) {
    Serial.printf("LoRa TX listen arm failed: %s (%d)\r\n",
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
  } else {
    Serial.println("LoRa TX control listen armed");
  }
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
