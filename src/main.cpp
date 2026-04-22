#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>
#include <esp_system.h>

#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

#include "ADXL345Driver.h"
#include "board_pins.h"
#include "config.h"
#include "lora_transport.h"
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

uint16_t packetSeq = 0;
uint32_t samplePeriodUs = 10000000UL / AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
uint32_t currentBatchStartUs = 0;
uint8_t pendingSamples = 0;
int16_t lastRxRadioStateLogged = 0;

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

uint32_t randomDelayMs(uint32_t maxDelayMs) {
  if (maxDelayMs == 0) {
    return 0;
  }
  return static_cast<uint32_t>(random(static_cast<long>(maxDelayMs) + 1L));
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
  Serial.println("ROLE=TX");
#elif defined(ROLE_RX)
  Serial.println("ROLE=RX");
#endif
}

void clearPendingBatch() {
  pendingSamples = 0;
  currentBatchStartUs = 0;
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
    oled.printf("BUF %u/%u\r\n", pendingSamples, AppConfig::MAX_SAMPLES_PER_PACKET);
    if (overloadActive()) {
      oled.printf("OVERLOAD\r\n");
    } else {
      oled.printf("RADIO READY\r\n");
    }
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
  oled.printf("LoRa %.1f MHz\r\n", AppConfig::LORA_FREQUENCY_MHZ);
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
  if (!LoRaTransport::begin()) {
    Serial.println("LoRa init failed");
    return false;
  }

  Serial.println("LoRa init ok");
  LoRaTransport::printConfig(Serial);
  return true;
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
  const uint32_t batchStartMs = (currentBatchStartUs + 500UL) / 1000UL;
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

  const uint32_t packetJitterMs = randomDelayMs(AppConfig::TX_PACKET_JITTER_MAX_MS);
  if (packetJitterMs > 0) {
    delay(packetJitterMs);
  }

  if (!LoRaTransport::sendPacket(txPacketBuffer, packetLength)) {
    Serial.println("TX LoRa send failed");
    return false;
  }

  Serial.printf("TX dev=%u,seq=%u,count=%u,fs=%.1f%s,t0=%lu,len=%u,jitter=%lu\r\n",
                AppConfig::DEVICE_ID,
                packetSeq,
                pendingSamples,
                sampleRateHzFloat(activeRateProfile().sampleRateX10Hz),
                experimentalTag(activeRateProfile()),
                static_cast<unsigned long>(batchStartMs),
                static_cast<unsigned>(packetLength),
                static_cast<unsigned long>(packetJitterMs));
  ++packetSeq;
  clearPendingBatch();
  markDisplayDirty();
  delay(AppConfig::TX_IDLE_DELAY_MS);
  return true;
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

void txLoop() {
  const uint32_t nowMs = millis();
  updateTxButton(nowMs);

  if (txUiMode == TxUiMode::Select &&
      (nowMs - lastUiInteractionMs) >= AppConfig::UI_SELECT_TIMEOUT_MS) {
    txUiMode = TxUiMode::Normal;
    candidateRateIndex = activeRateIndex;
    markDisplayDirty();
  }

  if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
    (void)sendCurrentBatch();
  }

  processFifoSamples();
  refreshTxScreen(nowMs);
}

void printDecodedPacket(const Protocol::DecodedPacketView& packet) {
  Serial.printf("dev_id=%u,seq=%u,count=%u,fs=%.1f,t0=%lu\r\n",
                packet.header.devId,
                packet.header.seq,
                packet.header.count,
                sampleRateHzFloat(packet.header.fsHzX10),
                static_cast<unsigned long>(packet.header.t0Ms));

  for (uint8_t i = 0; i < packet.header.count; ++i) {
    const uint32_t timestampMs =
        packet.header.t0Ms + sampleIntervalMsRounded(i, packet.header.fsHzX10);
    const auto& s = packet.samples[i];
    Serial.printf("%u,%lu,%d,%d,%d\r\n",
                  packet.header.devId,
                  static_cast<unsigned long>(timestampMs),
                  s.ax,
                  s.ay,
                  s.az);
  }
}

void rxLoop() {
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

  Serial.printf("RX ok: len=%u,rssi=%d,snr=%.1f\r\n",
                static_cast<unsigned>(packetLength),
                meta.rssi,
                meta.snr);
  printDecodedPacket(packet);
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
