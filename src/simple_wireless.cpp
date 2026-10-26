#if defined(APP_SIMPLE_WIRELESS)
#include <Arduino.h>
#include <SPI.h>
#include <esp_timer.h>
#include "board_pins.h"
#include "config.h"
#include "lora_transport.h"
#include "simple_wireless_protocol.h"
#if defined(ROLE_TX)
#include "IIM42352Driver.h"
#endif

namespace {
namespace Link = SimpleWireless;
constexpr uint32_t kRetryMs = 2000;
bool radioReady = false;
uint32_t lastRadioAttemptMs = 0;
uint8_t packetBuffer[Link::MAX_PACKET_SIZE] = {};

bool initRadio() {
  lastRadioAttemptMs = millis();
  radioReady = LoRaTransport::begin();
#if defined(ROLE_RX)
  if (radioReady) radioReady = LoRaTransport::startReceive();
#endif
  if (!radioReady) {
    Serial.printf("RADIO_ERROR state=%s (%d); retry in 2s\r\n",
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
  } else {
    Serial.println("RADIO_READY");
    LoRaTransport::printConfig(Serial);
  }
  return radioReady;
}

#if defined(ROLE_TX)
SPIClass sensorSpi(HSPI);
IIM42352Driver sensor(sensorSpi, PIN_IIM42352_SCK, PIN_IIM42352_MISO,
                     PIN_IIM42352_MOSI, PIN_IIM42352_CS, AppConfig::IIM42352_SPI_CLOCK_HZ);
bool sensorReady = false;
uint32_t lastSensorAttemptMs = 0;
uint32_t lastSampleMs = 0;
uint16_t sequence = 0;
Link::Sample samples[Link::SAMPLES_PER_PACKET] = {};
IIM42352Driver::RawSample fifo[Link::SAMPLES_PER_PACKET] = {};
uint8_t pending = 0;
uint64_t firstSampleUs = 0, nextSampleUs = 0;
bool timelineValid = false;

void sendPacket(Link::Status status, uint8_t who) {
  size_t length = 0;
  const uint8_t count = status == Link::Status::Data ? pending : 0;
  const uint16_t seq = sequence++;
  if (!Link::encode(status, who, seq, firstSampleUs, samples, count,
                    packetBuffer, sizeof(packetBuffer), length)) {
    Serial.println("TX_ENCODE_ERROR");
    return;
  }
  if (!LoRaTransport::sendPacket(packetBuffer, length)) {
    Serial.printf("TX_SEND_FAILED seq=%u state=%s (%d)\r\n", seq,
                  LoRaTransport::stateToString(LoRaTransport::lastState()),
                  LoRaTransport::lastState());
    radioReady = false;
    lastRadioAttemptMs = millis();
    return;
  }
  Serial.printf("TX_SENT dev=1001 seq=%u samples=%u status=%u\r\n", seq, count,
                static_cast<unsigned>(status));
}

void initSensor() {
  lastSensorAttemptMs = millis();
  pending = 0;
  timelineValid = false;
  sensorReady = sensor.begin(false);  // Internal clock; no DS3231 or GNSS needed.
  if (sensorReady) {
    sensor.setRange(IIM42352Driver::Range::Range4G);
    sensor.setDataRate(IIM42352Driver::DataRate::Hz50);
    sensorReady = sensor.rangeConfigured(IIM42352Driver::Range::Range4G) &&
                  sensor.internalClockConfigured() && sensor.checkDeviceId();
  }
  if (!sensorReady) {
    Serial.printf("SENSOR_ERROR who=0x%02X expected=0x6D; retry in 2s\r\n", sensor.lastDeviceId());
    if (radioReady) sendPacket(Link::Status::SensorInitFailed, sensor.lastDeviceId());
    return;
  }
  // Discard startup/rate-transition samples, with a bounded drain.
  for (unsigned i = 0; i < 32; ++i) {
    if (sensor.readFifoBurst(fifo, Link::SAMPLES_PER_PACKET) < Link::SAMPLES_PER_PACKET) break;
  }
  lastSampleMs = millis();
  Serial.println("SENSOR_READY who=0x6D clock=internal sample_hz=50 range_g=4");
}

void transmitSamples() {
  if (!sensorReady) {
    if (static_cast<uint32_t>(millis() - lastSensorAttemptMs) >= kRetryMs) initSensor();
    return;
  }
  const uint8_t available = sensor.fifoAvailable();
  // A full FIFO makes the old reconstructed sample timeline unreliable.
  if (available >= 250) {
    Serial.println("SENSOR_FIFO_FULL; restarting acquisition");
    sendPacket(Link::Status::SensorStalled, sensor.readRegister(0x75));
    sensorReady = false;
    pending = 0;
    lastSensorAttemptMs = millis();
    return;
  }
  if (!available) {
    if (static_cast<uint32_t>(millis() - lastSampleMs) >= kRetryMs) {
      Serial.println("SENSOR_STALLED; retry in 2s");
      sendPacket(Link::Status::SensorStalled, sensor.readRegister(0x75));
      sensorReady = false;
      pending = 0;
      lastSensorAttemptMs = millis();
    }
    return;
  }
  const uint64_t observedUs = static_cast<uint64_t>(esp_timer_get_time());
  const size_t count = sensor.readFifoBurst(fifo, Link::SAMPLES_PER_PACKET - pending);
  if (!count) {
    if (static_cast<uint32_t>(millis() - lastSampleMs) >= kRetryMs) {
      sendPacket(Link::Status::SensorStalled, sensor.readRegister(0x75));
      sensorReady = false;
      pending = 0;
      lastSensorAttemptMs = millis();
    }
    return;
  }
  lastSampleMs = millis();
  if (!timelineValid) {
    const uint64_t bufferedUs = static_cast<uint64_t>(available - 1) * Link::SAMPLE_PERIOD_US;
    nextSampleUs = observedUs >= bufferedUs ? observedUs - bufferedUs : 0;
    timelineValid = true;
  }
  for (size_t i = 0; i < count; ++i) {
    if (!pending) firstSampleUs = nextSampleUs;
    samples[pending++] = {fifo[i].x, fifo[i].y, fifo[i].z};
    nextSampleUs += Link::SAMPLE_PERIOD_US;
  }
  if (pending == Link::SAMPLES_PER_PACKET) {
    sendPacket(Link::Status::Data, sensor.lastDeviceId());
    pending = 0;
  }
}
#else
uint32_t lastReceiveMs = 0;
int16_t lastError = 0;
uint16_t lastSequence = 0;
uint64_t lastFirstSampleUs = 0;
bool haveLastPacket = false;
char command[96] = {};
size_t commandLength = 0;
void receiveSamples() {
  // Consume old GUI START_AT commands without changing this auto-start link.
  for (unsigned n = 0; n < 128 && Serial.available(); ++n) {
    const char ch = static_cast<char>(Serial.read());
    if (ch == '\n') {
      if (commandLength) Serial.println("RX_INFO auto-start; fixed 50Hz, +/-4g; START_AT is not required");
      commandLength = 0;
    } else if (ch != '\r' && commandLength + 1 < sizeof(command)) {
      command[commandLength++] = ch;
    }
  }
  size_t length = 0;
  LoRaTransport::ReceiveMeta meta = {};
  if (!LoRaTransport::receivePacket(packetBuffer, sizeof(packetBuffer), length, meta)) {
    const int16_t state = LoRaTransport::lastState();
    if (state && state != lastError) Serial.printf("RX_RADIO_ERROR state=%s (%d)\r\n", LoRaTransport::stateToString(state), state);
    lastError = state;
    if (static_cast<uint32_t>(millis() - lastReceiveMs) >= 3000) {
      Serial.println("RX_WAITING dev=1001");
      lastReceiveMs = millis();
    }
    return;
  }
  lastError = 0;
  Link::Packet packet = {};
  if (!Link::decode(packetBuffer, length, packet)) {
    Serial.println("RX_INVALID_PACKET; use simple TX and simple RX firmware together");
    return;
  }
  lastReceiveMs = millis();
  const auto& h = packet.header;
  if (h.status != static_cast<uint8_t>(Link::Status::Data)) {
    Serial.printf("RX_SENSOR_ERROR dev=%u reason=%s who=0x%02X rssi=%d\r\n", h.deviceId,
                  h.status == static_cast<uint8_t>(Link::Status::SensorInitFailed) ? "init_failed" : "stalled",
                  h.sensorWho, meta.rssi);
    return;
  }
  if (haveLastPacket && h.seq == lastSequence && h.firstSampleUs == lastFirstSampleUs) return;
  haveLastPacket = true; lastSequence = h.seq; lastFirstSampleUs = h.firstSampleUs;
  Serial.printf("RX_DATA dev=%u seq=%u samples=%u rssi=%d snr=%.1f\r\n", h.deviceId, h.seq, h.count, meta.rssi, meta.snr);
  for (uint8_t i = 0; i < h.count; ++i) {
    const uint64_t sampleMs = (h.firstSampleUs + static_cast<uint64_t>(i) * h.samplePeriodUs) / 1000;
    const auto& sample = packet.samples[i];
    Serial.printf("%u,%llu,%d,%d,%d\r\n", h.deviceId,
                  static_cast<unsigned long long>(sampleMs), sample.ax, sample.ay, sample.az);
  }
}
#endif
}  // namespace

void setup() {
  Serial.begin(460800);
  delay(1200);
  pinMode(PIN_VEXT_CTRL, OUTPUT);
  digitalWrite(PIN_VEXT_CTRL, LOW);
  pinMode(PIN_IIM42352_CS, OUTPUT);
  digitalWrite(PIN_IIM42352_CS, HIGH);
  delay(100);
#if defined(ROLE_TX)
  Serial.println("ROLE=SIMPLE_TX dev=1001 sample_hz=50 range_g=4 clock=internal");
#else
  Serial.println("ROLE=SIMPLE_RX dev=1001 serial_baud=460800 timebase=TX_LOCAL_MS");
#endif
  initRadio();
#if defined(ROLE_TX)
  initSensor();
#endif
}
void loop() {
  if (!radioReady) {
    if (static_cast<uint32_t>(millis() - lastRadioAttemptMs) >= kRetryMs) initRadio();
    delay(1);
    return;
  }
#if defined(ROLE_TX)
  transmitSamples();
#else
  receiveSamples();
#endif
  delay(1);
}
#endif  // APP_SIMPLE_WIRELESS
