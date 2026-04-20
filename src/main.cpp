#include <Arduino.h>
#include <SPI.h>
#include <Wire.h>

#include "ADXL345Driver.h"
#include "board_pins.h"
#include "config.h"
#include "lora_transport.h"
#include "protocol.h"

namespace {

TwoWire& sensorWire = Wire;

ADXL345Driver adxl(sensorWire,
                   ADXL345_I2C_ADDRESS,
                   PIN_ADXL345_SDA,
                   PIN_ADXL345_SCL,
                   AppConfig::ADXL345_I2C_CLOCK_HZ);

Protocol::AccelRawSample sampleBuffer[AppConfig::MAX_SAMPLES_PER_PACKET] = {};
uint8_t txPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint8_t rxPacketBuffer[Protocol::MAX_PACKET_SIZE] = {};
uint16_t packetSeq = 0;
uint32_t samplePeriodMs = 10000UL / AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
uint32_t nextSampleMs = 0;
uint32_t currentBatchStartMs = 0;
uint8_t pendingSamples = 0;
int16_t lastRxRadioStateLogged = 0;

float sampleRateHzFloat(uint16_t sampleRateX10Hz) { return sampleRateX10Hz / 10.0f; }

ADXL345Driver::DataRate mapSampleRateToAdxl(uint16_t sampleRateX10Hz) {
  switch (sampleRateX10Hz) {
    case 125:
      return ADXL345Driver::DataRate::Hz12_5;
    case 250:
      return ADXL345Driver::DataRate::Hz25;
    case 500:
      return ADXL345Driver::DataRate::Hz50;
    case 1000:
      return ADXL345Driver::DataRate::Hz100;
    case 2000:
      return ADXL345Driver::DataRate::Hz200;
    default:
      return ADXL345Driver::DataRate::Hz100;
  }
}

void printRoleInfo() {
#if defined(ROLE_TX)
  Serial.println("ROLE=TX");
#elif defined(ROLE_RX)
  Serial.println("ROLE=RX");
#endif
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
  adxl.setDataRate(mapSampleRateToAdxl(AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ));

  Serial.println("ADXL345 init ok");
  Serial.printf("ADXL345 devid=0x%02X\r\n", adxl.readRegister(0x00));
  Serial.println("ADXL345 range=+-4g, full-resolution=on");
  Serial.printf("ADXL345 sample_rate=%.1f Hz\r\n",
                sampleRateHzFloat(AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ));
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

bool sendCurrentBatch() {
  if (pendingSamples == 0) {
    return true;
  }

  size_t packetLength = 0;
  const bool ok =
      Protocol::encodePacket(AppConfig::DEVICE_ID,
                             packetSeq,
                             currentBatchStartMs,
                             AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ,
                             sampleBuffer,
                             pendingSamples,
                             txPacketBuffer,
                             sizeof(txPacketBuffer),
                             packetLength);
  if (!ok) {
    Serial.println("TX encode failed");
    return false;
  }

  if (!LoRaTransport::sendPacket(txPacketBuffer, packetLength)) {
    Serial.println("TX LoRa send failed");
    return false;
  }

  Serial.printf("TX seq=%u,count=%u,fs=%.1f,t0=%lu,len=%u\r\n",
                packetSeq,
                pendingSamples,
                sampleRateHzFloat(AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ),
                static_cast<unsigned long>(currentBatchStartMs),
                static_cast<unsigned>(packetLength));
  ++packetSeq;
  pendingSamples = 0;
  currentBatchStartMs = 0;
  delay(AppConfig::TX_IDLE_DELAY_MS);
  return true;
}

void txLoop() {
  const uint32_t now = millis();
  if (static_cast<int32_t>(now - nextSampleMs) < 0) {
    return;
  }

  nextSampleMs = now + samplePeriodMs;

  int16_t x = 0;
  int16_t y = 0;
  int16_t z = 0;
  if (!adxl.readXYZRaw(x, y, z)) {
    Serial.println("ADXL345 readXYZRaw failed");
    return;
  }

  if (pendingSamples == 0) {
    currentBatchStartMs = now;
  }
  sampleBuffer[pendingSamples++] = {x, y, z};

  if (pendingSamples >= AppConfig::MAX_SAMPLES_PER_PACKET) {
    sendCurrentBatch();
  }
}

void printDecodedPacket(const Protocol::DecodedPacketView& packet) {
  Serial.printf("seq=%u,count=%u,fs=%.1f,t0=%lu\r\n",
                packet.header.seq,
                packet.header.count,
                sampleRateHzFloat(packet.header.fsHzX10),
                static_cast<unsigned long>(packet.header.t0Ms));

  const uint32_t dtMs = 10000UL / packet.header.fsHzX10;
  for (uint8_t i = 0; i < packet.header.count; ++i) {
    const uint32_t timestampMs = packet.header.t0Ms + static_cast<uint32_t>(i) * dtMs;
    const auto& s = packet.samples[i];
    Serial.printf("%lu,%d,%d,%d\r\n",
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

  printRoleInfo();
  Serial.printf("DEV_ID=0x%04X\r\n", AppConfig::DEVICE_ID);

#if defined(ROLE_TX)
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

  samplePeriodMs = 10000UL / AppConfig::DEFAULT_SAMPLE_RATE_X10_HZ;
  nextSampleMs = millis();
}

void loop() {
#if defined(ROLE_TX)
  txLoop();
#elif defined(ROLE_RX)
  rxLoop();
#endif
}
