#include "lora_transport.h"

#include <Arduino.h>
#include <RadioLib.h>
#include <SPI.h>

#include "board_pins.h"
#include "config.h"

namespace LoRaTransport {

namespace {

SX1262 radio = new Module(PIN_LORA_NSS, PIN_LORA_DIO1, PIN_LORA_RST, PIN_LORA_BUSY);
bool rxModeArmed = false;
int16_t lastRadioState = RADIOLIB_ERR_NONE;

bool armReceiveMode() {
  const int16_t state = radio.startReceive();
  lastRadioState = state;
  rxModeArmed = (state == RADIOLIB_ERR_NONE);
  return rxModeArmed;
}

}  // namespace

bool begin() {
  SPI.begin(PIN_LORA_SCK, PIN_LORA_MISO, PIN_LORA_MOSI, PIN_LORA_NSS);
  pinMode(PIN_LORA_DIO1, INPUT);

  const int16_t state =
      radio.begin(AppConfig::LORA_FREQUENCY_MHZ,
                  AppConfig::LORA_BANDWIDTH_KHZ,
                  AppConfig::LORA_SPREADING_FACTOR,
                  AppConfig::LORA_CODING_RATE,
                  AppConfig::LORA_SYNC_WORD,
                  AppConfig::LORA_TX_POWER_DBM,
                  AppConfig::LORA_PREAMBLE_LEN);

  if (state != RADIOLIB_ERR_NONE) {
    lastRadioState = state;
    return false;
  }

  const int16_t crcState = radio.setCRC(true);
  if (crcState != RADIOLIB_ERR_NONE) {
    lastRadioState = crcState;
    return false;
  }

  lastRadioState = RADIOLIB_ERR_NONE;
  rxModeArmed = false;
  return true;
}

bool startReceive() { return armReceiveMode(); }

bool sendPacket(const uint8_t* data, size_t length) {
  if (data == nullptr || length == 0) {
    lastRadioState = RADIOLIB_ERR_NULL_POINTER;
    return false;
  }

  const int16_t state = radio.transmit(data, length);
  lastRadioState = state;
  rxModeArmed = false;
  return state == RADIOLIB_ERR_NONE;
}

bool receivePacket(uint8_t* buffer,
                   size_t capacity,
                   size_t& outLength,
                   ReceiveMeta& meta) {
  outLength = 0;
  meta = {};
  if (buffer == nullptr || capacity == 0) {
    lastRadioState = RADIOLIB_ERR_NULL_POINTER;
    return false;
  }

  if (!rxModeArmed && !armReceiveMode()) {
    return false;
  }

  if (digitalRead(PIN_LORA_DIO1) == LOW) {
    lastRadioState = RADIOLIB_ERR_NONE;
    return false;
  }

  const size_t packetLength = radio.getPacketLength();
  if (packetLength == 0) {
    lastRadioState = RADIOLIB_ERR_PACKET_TOO_SHORT;
    (void)armReceiveMode();
    return false;
  }

  if (packetLength > capacity) {
    lastRadioState = RADIOLIB_ERR_PACKET_TOO_LONG;
    (void)armReceiveMode();
    return false;
  }

  const int16_t state = radio.readData(buffer, packetLength);
  lastRadioState = state;

  if (state != RADIOLIB_ERR_NONE) {
    (void)armReceiveMode();
    return false;
  }

  outLength = packetLength;
  meta.rssi = static_cast<int16_t>(radio.getRSSI());
  meta.snr = radio.getSNR();

  (void)armReceiveMode();
  return true;
}

int16_t lastState() { return lastRadioState; }

const char* stateToString(int16_t state) {
  switch (state) {
    case RADIOLIB_ERR_NONE:
      return "ok";
    case RADIOLIB_ERR_RX_TIMEOUT:
      return "rx timeout";
    case RADIOLIB_ERR_CRC_MISMATCH:
      return "crc mismatch";
    case RADIOLIB_ERR_LORA_HEADER_DAMAGED:
      return "header damaged";
    case RADIOLIB_ERR_PACKET_TOO_SHORT:
      return "packet too short";
    case RADIOLIB_ERR_PACKET_TOO_LONG:
      return "packet too long";
    case RADIOLIB_ERR_SPI_WRITE_FAILED:
      return "spi write failed";
    case RADIOLIB_ERR_SPI_CMD_TIMEOUT:
      return "spi cmd timeout";
    case RADIOLIB_ERR_SPI_CMD_INVALID:
      return "spi cmd invalid";
    case RADIOLIB_ERR_SPI_CMD_FAILED:
      return "spi cmd failed";
    case RADIOLIB_ERR_CHIP_NOT_FOUND:
      return "chip not found";
    case RADIOLIB_ERR_INVALID_FREQUENCY:
      return "invalid frequency";
    case RADIOLIB_ERR_INVALID_MODULATION_PARAMETERS:
      return "invalid modulation parameters";
    case RADIOLIB_ERR_INVALID_SYNC_WORD:
      return "invalid sync word";
    case RADIOLIB_ERR_NULL_POINTER:
      return "null pointer";
    default:
      return "unknown";
  }
}

void printConfig(Stream& stream) {
  stream.printf("LoRa freq=%.3f MHz, bw=%.1f kHz, sf=%u, cr=4/%u, tx_power=%d dBm\r\n",
                AppConfig::LORA_FREQUENCY_MHZ,
                AppConfig::LORA_BANDWIDTH_KHZ,
                AppConfig::LORA_SPREADING_FACTOR,
                AppConfig::LORA_CODING_RATE,
                AppConfig::LORA_TX_POWER_DBM);
}

}  // namespace LoRaTransport
