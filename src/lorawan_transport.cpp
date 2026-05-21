#include "lorawan_transport.h"

#include <Arduino.h>
#include <RadioLib.h>
#include <SPI.h>

#include "board_pins.h"
#include "config.h"

#if defined(APP_LORAWAN_TX)

namespace LoRaWanTransport {

namespace {

SX1262 radio = new Module(PIN_LORA_NSS, PIN_LORA_DIO1, PIN_LORA_RST, PIN_LORA_BUSY);
LoRaWANNode node(&radio, &CN470, AppConfig::LORAWAN_SUB_BAND);
int16_t lastRadioState = RADIOLIB_ERR_NONE;
UplinkMeta lastMeta = {};

}  // namespace

bool begin() {
  SPI.begin(PIN_LORA_SCK, PIN_LORA_MISO, PIN_LORA_MOSI, PIN_LORA_NSS);
  pinMode(PIN_LORA_DIO1, INPUT);

  int16_t state = radio.begin();
  lastRadioState = state;
  if (state != RADIOLIB_ERR_NONE) {
    return false;
  }

  state = node.beginABP(AppConfig::LORAWAN_DEV_ADDR,
                        AppConfig::LORAWAN_FNWKSINT_KEY,
                        AppConfig::LORAWAN_SNWKSINT_KEY,
                        AppConfig::LORAWAN_NWKSENC_KEY,
                        AppConfig::LORAWAN_APPS_KEY);
  lastRadioState = state;
  if (state != RADIOLIB_ERR_NONE) {
    return false;
  }

  state = node.activateABP();
  lastRadioState = state;
  if (state != RADIOLIB_ERR_NONE &&
      state != RADIOLIB_LORAWAN_NEW_SESSION &&
      state != RADIOLIB_LORAWAN_SESSION_RESTORED) {
    return false;
  }

  node.setADR(AppConfig::LORAWAN_ADR_ENABLED);
  node.setDutyCycle(AppConfig::LORAWAN_DUTY_CYCLE_ENABLED);

  state = node.setDatarate(AppConfig::LORAWAN_DATARATE);
  lastRadioState = state;
  if (state != RADIOLIB_ERR_NONE) {
    return false;
  }

  lastRadioState = RADIOLIB_ERR_NONE;
  return true;
}

bool sendUplink(const uint8_t* data, size_t length) {
  if (data == nullptr || length == 0) {
    lastRadioState = RADIOLIB_ERR_NULL_POINTER;
    return false;
  }

  LoRaWANEvent_t uplink = {};
  uint8_t downlink[1] = {};
  size_t downlinkLength = 0;
  const int16_t state = node.sendReceive(data,
                                         length,
                                         AppConfig::LORAWAN_FPORT,
                                         downlink,
                                         &downlinkLength,
                                         AppConfig::LORAWAN_CONFIRMED_UPLINK,
                                         &uplink,
                                         nullptr);
  lastRadioState = state;
  if (state < RADIOLIB_ERR_NONE) {
    return false;
  }

  lastMeta.frameCounter = uplink.fCnt;
  lastMeta.dataRate = uplink.datarate;
  lastMeta.frequencyMHz = uplink.freq;
  lastMeta.timeOnAirMs = static_cast<uint32_t>(node.getLastToA());
  return true;
}

int16_t lastState() { return lastRadioState; }

const UplinkMeta& lastUplinkMeta() { return lastMeta; }

const char* stateToString(int16_t state) {
  switch (state) {
    case RADIOLIB_ERR_NONE:
      return "ok/no downlink";
    case RADIOLIB_LORAWAN_NEW_SESSION:
      return "new session";
    case RADIOLIB_LORAWAN_SESSION_RESTORED:
      return "session restored";
    case RADIOLIB_ERR_CHIP_NOT_FOUND:
      return "chip not found";
    case RADIOLIB_ERR_PACKET_TOO_LONG:
      return "packet too long";
    case RADIOLIB_ERR_INVALID_DATA_RATE:
      return "invalid data rate";
    case RADIOLIB_ERR_INVALID_FREQUENCY:
      return "invalid frequency";
    case RADIOLIB_ERR_INVALID_OUTPUT_POWER:
      return "invalid output power";
    case RADIOLIB_ERR_NETWORK_NOT_JOINED:
      return "network not joined";
    case RADIOLIB_ERR_INVALID_PORT:
      return "invalid fport";
    case RADIOLIB_ERR_UPLINK_UNAVAILABLE:
      return "uplink unavailable";
    case RADIOLIB_ERR_DWELL_TIME_EXCEEDED:
      return "dwell time exceeded";
    case RADIOLIB_ERR_RX_TIMEOUT:
      return "rx timeout";
    case RADIOLIB_ERR_NULL_POINTER:
      return "null pointer";
    default:
      return "unknown";
  }
}

void printConfig(Stream& stream) {
  stream.printf("LoRaWAN region=CN470, subband=%u, dr=%u, fport=%u, devaddr=0x%08lX\r\n",
                AppConfig::LORAWAN_SUB_BAND,
                AppConfig::LORAWAN_DATARATE,
                AppConfig::LORAWAN_FPORT,
                static_cast<unsigned long>(AppConfig::LORAWAN_DEV_ADDR));
  stream.printf("LoRaWAN confirmed=%s, adr=%s, duty_cycle=%s\r\n",
                AppConfig::LORAWAN_CONFIRMED_UPLINK ? "on" : "off",
                AppConfig::LORAWAN_ADR_ENABLED ? "on" : "off",
                AppConfig::LORAWAN_DUTY_CYCLE_ENABLED ? "on" : "off");
}

}  // namespace LoRaWanTransport

#endif  // defined(APP_LORAWAN_TX)
