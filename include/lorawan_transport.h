#pragma once

#include <Arduino.h>

namespace LoRaWanTransport {

struct UplinkMeta {
  uint32_t frameCounter;
  uint8_t dataRate;
  float frequencyMHz;
  uint32_t timeOnAirMs;
};

bool begin();
bool sendUplink(const uint8_t* data, size_t length);
int16_t lastState();
const char* stateToString(int16_t state);
const UplinkMeta& lastUplinkMeta();
void printConfig(Stream& stream);

}  // namespace LoRaWanTransport
