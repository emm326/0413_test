#pragma once

#include <Arduino.h>

namespace LoRaTransport {

struct ReceiveMeta {
  int16_t rssi;
  float snr;
};

bool begin();
bool startReceive();
bool sendPacket(const uint8_t* data, size_t length);
bool receivePacket(uint8_t* buffer,
                   size_t capacity,
                   size_t& outLength,
                   ReceiveMeta& meta);
int16_t lastState();
const char* stateToString(int16_t state);
void printConfig(Stream& stream);

}  // namespace LoRaTransport
