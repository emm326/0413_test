#pragma once
#include <stddef.h>
#include <stdint.h>
namespace Radio {
bool begin();
bool channel(float frequency, uint8_t sf);
bool send(const uint8_t* bytes, size_t length);
bool receive(uint8_t* bytes, size_t& length, int16_t& rssi, float& snr);
void update();
bool transmitting();
int16_t lastError();
int16_t recoveryError();
}  // namespace Radio
