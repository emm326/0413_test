#pragma once

#include <Arduino.h>

#include "board_pins.h"

// Optional build flag compatibility:
// -D DEVICE_ID=1001
// -D LORA_FREQUENCY_MHZ=470.0f
#if defined(DEVICE_ID) && !defined(APP_DEVICE_ID_VALUE)
#define APP_DEVICE_ID_VALUE DEVICE_ID
#endif

#if defined(LORA_FREQUENCY_MHZ) && !defined(APP_LORA_FREQUENCY_MHZ_VALUE)
#define APP_LORA_FREQUENCY_MHZ_VALUE LORA_FREQUENCY_MHZ
#endif

#ifdef DEVICE_ID
#undef DEVICE_ID
#endif

#ifdef LORA_FREQUENCY_MHZ
#undef LORA_FREQUENCY_MHZ
#endif

#ifndef APP_DEVICE_ID_VALUE
#define APP_DEVICE_ID_VALUE 1001
#endif

#ifndef APP_LORA_FREQUENCY_MHZ_VALUE
#define APP_LORA_FREQUENCY_MHZ_VALUE 470.0f
#endif

#ifndef APP_TX_STARTUP_JITTER_MAX_MS
#define APP_TX_STARTUP_JITTER_MAX_MS 200UL
#endif

#ifndef APP_TX_PACKET_JITTER_MAX_MS
#define APP_TX_PACKET_JITTER_MAX_MS 5UL
#endif

#if defined(ROLE_TX) && defined(ROLE_RX)
#error "ROLE_TX and ROLE_RX cannot be defined at the same time"
#endif

#if !defined(ROLE_TX) && !defined(ROLE_RX)
#define ROLE_TX
#endif

namespace AppConfig {

constexpr uint32_t SERIAL_BAUD = 115200;
constexpr uint16_t DEVICE_ID = static_cast<uint16_t>(APP_DEVICE_ID_VALUE);
constexpr uint8_t MAX_SAMPLES_PER_PACKET = 20;
constexpr uint16_t DEFAULT_SAMPLE_RATE_X10_HZ = 1000;  // 100.0 Hz

// ADXL345 configuration
constexpr uint32_t ADXL345_I2C_CLOCK_HZ = 400000UL;
constexpr uint8_t ADXL345_FIFO_WATERMARK = 16;
constexpr uint8_t ADXL345_FIFO_OVERLOAD_LEVEL = 28;

// LoRa configuration tuned toward higher throughput / short-to-mid range
constexpr float LORA_FREQUENCY_MHZ = APP_LORA_FREQUENCY_MHZ_VALUE;
constexpr float LORA_BANDWIDTH_KHZ = 250.0f;
constexpr uint8_t LORA_SPREADING_FACTOR = 7;
constexpr uint8_t LORA_CODING_RATE = 5;  // 4/5
constexpr uint8_t LORA_SYNC_WORD = 0x12;
constexpr uint16_t LORA_PREAMBLE_LEN = 8;
constexpr int8_t LORA_TX_POWER_DBM = 14;

// Multi-node mitigation knobs
constexpr uint32_t TX_STARTUP_JITTER_MAX_MS = APP_TX_STARTUP_JITTER_MAX_MS;
constexpr uint32_t TX_PACKET_JITTER_MAX_MS = APP_TX_PACKET_JITTER_MAX_MS;

// OLED / button UI
constexpr uint8_t OLED_I2C_ADDR_PRIMARY = 0x3C;
constexpr uint8_t OLED_I2C_ADDR_FALLBACK = 0x3D;
constexpr uint32_t OLED_I2C_CLOCK_HZ = 400000UL;
constexpr uint8_t OLED_WIDTH = 128;
constexpr uint8_t OLED_HEIGHT = 64;
constexpr uint32_t UI_DEBOUNCE_MS = 30;
constexpr uint32_t UI_LONG_PRESS_MS = 800;
constexpr uint32_t UI_SELECT_TIMEOUT_MS = 5000;
constexpr uint32_t UI_REFRESH_MS = 200;

constexpr uint32_t TX_IDLE_DELAY_MS = 2;
constexpr uint32_t RX_RESTART_DELAY_MS = 5;

}  // namespace AppConfig
