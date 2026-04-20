#pragma once

#include <Arduino.h>

#include "board_pins.h"

// 角色宏：默认编译为 TX，若要接收端可在 build_flags 中定义 ROLE_RX
#if defined(ROLE_TX) && defined(ROLE_RX)
#error "ROLE_TX 和 ROLE_RX 不能同时定义"
#endif

#if !defined(ROLE_TX) && !defined(ROLE_RX)
#define ROLE_TX
#endif

namespace AppConfig {

constexpr uint32_t SERIAL_BAUD = 115200;
constexpr uint16_t DEVICE_ID = 0x1001;
constexpr uint8_t MAX_SAMPLES_PER_PACKET = 20;
constexpr uint16_t DEFAULT_SAMPLE_RATE_X10_HZ = 1000;  // 100.0 Hz

// ADXL345 配置
constexpr uint32_t ADXL345_I2C_CLOCK_HZ = 400000UL;
constexpr uint8_t ADXL345_FIFO_WATERMARK = 16;

// LoRa 配置：偏向较高吞吐、近中距离
constexpr float LORA_FREQUENCY_MHZ = 470.0f;
constexpr float LORA_BANDWIDTH_KHZ = 250.0f;
constexpr uint8_t LORA_SPREADING_FACTOR = 7;
constexpr uint8_t LORA_CODING_RATE = 5;      // 4/5
constexpr uint8_t LORA_SYNC_WORD = 0x12;
constexpr uint16_t LORA_PREAMBLE_LEN = 8;
constexpr int8_t LORA_TX_POWER_DBM = 14;

constexpr uint32_t TX_IDLE_DELAY_MS = 2;
constexpr uint32_t RX_RESTART_DELAY_MS = 5;

}  // namespace AppConfig
