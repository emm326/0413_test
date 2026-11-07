#pragma once
#include <stdint.h>
namespace IimRegister {
constexpr uint8_t REG_BANK_SEL = 0x76;
constexpr uint8_t WHO_AM_I = 0x75;
constexpr uint8_t DEVICE_CONFIG = 0x11;
constexpr uint8_t FIFO_CONFIG = 0x16;
constexpr uint8_t FIFO_CONFIG1 = 0x5f;
constexpr uint8_t FIFO_CONFIG2 = 0x60;
constexpr uint8_t FIFO_CONFIG3 = 0x61;
constexpr uint8_t INT_CONFIG = 0x14;
constexpr uint8_t INT_CONFIG1 = 0x64;
constexpr uint8_t INTF_CONFIG1 = 0x4d;
constexpr uint8_t INTF_CONFIG5 = 0x7b;
constexpr uint8_t INT_SOURCE0 = 0x65;
constexpr uint8_t ACCEL_CONFIG0 = 0x50;
constexpr uint8_t PWR_MGMT0 = 0x4e;
constexpr uint8_t SIGNAL_PATH_RESET = 0x4b;
constexpr uint8_t FIFO_COUNTH = 0x2e;
constexpr uint8_t FIFO_DATA = 0x30;
}  // namespace IimRegister
