#pragma once
#include <stdint.h>
#ifndef APP_DEVICE_ID_VALUE
#error "Choose an explicit board/sensor environment"
#endif
#ifndef APP_UPLINK_MHZ
#define APP_UPLINK_MHZ 470.3f
#endif
#ifndef APP_FIRMWARE_VERSION
#define APP_FIRMWARE_VERSION "3.0.0"
#endif
namespace Config {
constexpr uint16_t nodeId = APP_DEVICE_ID_VALUE;
static_assert(nodeId >= 1001 && nodeId <= 1010, "Node ID must be 1001..1010");
constexpr uint32_t clockHz = 32000;
constexpr uint32_t serialBaud = 460800;
constexpr uint32_t gpsBaud = 115200;
constexpr float controlMHz = 470.3f;
constexpr float uplinkMHz = APP_UPLINK_MHZ;
constexpr float bandwidthKHz = 125.0f;
constexpr uint8_t spreadingFactor = 6;
constexpr uint8_t codingRate = 5;
constexpr uint8_t syncWord = 0x12;
constexpr uint16_t preamble = 12;
constexpr int8_t powerDbm = 14;
constexpr uint8_t maxSamples = 25;
constexpr uint16_t queueCapacity = 128;
constexpr uint32_t warmupUs = 100000;
}  // namespace Config
