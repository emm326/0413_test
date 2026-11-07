#pragma once
#include <stdint.h>
#ifndef APP_DEVICE_ID_VALUE
#error "Choose an explicit board/sensor environment"
#endif
#ifndef APP_NODE_ID
#define APP_NODE_ID APP_DEVICE_ID_VALUE
#endif
#ifndef APP_FIRMWARE_VERSION
#define APP_FIRMWARE_VERSION "3.0.0"
#endif
namespace Config {
constexpr uint16_t nodeId = APP_NODE_ID;
static_assert(nodeId >= 1001 && nodeId <= 1008, "APP_NODE_ID must be 1001..1008");
constexpr float uplinkMHzFor(uint16_t id) {
  return 470.3f + float(id - 1001) * 0.2f;
}
constexpr uint32_t clockHz = 32000;
constexpr uint32_t serialBaud = 460800;
constexpr uint32_t gpsBaud = 115200;
constexpr float controlMHz = 470.3f;
constexpr float uplinkMHz = uplinkMHzFor(nodeId);
constexpr float bandwidthKHz = 125.0f;
constexpr uint8_t spreadingFactor = 6;
constexpr uint8_t codingRate = 5;
constexpr uint8_t syncWord = 0x12;
constexpr uint16_t preamble = 12;
constexpr int8_t powerDbm = 14;
constexpr uint8_t maxSamples = 25;
constexpr uint16_t queueCapacity = 128;
constexpr uint32_t warmupUs = 100000;
constexpr uint32_t cycleUs = 5000000;
constexpr uint32_t normalUplinkEndUs = 4000000;
constexpr uint32_t downlinkEndUs = 4250000;
constexpr uint32_t responseEndUs = 4450000;
constexpr uint32_t responseDelayMs = 30;
}  // namespace Config
