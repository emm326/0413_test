#pragma once
#include <stdint.h>
#include "protocol/Wire.h"

namespace SensorChecks {
struct StartupCheck {
  uint32_t dataReadyEdges;
  uint16_t fifoSamples;
  uint64_t clockEdges;
  bool fifoCountValid;
  bool sampleValid;
  bool identityValid;
};
constexpr bool startupPassed(const StartupCheck& check) {
  return check.dataReadyEdges >= 4 && check.dataReadyEdges <= 30 &&
         check.fifoSamples >= 4 && check.fifoSamples <= 30 &&
         check.clockEdges >= 3000 && check.clockEdges <= 4700 &&
         check.fifoCountValid && check.sampleValid && check.identityValid;
}
constexpr bool canRetry(WireProtocol::State state, bool sensorActive, uint16_t queued) {
  return (state == WireProtocol::Waiting || state == WireProtocol::Fault) &&
         !sensorActive && queued == 0;
}
}  // namespace SensorChecks
