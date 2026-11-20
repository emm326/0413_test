#pragma once
#include <stdint.h>
#include "protocol/Wire.h"

namespace SensorChecks {
constexpr uint8_t sensorStartupMaxAttempts = 3;
constexpr uint32_t sensorStartupRetryDelayMs = 300;
constexpr uint64_t startupExternalClockMinEdges = 3000;
constexpr uint64_t startupExternalClockMaxEdges = 4700;
constexpr bool shouldRetrySensorStartup(uint8_t failedAttempt) {
  return failedAttempt < sensorStartupMaxAttempts;
}
struct StartupCheck {
  uint32_t dataReadyEdges;
  uint16_t fifoSamples;
  uint64_t clockEdges;
  bool fifoCountValid;
  bool sampleValid;
  bool identityValid;
};
// INT1 is optional in the six-wire setup, so a zero DRDY count is allowed.
// The upper bound still catches an interrupt storm. The startup window can
// include a short FIFO transient, so allow up to 64 samples (about 1.28 s at
// 50 Hz). FIFO count, a valid sample and the identity register are the
// authoritative startup evidence.
// External-clock mode additionally requires the 32 kHz board-side loopback.
constexpr bool startupPassed(const StartupCheck& check,
                             bool requireExternalClock = true) {
  return (!requireExternalClock ||
          (check.clockEdges >= startupExternalClockMinEdges &&
           check.clockEdges <= startupExternalClockMaxEdges)) &&
         check.dataReadyEdges <= 30 &&
         check.fifoSamples >= 4 && check.fifoSamples <= 64 &&
         check.fifoCountValid && check.sampleValid && check.identityValid;
}
// External CLKIN must be measured against PPS. In sensor-internal mode the
// oscillator is qualified by the sensor startup/FIFO checks instead, so a
// missing GPIO36 carrier is expected and must not block capture.
constexpr bool clockReadyForCapture(bool externalClock, bool externalMeasured,
                                    bool sensorReady) {
  return sensorReady && (!externalClock || externalMeasured);
}
constexpr bool canRetry(WireProtocol::State state, bool sensorActive, uint16_t queued) {
  return (state == WireProtocol::Waiting || state == WireProtocol::Fault) &&
         !sensorActive && queued == 0;
}
}  // namespace SensorChecks
