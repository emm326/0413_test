#pragma once
#include <Arduino.h>
class SampleClock {
 public:
  bool begin(int pin);
  bool setOutputEnabled(bool enabled);
  // Count edges for windowMs and check the result is inside [minEdges, maxEdges].
  // Used for boot self-test and runtime watchdog; an idle or floating pin fails.
  bool verifyOutput(uint32_t windowMs, uint32_t minEdges, uint32_t maxEdges);
  // Count edges with both PCNT and a direct GPIO poll, return the plausible one.
  // Startup checks use this so a marginal PCNT route cannot hide a live carrier.
  uint32_t measureEdges(uint32_t windowMs);
  // Rebuild the MCPWM carrier after a detected clock loss.
  bool recover();
  // True when a plain GPIO drive test proved the CLKIN pad can go high.
  bool padUsable() const {
    return padOk_;
  }
  void update(bool ppsHealthy);
  uint64_t edgeCount() const {
    return edges();
  }
  bool measured() const {
    return measured_;
  }
  bool healthy() const {
    return measured_ && abs(ppm_) <= 100.0f;
  }
  float hz() const {
    return measuredHz_;
  }
  float ppm() const {
    return ppm_;
  }
  static void IRAM_ATTR onPps(uint64_t localUs);

 private:
  static bool configureCounter(int pin);
  bool startCarrier(int pin);
  bool restartCarrier();
  uint32_t pollEdges(uint32_t windowUs);
  int outputPin_ = -1;
  bool padOk_ = false;
  static void IRAM_ATTR onOverflow(void*);
  static uint64_t IRAM_ATTR edges();
  uint32_t consumed_ = 0, seconds_ = 0;
  uint64_t firstEdges_ = 0, lastLocal_ = 0;
  float measuredHz_ = 0, ppm_ = 0;
  bool measured_ = false;
};
