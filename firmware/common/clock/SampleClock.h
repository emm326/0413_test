#pragma once
#include <Arduino.h>
class SampleClock {
 public:
  bool begin(int pin);
  bool setOutputEnabled(bool enabled);
  // Count edges for windowMs and check the result is inside [minEdges, maxEdges].
  // Used for boot self-test and runtime watchdog; an idle or floating pin fails.
  bool verifyOutput(uint32_t windowMs, uint32_t minEdges, uint32_t maxEdges);
  // Re-apply the LEDC route and output enable after a detected clock loss.
  bool recover();
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
  static bool attachOutput(int pin);
  int outputPin_ = -1;
  static void IRAM_ATTR onOverflow(void*);
  static uint64_t IRAM_ATTR edges();
  uint32_t consumed_ = 0, seconds_ = 0;
  uint64_t firstEdges_ = 0, lastLocal_ = 0;
  float measuredHz_ = 0, ppm_ = 0;
  bool measured_ = false;
};
