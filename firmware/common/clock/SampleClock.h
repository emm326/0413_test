#pragma once
#include <Arduino.h>
class SampleClock {
 public:
  bool begin(int pin);
  bool setOutputEnabled(bool enabled);
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
  int outputPin_ = -1;
  static void IRAM_ATTR onOverflow(void*);
  static uint64_t IRAM_ATTR edges();
  uint32_t consumed_ = 0, seconds_ = 0;
  uint64_t firstEdges_ = 0, lastLocal_ = 0;
  float measuredHz_ = 0, ppm_ = 0;
  bool measured_ = false;
};
