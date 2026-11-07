#pragma once
#include <Arduino.h>
class SampleClock {
 public:
  bool begin(int pin);
  void update(bool ppsHealthy);
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
  static void IRAM_ATTR onOverflow(void*);
  static uint64_t IRAM_ATTR edges();
  uint32_t consumed_ = 0, seconds_ = 0;
  uint64_t firstEdges_ = 0, lastLocal_ = 0;
  float measuredHz_ = 0, ppm_ = 0;
  bool measured_ = false;
};
