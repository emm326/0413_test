#pragma once
#include "sensor/IimBase.h"
class Iim42652 final : public IimBase {
 public:
  Iim42652();

 private:
  uint8_t standbyPower() const override { return 0x10; }
  bool configureInterface() override;
  void configureFifo() override;
};
