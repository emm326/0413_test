#pragma once
#include "sensor/IimBase.h"
class Iim42652 final : public IimBase {
 public:
  Iim42652();

 private:
  void configureFifo() override;
};
