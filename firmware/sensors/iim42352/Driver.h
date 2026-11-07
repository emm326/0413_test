#pragma once
#include "sensor/IimBase.h"
class Iim42352 final : public IimBase {
 public:
  Iim42352();

 private:
  void configureFifo() override;
};
