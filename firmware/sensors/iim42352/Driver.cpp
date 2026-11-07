#include "Driver.h"

#include "sensor/Registers.h"
Iim42352::Iim42352() : IimBase("IIM-42352", 0x6d) {}
void Iim42352::configureFifo() {
  writeRegister(IimRegister::FIFO_CONFIG1, 0x01);
}  // 16-bit accel-only packet 1.
