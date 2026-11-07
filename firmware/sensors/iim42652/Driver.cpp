#include "Driver.h"

#include "sensor/Registers.h"
Iim42652::Iim42652() : IimBase("IIM-42652", 0x6f) {}
void Iim42652::configureFifo() {
  writeRegister(IimRegister::PWR_MGMT0,
                0x00);  // Explicitly disable gyro and accel before configuration.
  writeRegister(IimRegister::FIFO_CONFIG1, 0x01);  // FIFO_ACCEL_EN=1, FIFO_GYRO_EN=0, HIRES=0.
}
