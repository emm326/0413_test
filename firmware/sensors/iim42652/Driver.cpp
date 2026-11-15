#include "Driver.h"

#include "sensor/Registers.h"
Iim42652::Iim42652() : IimBase("IIM-42652", 0x6f) {}
bool Iim42652::configureInterface() {
  // DS-000440 v1.1 section 12.3: SPI uses these I3C settings as well.
  if (!selectBank(1)) {
    selectBank(0);
    return false;
  }
  const uint8_t old4 = readRegister(IimRegister::INTF_CONFIG4);
  const uint8_t old6 = readRegister(IimRegister::INTF_CONFIG6);
  const bool config4Ok = writeVerified(IimRegister::INTF_CONFIG4,
                                       (old4 & ~0x42) | 0x02, 0x42, "spi4wire");
  const bool config6Ok = writeVerified(IimRegister::INTF_CONFIG6, old6 | 0x13,
                                       0x13, "spi_interface");
  const uint8_t interface4 = readRegister(IimRegister::INTF_CONFIG4);
  const uint8_t interface6 = readRegister(IimRegister::INTF_CONFIG6);
  if (!selectBank(0))
    return false;
  const uint8_t drive = readRegister(IimRegister::DRIVE_CONFIG);
  const bool driveOk = writeVerified(IimRegister::DRIVE_CONFIG,
                                     (drive & ~0x3f) | 0x05, 0x3f, "spi_slew");
  const uint8_t interface0 = readRegister(IimRegister::INTF_CONFIG0);
  const bool interface0Ok = writeVerified(IimRegister::INTF_CONFIG0,
                                          (interface0 & ~0x03) | 0x03, 0x03,
                                          "spi_only");
  const uint8_t actualDrive = readRegister(IimRegister::DRIVE_CONFIG);
  const uint8_t actualInterface0 = readRegister(IimRegister::INTF_CONFIG0);
  const bool ok = config4Ok && config6Ok && driveOk && interface0Ok &&
                  (interface4 & 0x42) == 0x02 && (interface6 & 0x13) == 0x13 &&
                  (actualDrive & 0x3f) == 0x05 && (actualInterface0 & 3) == 3;
  Serial.printf("SENSOR_SPI_CONFIG,intf4=0x%02X,intf6=0x%02X,drive=0x%02X,"
                "intf0=0x%02X,ok=%u\n", interface4, interface6, actualDrive,
                actualInterface0, ok);
  return ok;
}
void Iim42652::configureFifo() {
  // Accel and gyro were stopped and IDLE was verified before interface setup.
  writeRegister(IimRegister::FIFO_CONFIG1, 0x41);  // FIFO_ACCEL_EN=1, RESUME_PARTIAL_RD=1, GYRO/HIRES=0.
}
