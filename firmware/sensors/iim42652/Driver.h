#pragma once

#include <Arduino.h>

#include "sensor/AccelSample.h"
#include "SensorSpiTransport.h"
#include "sensors/iim42652_vendor/Ixm42xxxDriver_HL.h"

// Formal IIM42652 driver built on the vendor Ixm42xxx 1.1.1 high-level API.
// It intentionally has no legacy common base because IIM42352 still uses that
// separate implementation.
//
// resetDevice is kept for compatibility with NodeApp. The vendor
// inv_ixm42xxx_init() always runs its own soft reset, so begin(false, ...)
// cannot skip that reset. The startup log therefore always prints
// soft_reset=1 and the reset_requested field only records the caller intent.
class Iim42652 final {
 public:
  Iim42652();

  bool begin(bool resetDevice = true, bool externalClock = true);
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  bool readDirect(AccelSample& sample);

  const char* name() const { return "IIM-42652"; }
  uint8_t expectedIdentity() const { return 0x6f; }
  uint8_t readRegister(uint8_t address);
  uint16_t lastFifoBytes() const { return lastFifoBytes_; }
  uint16_t fifoPacketBytes() const { return packetBytes_; }
  uint8_t lastHeader() const { return lastHeader_; }
  bool fifoCountValid() const;

  void diagnostic();
  void linkTest();

 private:
  static constexpr uint16_t kSampleCapacity = 256;
  static constexpr uint16_t kFifoCapacityBytes = 2048;
  static constexpr uint16_t kFifoHeaderBytes = 1;
  static constexpr uint16_t kFifoAxisBytes = 6;
  // In 16-bit FIFO mode the temperature field is one byte, not a full int16.
  static constexpr uint16_t kFifoTempBytes = 1;
  static constexpr uint16_t kFifoTimeBytes = 2;

  static Iim42652* active_;
  static void eventThunk(inv_ixm42xxx_sensor_event_t* event);
  void onEvent(const inv_ixm42xxx_sensor_event_t& event);

  bool applyAccelConfig(uint16_t rate, uint8_t range);
  bool readFifoPacket();
  static uint16_t fifoPacketBytesForHeader(uint8_t header);
  bool configureFifo();
  bool forceSixteenBitFifo();
  bool configureInterrupt();
  bool configureClock(bool enabled);
  bool verifyClock(bool enabled);
  bool setBank(uint8_t bank);
  // Vendor reads: bank-aware and served from the register cache when the
  // address is cacheable. Use these for normal driver operation.
  bool readRaw(uint8_t address, uint8_t& value);
  bool readRaw(uint8_t address, uint8_t* buffer, uint32_t length);
  bool writeRaw(uint8_t address, uint8_t value);
  // Physical reads over SPI that bypass the vendor register cache. Use these
  // whenever the result must prove what the chip actually latched.
  bool directRead(uint8_t address, uint8_t& value);
  bool directRead(uint8_t address, uint8_t* buffer, uint32_t length);
  void updateFifoPacketSize();
  uint16_t maxFifoPackets() const;

  SensorSpiTransport transport_;
  inv_ixm42xxx driver_{};
  inv_ixm42xxx_serif serif_{};
  AccelSample samples_[kSampleCapacity]{};
  uint8_t fifoPayload_[20]{};

  uint16_t head_ = 0;
  uint16_t tail_ = 0;
  uint16_t count_ = 0;
  uint16_t lastFifoBytes_ = 0;
  uint8_t fifoErrors_ = 0;
  uint16_t packetBytes_ = 11;
  uint8_t lastHeader_ = 0;
  uint16_t rate_ = 50;
  uint8_t range_ = 4;
  bool initialized_ = false;
  bool running_ = false;
  bool overflow_ = false;
};
