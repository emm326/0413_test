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
  struct FifoSample {
    AccelSample accel;
    uint16_t timestamp;
    uint8_t header;
    uint16_t packetBytes;
    bool timed;
  };

  Iim42652();

  bool begin(bool resetDevice = true, bool externalClock = true);
  bool configure(uint16_t rate, uint8_t range);
  void start();
  void standby();
  void flush();
  uint16_t available();
  bool read(AccelSample& sample);
  // FIFO timestamp is the ODR interval encoded by the sensor. timed=false
  // means the packet had no timestamp field and the caller must use a fallback.
  bool read(AccelSample& sample, uint16_t& fifoTimestamp, bool& timed);
  bool read(FifoSample& sample);
  bool readDirect(AccelSample& sample);

  const char* name() const { return "IIM-42652"; }
  uint8_t expectedIdentity() const { return 0x6f; }
  uint8_t readRegister(uint8_t address);
  uint16_t lastFifoBytes() const { return lastFifoBytes_; }
  uint16_t fifoPacketBytes() const { return packetBytes_; }
  uint8_t lastHeader() const { return lastHeader_; }
  bool fifoCountValid() const;
  uint8_t lastFifoIdentity() const { return lastFifoIdentity_; }
  uint32_t fifoRecoveryTotal() const { return fifoRecoveryTotal_; }
  uint16_t lastFifoCountRaw() const { return lastFifoCountRaw_; }
  uint8_t lastFifoCountAttempts() const { return lastFifoCountAttempts_; }
  bool lastFifoCountOk() const { return lastFifoCountOk_; }
  bool lastFifoReadOk() const { return lastFifoReadOk_; }
  uint8_t fifoConsecutiveFailures() const { return fifoConsecutiveFailures_; }
  uint32_t fifoErrorTotal() const { return fifoErrorTotal_; }
  const char* lastFifoErrorReason() const { return lastFifoErrorReason_; }
  uint16_t lastFifoTimestamp() const { return lastFifoTimestamp_; }
  bool lastFifoTimestampValid() const { return lastFifoTimestampValid_; }
  uint8_t timestampConfig() const { return timestampConfig_; }
  bool timestampRtcMode() const { return timestampRtcMode_; }
  uint32_t timestampUs(uint16_t ticks) const;
  // Effective tick rate of the FIFO timestamp counter for the active clock
  // mode. RTC mode follows the 32 kHz CLKIN; internal mode uses the sensor's
  // own timestamp resolution.
  uint32_t timestampTicksPerSecond() const;

  void diagnostic();
  void linkTest();

 private:
  static constexpr uint16_t kSampleCapacity = 256;
  static constexpr uint16_t kFifoCapacityBytes = 2048;
  static constexpr uint16_t kFifoBatchCapacityBytes = 2080;
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
  bool configureTimestamp();
  bool recoverFifoState();
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
  FifoSample samples_[kSampleCapacity]{};
  uint8_t fifoPayload_[20]{};
  uint8_t fifoBatch_[kFifoBatchCapacityBytes]{};
  uint16_t fifoBatchBytes_ = 0;
  uint16_t fifoParseOffset_ = 0;

  uint16_t head_ = 0;
  uint16_t tail_ = 0;
  uint16_t count_ = 0;
  uint16_t lastFifoBytes_ = 0;
  uint16_t lastFifoCountRaw_ = 0;
  uint8_t lastFifoCountAttempts_ = 0;
  bool lastFifoCountOk_ = true;
  bool lastFifoReadOk_ = true;
  uint8_t fifoConsecutiveFailures_ = 0;
  uint8_t lastFifoIdentity_ = 0;
  uint32_t fifoRecoveryTotal_ = 0;
  uint32_t fifoErrorTotal_ = 0;
  const char* lastFifoErrorReason_ = "none";
  uint16_t lastFifoTimestamp_ = 0;
  bool lastFifoTimestampValid_ = false;
  uint8_t timestampConfig_ = 0;
  bool timestampRtcMode_ = false;
  uint16_t packetBytes_ = 11;
  uint8_t lastHeader_ = 0;
  uint16_t rate_ = 50;
  uint8_t range_ = 4;
  bool initialized_ = false;
  bool running_ = false;
  bool overflow_ = false;
};
