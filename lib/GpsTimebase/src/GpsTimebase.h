#pragma once

#include <Arduino.h>

class GpsTimebase {
 public:
  explicit GpsTimebase(HardwareSerial& serial);

  bool begin(int8_t rxPin, int8_t txPin, int8_t ppsPin, uint32_t baud);
  void update();
  bool fixValid() const;
  bool ppsValid() const;
  bool timeValid() const;
  bool healthy() const;
  bool localMicrosForEpoch(uint64_t targetEpochUs, uint64_t& outLocalUs) const;
  bool localMicrosForEpoch(uint64_t targetEpochUs, uint32_t& outLocalUs) const;
  bool localMicrosToEpoch(uint64_t localUs, uint64_t& outEpochUs) const;
  bool localMicrosToEpoch(uint32_t localUs, uint64_t& outEpochUs) const;
  uint64_t epochUsAtLastPps() const;
  uint32_t localUsAtLastPps() const;
  uint64_t localUs64AtLastPps() const;
  uint32_t ppsIntervalUs() const;
  uint32_t timebaseAgeMs() const;
  uint32_t ppsBadCount() const;
  uint32_t localTimerJumpCount() const;

 private:
  static constexpr size_t LINE_CAPACITY = 128;
  static void IRAM_ATTR onPpsThunk();
  void IRAM_ATTR handlePpsIsr();
  void processLine(char* line);
  bool parseRmc(char* line);
  static bool parseUtcDateTime(const char* timeText, const char* dateText, uint64_t& outEpochUs);
  static int64_t daysFromCivil(int year, unsigned month, unsigned day);

  HardwareSerial& serial_;
  int8_t ppsPin_;
  char line_[LINE_CAPACITY];
  size_t lineLen_;
  bool fixValid_;
  bool timeValid_;
  uint64_t lastNmeaEpochUs_;
  uint32_t lastNmeaLocalMs_;
  uint64_t lastPpsEpochUs_;
  uint64_t lastMappedPpsLocalUs64_;
  uint32_t lastMappedPpsLocalUs_;
  uint32_t lastMappedPpsLocalMs_;
  volatile uint64_t lastPpsLocalUs64_;
  volatile uint32_t lastPpsLocalUs_;
  volatile uint32_t lastPpsIntervalUs_;
  volatile uint32_t ppsCount_;
  uint32_t consumedPpsCount_;
  uint32_t ppsBadCount_;
  uint32_t localTimerJumpCount_;

  static GpsTimebase* activeInstance_;
};
