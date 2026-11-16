#pragma once
#include <Arduino.h>

#include "Config.h"
#include "SelectedSensor.h"
#include "clock/SampleClock.h"
#include "gnss/GpsTimebase.h"
#include "protocol/Wire.h"
#include "radio/PacketQueue.h"
class NodeApp {
 public:
  NodeApp();
  void begin();
  void tick();

 private:
  void commands();
  void process(const uint8_t* bytes, size_t length);
  void serialCommands();
  bool initializeSensor(bool resetDevice);
  bool startSensorWithRecovery(const char* trigger, bool resetDevice);
  void sensorSampleTest();
  void acceptStart(const WireProtocol::Header&, const WireProtocol::StartOptions&);
  void stop();
  void acquire();
  void flush(bool tail = false);
  void communicate();
  void report(uint8_t type);
  uint16_t flags() const;
  bool samplingClockHealthy() const;
  uint64_t epoch(uint64_t local) const;
  void resetEdges();
  void detachSensorInterrupt();
  HardwareSerial gpsSerial_;
  GpsTimebase gps_;
  SampleClock clock_;
  AccelDriver sensor_;
  PacketQueue queue_;
  WireProtocol::Sample samples_[25];
  WireProtocol::State state_ = WireProtocol::Boot;
  uint8_t count_ = 0, range_ = 4, sf_ = 6, result_ = 0;
  uint16_t rate_ = 100;
  uint32_t diagValid_ = 0, diagInvalid_ = 0, diagNoEdge_ = 0, diagResets_ = 0, diagEdges_ = 0;
  uint32_t diagRx_ = 0, diagAck_ = 0, diagStop_ = 0, diagWindow_ = 0, diagSamplesLogged_ = 0;
  uint32_t diagTimestamped_ = 0, diagTimestampFallback_ = 0, diagTimestampAnomaly_ = 0;
  uint32_t diagIntervalCount_ = 0, diagIntervalMin_ = 0, diagIntervalMax_ = 0;
  uint64_t diagIntervalSum_ = 0;
  uint64_t diagLastClock_ = 0;
  uint64_t clockWatchEdges_ = 0;
  uint32_t clockWatchMs_ = 0;
  uint8_t clockWatchFaults_ = 0;
  uint32_t run_ = 0, sequence_ = 0, duration_ = 0, lostSamples_ = 0, lostPackets_ = 0;
  uint32_t replyRun_ = 0, replyAt_ = 0, lastStatus_ = 0, lastLog_ = 0;
  uint32_t lastFifoPollMs_ = 0;
  uint64_t startEpoch_ = 0, firstEpoch_ = 0, lastEpoch_ = 0, anchorEpoch_ = 0, anchorLocal_ = 0;
  uint16_t lastFifoTimestampTicks_ = 0;
  bool haveLastFifoTimestamp_ = false;
  float uplink_ = Config::uplinkMHz;
  bool clockOutputReady_ = false;
  bool clockOutputBlocked_ = false;
  uint8_t sensorStartupAttempts_ = 0;
  const char* startupFailure_ = "none";
  const char* faultReason_ = "none";
  bool externalClock_ = false;
  bool intAttached_ = false;
  bool sensorReady_ = false, radioReady_ = false, sensorActive_ = false, gap_ = false,
       overflow_ = false;
  bool replyPending_ = false;
  char serialLine_[128] = {};
  size_t serialLength_ = 0;
};
