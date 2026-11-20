#include "NodeApp.h"
#include "sensor/Registers.h"
#include "sensor/StartupCheck.h"
#include <driver/gpio.h>

#include <esp_system.h>
#include <esp_timer.h>

#include "SelectedBoard.h"
#include "radio/Radio.h"
using namespace WireProtocol;
namespace {
constexpr uint32_t kExternalClockVerifyMs = 500;
constexpr uint32_t kExternalClockMinEdges = 12000;
constexpr uint32_t kExternalClockMaxEdges = 20000;
portMUX_TYPE edgeMux = portMUX_INITIALIZER_UNLOCKED;
volatile uint32_t edgeTotal = 0;
volatile uint64_t edgeLastUs = 0;
void IRAM_ATTR dataReady() {
  const uint64_t now = esp_timer_get_time();
  portENTER_CRITICAL_ISR(&edgeMux);
  // 50 Hz is 20 ms and 100 Hz is 10 ms apart. Ringing closer than 2 ms is
  // electrical noise, not a new sample. INT1 is now only a wake/threshold
  // signal; sample intervals come from the sensor's FIFO timestamp.
  if (now - edgeLastUs >= 2000) {
    edgeLastUs = now;
    ++edgeTotal;
  }
  portEXIT_CRITICAL_ISR(&edgeMux);
}
}  // namespace
NodeApp::NodeApp() : gpsSerial_(1), gps_(gpsSerial_) {}
void NodeApp::detachSensorInterrupt() {
  if (intAttached_) {
    detachInterrupt(digitalPinToInterrupt(Board::sensorInt));
    intAttached_ = false;
  }
}
void NodeApp::resetEdges() {
  // edgeTotal is cumulative so FLOW can report a delta. No per-sample edge
  // queue remains; FIFO timestamps are the sampling timebase.
}
void NodeApp::begin() {
  Serial.begin(Config::serialBaud);
  delay(5000);  // Allow the USB diagnostic reader to attach after upload.
  Board::begin();
  Serial.printf("BOOT,version=%s,board=%s,sensor=%s,id=%u,uplink=%.1f,clock=32000,packet=180\n",
                APP_FIRMWARE_VERSION, Board::name, sensor_.name(), Config::nodeId,
                Config::uplinkMHz);
  clockOutputReady_ = clock_.begin(Board::clockOut);
  for (uint8_t attempt = 2; attempt <= 3 && !clockOutputReady_; ++attempt) {
    Serial.printf("CLOCK_REINIT,attempt=%u\n", attempt);
    delay(10);
    clockOutputReady_ = clock_.begin(Board::clockOut);
  }
  externalClock_ = clockOutputReady_;
  clockOutputBlocked_ = !clockOutputReady_;
  if (clockOutputReady_) {
    // Let the carrier and PCNT input path settle before measuring.
    delay(100);
    clockWatchEdges_ = clock_.edgeCount();
    clockWatchMs_ = millis();
  }
  Serial.printf("CLOCK_OUTPUT,ready=%u,source=%s,pad_usable=%u\n",
                clockOutputReady_ ? 1 : 0,
                clockOutputReady_ ? "external" : "sensor_internal",
                clock_.padUsable() ? 1 : 0);
  gps_.setPpsObserver(SampleClock::onPps);
#if defined(APP_BOARD_EORA) && defined(APP_SENSOR_IIM42652)
  sensorReady_ = startSensorWithRecovery("boot", false);
#else
  sensorReady_ = startSensorWithRecovery("boot", true);
#endif
  if (clockOutputReady_) {
    Serial.println("DIAG_STAGE,before_radio");
    sensor_.diagnostic();
  }
  gps_.begin(Board::gpsRx, Board::gpsTx, Board::gpsPps, Config::gpsBaud);
#ifndef APP_USB_TEST
  radioReady_ = Radio::begin();
  if (clockOutputReady_) {
    Serial.println("DIAG_STAGE,after_radio");
    sensor_.diagnostic();
  }
#else
  radioReady_ = true;
#endif
  // External-clock mode is preferred but not mandatory: the IIM42652 can use
  // its internal oscillator, which keeps sampling alive when the board-side
  // CLKIN pin is unusable.
  faultReason_ = sensorReady_ ? (radioReady_ ? "none" : "radio_init") : "sensor_startup";
  state_ = sensorReady_ && radioReady_ ? Waiting : Fault;
  Serial.printf("READY,sensor=%u,radio=%u,clock_output=%u,clock_source=%s,"
                "clock_measurement=%s\n",
                sensorReady_, radioReady_, clockOutputReady_,
                externalClock_ ? "external" : "sensor_internal",
                externalClock_ ? "wait_60_pps" : "sensor_internal_fifo");
  lastStatus_ = millis() + ((Config::nodeId - 1001) % 16) * 120;
}
bool NodeApp::initializeSensor(bool resetDevice) {
  // Rebind only the sensor SPI bus; do not restart the clock, GNSS, or radio.
  detachSensorInterrupt();
  resetEdges();
  // Internal pull-down keeps a floating INT1 line from generating a false edge storm.
  pinMode(Board::sensorInt, INPUT_PULLDOWN);
  // The sensor requires a live CLKIN in RTC mode. Probe it once per boot, but
  // do not repeat the same failed hardware probe on every sensor retry.
  if (!clockOutputReady_ && !clockOutputBlocked_) {
    Serial.println("CLOCK_REINIT,reason=not_ready");
    clockOutputReady_ = clock_.begin(Board::clockOut);
  }
  bool clockAlive =
      clockOutputReady_ && clock_.verifyOutput(kExternalClockVerifyMs, kExternalClockMinEdges, kExternalClockMaxEdges);
  if (!clockAlive && !clockOutputBlocked_) {
    Serial.println("CLOCK_VERIFY_FAILED,recover=1");
    // Use a 50 ms window so a single PCNT hiccup cannot declare a live carrier
    // dead. Five rebuilds cover both a stopped timer and a lost GPIO route.
    for (uint8_t attempt = 1; attempt <= 5 && !clockAlive; ++attempt) {
      const bool rebuilt = clock_.recover();
      const uint32_t measured = rebuilt ? clock_.measureEdges(kExternalClockVerifyMs) : 0;
      clockAlive = measured >= kExternalClockMinEdges && measured <= kExternalClockMaxEdges;
      Serial.printf("CLOCK_RECOVER,attempt=%u,rebuilt=%u,edges_50ms=%lu,ok=%u\n",
                    attempt, rebuilt ? 1 : 0, (unsigned long)measured,
                    clockAlive ? 1 : 0);
      if (!clockAlive) {
        delay(10);
      }
    }
  }
  clockOutputReady_ = clockAlive;
  externalClock_ = clockAlive;
  if (!externalClock_) {
    if (!clockOutputBlocked_) {
      Serial.printf("CLOCK_SOURCE,source=sensor_internal,pad_usable=%u,"
                    "reason=clkin_unavailable\n",
                    clock_.padUsable() ? 1 : 0);
    }
    clockOutputBlocked_ = true;
  } else {
    clockOutputBlocked_ = false;
  }
  bool initialized = sensor_.begin(resetDevice, externalClock_);
  SensorChecks::StartupCheck last = {};
  bool passed = false;
  // Run the complete start/measure cycle. A marginal INT1 line can fail one
  // window and pass the next, so retry before either declaring a fault or
  // changing the clock source.
  auto runStartupChecks = [&]() {
    bool ok = false;
    for (uint8_t attempt = 1; attempt <= 5 && !ok; ++attempt) {
      SensorChecks::StartupCheck check = {};
      detachSensorInterrupt();
      resetEdges();
      sensor_.flush();
      attachInterrupt(digitalPinToInterrupt(Board::sensorInt), dataReady, RISING);
      intAttached_ = true;
      sensor_.start();
      // Ignore the FIFO/INT1 transient created while the sensor switches into
      // low-noise mode. Measure only a stable window after the transient.
      delay(200);
      resetEdges();
      const uint32_t before = edgeTotal;
      // measureEdges() covers this 120 ms window with both PCNT and a direct
      // GPIO poll, so a marginal PCNT route cannot fail an otherwise live
      // CLKIN output.
      check.clockEdges = clock_.measureEdges(120);
      check.dataReadyEdges = edgeTotal - before;
      check.fifoSamples = sensor_.available();
      check.fifoCountValid = sensor_.fifoCountValid();
      if (check.fifoSamples && check.fifoCountValid) {
        AccelSample sample = {};
        check.sampleValid = sensor_.read(sample) &&
                            !(sample.x == 0 && sample.y == 0 && sample.z == 0);
        Serial.printf("SENSOR_SAMPLE,valid=%u,x=%d,y=%d,z=%d\n", check.sampleValid,
                      sample.x, sample.y, sample.z);
      }
      check.identityValid = sensor_.readRegister(IimRegister::WHO_AM_I) ==
                            sensor_.expectedIdentity();
      sensor_.standby();
      detachSensorInterrupt();
      resetEdges();
      ok = SensorChecks::startupPassed(check, externalClock_);
      last = check;
      Serial.printf("SENSOR_CHECK_ATTEMPT,attempt=%u,drdy=%lu,fifo_samples=%u,"
                    "clock_edges=%llu,sample_valid=%u,identity_ok=%u,ok=%u\n",
                    attempt, (unsigned long)check.dataReadyEdges, check.fifoSamples,
                    (unsigned long long)check.clockEdges, check.sampleValid,
                    check.identityValid, ok ? 1 : 0);
      if (!ok)
        delay(50);
    }
    return ok;
  };
  if (initialized)
    passed = runStartupChecks();
  // GPIO36 can pass the boot self-test and then lose the carrier. Do not
  // leave the node faulted: stop CLKIN, let the sensor use its internal
  // oscillator, and rerun the same startup evidence checks. External clock is
  // preferred, but a stable internal-clock node is usable.
  // Fall back only when the measured external clock is outside the allowed
  // edge band. A failed check with a healthy clock (for example a FIFO count
  // outside the startup window) must not disable a working CLKIN.
  const bool externalClockOutOfRange =
      !initialized ||
      last.clockEdges < SensorChecks::startupExternalClockMinEdges ||
      last.clockEdges > SensorChecks::startupExternalClockMaxEdges;
  if (!passed && externalClock_ && externalClockOutOfRange) {
    Serial.printf("CLOCK_SOURCE,source=sensor_internal,pad_usable=%u,"
                  "reason=external_runtime_failed\n",
                  clock_.padUsable() ? 1 : 0);
    clock_.setOutputEnabled(false);
    clockOutputReady_ = false;
    externalClock_ = false;
    clockOutputBlocked_ = true;
    delay(20);
    initialized = sensor_.begin(false, false);
    passed = initialized && runStartupChecks();
  }
  startupFailure_ = passed ? "none" : (initialized ? "startup_checks"
                                                        : "vendor_init");
  Serial.printf("SENSOR_CHECK,init=%u,executed=%u,drdy=%lu,fifo_samples=%u,"
                "clock_edges=%llu,clock_source=%s,sample_valid=%u,identity_ok=%u,"
                "fifo_bytes=%u,int1_level=%u,failure=%s,ok=%u\n",
                initialized, initialized, (unsigned long)last.dataReadyEdges,
                last.fifoSamples, (unsigned long long)last.clockEdges,
                externalClock_ ? "external" : "sensor_internal",
                last.sampleValid, last.identityValid, sensor_.lastFifoBytes(),
                digitalRead(Board::sensorInt), startupFailure_, passed);
  if (!passed) {
    faultReason_ = "sensor_startup";
    detachSensorInterrupt();
    resetEdges();
    Serial.printf("SENSOR_FAULT,reason=sensor_startup,failure=%s,"
                  "drdy_interrupt_disabled=1,capture_blocked=1\n",
                  startupFailure_);
  }
  return passed;
}
bool NodeApp::startSensorWithRecovery(const char* trigger, bool resetDevice) {
  sensorStartupAttempts_ = 0;
  for (uint8_t attempt = 1;
       attempt <= SensorChecks::sensorStartupMaxAttempts; ++attempt) {
    sensorStartupAttempts_ = attempt;
    Serial.printf("SENSOR_STARTUP_ATTEMPT,trigger=%s,attempt=%u,max=%u\n",
                  trigger, attempt, SensorChecks::sensorStartupMaxAttempts);
    if (initializeSensor(resetDevice)) {
      startupFailure_ = "none";
      faultReason_ = "none";
      Serial.printf("SENSOR_STARTUP_END,trigger=%s,ok=1,attempt=%u\n",
                    trigger, attempt);
      return true;
    }
    Serial.printf("SENSOR_STARTUP_FAILED,trigger=%s,attempt=%u,failure=%s\n",
                  trigger, attempt, startupFailure_);
    if (SensorChecks::shouldRetrySensorStartup(attempt)) {
      Serial.printf("SENSOR_STARTUP_RETRY,delay_ms=%u\n",
                    SensorChecks::sensorStartupRetryDelayMs);
      delay(SensorChecks::sensorStartupRetryDelayMs);
    }
  }
  faultReason_ = "sensor_startup";
  Serial.printf("SENSOR_STARTUP_END,trigger=%s,ok=0,attempts=%u,"
                "failure=%s\n",
                trigger, sensorStartupAttempts_, startupFailure_);
  return false;
}
void NodeApp::sensorSampleTest() {
  if (!SensorChecks::canRetry(state_, sensorActive_, queue_.size())) {
    Serial.println("SENSOR_SAMPLE_TEST,ok=0,reason=not_idle");
    return;
  }
  detachSensorInterrupt();
  resetEdges();
  sensorReady_ = false;
  state_ = Fault;
  faultReason_ = "sensor_sample_test";
  const bool hadExternalClock = clockOutputReady_ && externalClock_;
  const bool isolated = !hadExternalClock || clock_.setOutputEnabled(false);
  Serial.printf("SENSOR_SAMPLE_TEST_BEGIN,clock=internal,rate=50,range_g=4,"
                "target=20,synchronized=0,sensor_reset=0,clkin_output_high_z=%u,"
                "prior_clock_source=%s\n",
                isolated, hadExternalClock ? "external" : "sensor_internal");
  const bool configured = isolated && sensor_.begin(false, false) &&
                          sensor_.configure(50, 4);
  uint16_t captured = 0;
  if (configured) {
    sensor_.start();
    const uint32_t started = millis();
    while (captured < 20 && millis() - started < 2500) {
      AccelSample sample = {};
      if (sensor_.readDirect(sample)) {
        ++captured;
        Serial.printf("SENSOR_TEST_SAMPLE,index=%u,local_us=%llu,x=%d,y=%d,z=%d,"
                      "rate=50,range_g=4,synchronized=0\n", captured,
                      (unsigned long long)esp_timer_get_time(), sample.x, sample.y,
                      sample.z);
      }
      gps_.update();
      clock_.update(gps_.healthy());
#ifndef APP_USB_TEST
      if (radioReady_)
        Radio::update();
#endif
      delay(1);
    }
    sensor_.standby();
  }
  Serial.printf("SENSOR_SAMPLE_TEST_END,configured=%u,samples=%u,target=20,ok=%u\n",
                configured, captured, configured && captured == 20);
  if (hadExternalClock) {
    // Restore the clock buffer before selecting external RTC mode again.
    clockOutputReady_ = clock_.setOutputEnabled(true);
    // Ignore the output-matrix reconnect transient.
    delay(100);
    const bool restored =
        clockOutputReady_ && clock_.verifyOutput(kExternalClockVerifyMs, kExternalClockMinEdges, kExternalClockMaxEdges);
    clockOutputReady_ = restored;
    externalClock_ = restored;
    clockOutputBlocked_ = !restored;
    Serial.printf("SENSOR_CLOCK_RESTORE,output_enabled=%u,"
                  "verified_50ms=%u,pps_recalibration=1\n",
                  restored ? 1 : 0, restored ? 1 : 0);
  } else {
    clockOutputReady_ = false;
    externalClock_ = false;
    clockOutputBlocked_ = true;
    Serial.println("SENSOR_CLOCK_RESTORE,skipped=1,source=sensor_internal");
  }
  // The internal-clock diagnostic never authorizes synchronized acquisition.
  sensorReady_ = startSensorWithRecovery("sample_test_restore", false);
  state_ = sensorReady_ && radioReady_ ? Waiting : Fault;
  faultReason_ =
      sensorReady_ ? (radioReady_ ? "none" : "radio_init") : "sensor_startup";
  Serial.printf("SENSOR_SAMPLE_TEST_RESTORE,external_clock=%u,sensor_ready=%u,"
                "state=%u,fault_reason=%s\n",
                hadExternalClock ? 1 : 0, sensorReady_, state_, faultReason_);
}
uint64_t NodeApp::epoch(uint64_t local) const {
  if (!anchorEpoch_) {
    // Bench builds have no GNSS anchor; use a fixed base plus uptime so the
    // capture scheduler can still run. gps=0 marks the time as untrusted.
    return APP_REQUIRE_TIME_SYNC ? 0 : static_cast<uint64_t>(APP_BENCH_EPOCH_US) + local;
  }
  const int64_t value = int64_t(anchorEpoch_) + int64_t(local) - int64_t(anchorLocal_);
  return value > 0 ? uint64_t(value) : 0;
}
uint16_t NodeApp::flags() const {
  return (gps_.healthy() ? GpsHealthy : 0) | (samplingClockHealthy() ? ClockHealthy : 0) |
         (gap_ ? SampleGap : 0) | (overflow_ ? QueueOverflow : 0);
}
bool NodeApp::samplingClockHealthy() const {
  return SensorChecks::clockReadyForCapture(externalClock_, clock_.healthy(), sensorReady_);
}
void NodeApp::tick() {
  gps_.update();
  clock_.update(gps_.healthy());
  // The board-side 32 kHz carrier can pass presence checks but fail the
  // frequency tolerance. If that happens while idle, switch to the sensor's
  // internal oscillator instead of leaving capture blocked forever.
  if (externalClock_ && gps_.healthy() && clock_.measured() &&
      !clock_.healthy() && state_ == Waiting && !sensorActive_ &&
      queue_.size() == 0) {
    Serial.printf("CLOCK_SOURCE,source=sensor_internal,pad_usable=%u,"
                  "reason=external_frequency_out_of_range,hz=%.4f,ppm=%.2f\n",
                  clock_.padUsable() ? 1 : 0, clock_.hz(), clock_.ppm());
    clock_.setOutputEnabled(false);
    clockOutputReady_ = false;
    clockOutputBlocked_ = true;
    externalClock_ = false;
    sensorReady_ = startSensorWithRecovery("clock_frequency", false);
    state_ = sensorReady_ && radioReady_ ? Waiting : Fault;
    faultReason_ = sensorReady_ ? (radioReady_ ? "none" : "radio_init")
                                : "sensor_startup";
    Serial.printf("CLOCK_FREQUENCY_FALLBACK,sensor_ready=%u,state=%u,"
                  "fault_reason=%s\n",
                  sensorReady_, state_, faultReason_);
  }
  if (gps_.healthy()) {
    anchorEpoch_ = gps_.epochUsAtLastPps();
    anchorLocal_ = gps_.localUs64AtLastPps();
  }
#ifndef APP_USB_TEST
  if (radioReady_)
    Radio::update();
#endif
  // Clock watchdog: the MCPWM loopback must keep producing ~32 kHz. Two bad
  // 1 s windows in a row trigger one full route restore.
  if (clockOutputReady_ && state_ != WireProtocol::Fault &&
      millis() - clockWatchMs_ >= 1000) {
    const uint64_t edgesNow = clock_.edgeCount();
    const uint64_t delta = edgesNow - clockWatchEdges_;
    clockWatchEdges_ = edgesNow;
    clockWatchMs_ = millis();
    const bool pcntHealthy = delta >= 24000 && delta <= 40000;
    // Do not treat a failed PCNT readback as a stopped clock. The pad poll
    // independently proves that GPIO36 is still toggling.
    const bool outputHealthy =
        pcntHealthy || clock_.verifyOutput(kExternalClockVerifyMs, kExternalClockMinEdges, kExternalClockMaxEdges);
    if (!outputHealthy) {
      ++clockWatchFaults_;
      Serial.printf("CLOCK_WATCHDOG,edges_1s=%llu,pcnt_ok=%u,"
                    "output_ok=%u,faults=%u\n",
                    (unsigned long long)delta, pcntHealthy ? 1 : 0,
                    outputHealthy ? 1 : 0, clockWatchFaults_);
      if (clockWatchFaults_ >= 2) {
        const bool recovered = clock_.recover() && clock_.verifyOutput(kExternalClockVerifyMs, kExternalClockMinEdges, kExternalClockMaxEdges);
        Serial.printf("CLOCK_RECOVER,ok=%u\n", recovered ? 1 : 0);
        clockWatchFaults_ = 0;
        if (!recovered) {
          const bool canFallback =
              SensorChecks::canRetry(state_, sensorActive_, queue_.size());
          Serial.printf("CLOCK_WATCHDOG_FALLBACK,idle=%u,source=sensor_internal\n",
                        canFallback ? 1 : 0);
          clock_.setOutputEnabled(false);
          clockOutputReady_ = false;
          clockOutputBlocked_ = true;
          externalClock_ = false;
          if (canFallback) {
            // A failed external carrier is not a sensor failure. Reinitialize
            // from the beginning using the sensor's internal oscillator.
            sensorReady_ = startSensorWithRecovery("clock_watchdog", false);
            state_ = sensorReady_ && radioReady_ ? Waiting : Fault;
            faultReason_ = sensorReady_
                               ? (radioReady_ ? "none" : "radio_init")
                               : "sensor_startup";
            Serial.printf("CLOCK_WATCHDOG_RECOVERED,sensor_ready=%u,state=%u,"
                          "fault_reason=%s\n",
                          sensorReady_, state_, faultReason_);
          } else {
            sensorReady_ = false;
            faultReason_ = "clock_watchdog";
            state_ = WireProtocol::Fault;
          }
        }
      }
    } else {
      clockWatchFaults_ = 0;
    }
  }
  serialCommands();
  commands();
  const uint64_t now = epoch(esp_timer_get_time());
  if (state_ == Armed && now && now + Config::warmupUs >= startEpoch_) {
    if (APP_REQUIRE_TIME_SYNC && (!gps_.healthy() || !samplingClockHealthy())) {
      result_ = NotReady;
      state_ = Waiting;
      replyPending_ = true;
      replyAt_ = millis();
    } else {
      sensor_.flush();
      detachSensorInterrupt();
      attachInterrupt(digitalPinToInterrupt(Board::sensorInt), dataReady, RISING);
      intAttached_ = true;
      resetEdges();
      sensor_.start();
      // Give the first FIFO records time to appear before the first count read.
      delay(100);
      sensorActive_ = true;
      state_ = Capturing;
    }
  }
  if (sensorActive_)
    acquire();
  if (state_ == Capturing && duration_ && now >= startEpoch_ + uint64_t(duration_) * 1000000)
    stop();
  communicate();
  if (state_ == Draining && !queue_.size())
    state_ = Waiting;
  if (millis() - lastLog_ >= 5000) {
    lastLog_ = millis();
    const uint32_t clk10ms = clock_.measureEdges(10);
    const uint64_t edgesNow = clock_.edgeCount();
    Serial.printf("FLOW,edge_delta=%lu,valid=%lu,invalid=%lu,no_edge=%lu,resets=%lu,fifo_bytes=%u,rx=%lu,ack=%lu,stop=%lu,window=%lu,clock_delta=%llu,clk_10ms=%lu,phase=%lu,timed=%lu,fallback=%lu,ts_anomaly=%lu,fifo_errors=%lu,fifo_consecutive=%u,fifo_header=0x%02X,fifo_count=%u,fifo_packet=%u,dt_mean_us=%llu,dt_min_us=%u,dt_max_us=%u\n",
      (unsigned long)(edgeTotal - diagEdges_), (unsigned long)diagValid_, (unsigned long)diagInvalid_,
      (unsigned long)diagNoEdge_, (unsigned long)diagResets_, sensor_.lastFifoBytes(),
      (unsigned long)diagRx_, (unsigned long)diagAck_, (unsigned long)diagStop_, (unsigned long)diagWindow_,
      (unsigned long long)(edgesNow - diagLastClock_), (unsigned long)clk10ms,
      (unsigned long)(now >= startEpoch_ ? (now - startEpoch_) % Config::cycleUs : 0),
      (unsigned long)diagTimestamped_, (unsigned long)diagTimestampFallback_,
      (unsigned long)diagTimestampAnomaly_, (unsigned long)sensor_.fifoErrorTotal(),
      sensor_.fifoConsecutiveFailures(), sensor_.lastHeader(), sensor_.lastFifoCountRaw(),
      sensor_.fifoPacketBytes(),
      (unsigned long long)(diagIntervalCount_ ? diagIntervalSum_ / diagIntervalCount_ : 0),
      diagIntervalMin_, diagIntervalMax_);
    diagEdges_ = edgeTotal; diagLastClock_ = edgesNow;
    Serial.printf(
        "STATUS,id=%u,state=%u,gps=%u,clock_hz=%.4f,ppm=%.2f,clock_ok=%u,"
        "clock_source=%s,queue=%u,lost_samples=%lu,lost_packets=%lu,"
        "startup_attempts=%u,startup_failure=%s,fault_reason=%s\n",
        Config::nodeId, state_, gps_.healthy(), clock_.hz(), clock_.ppm(),
        samplingClockHealthy() ? 1 : 0,
        externalClock_ ? "external" : "sensor_internal", queue_.size(),
        (unsigned long)lostSamples_, (unsigned long)lostPackets_,
        sensorStartupAttempts_, startupFailure_, faultReason_);
  }
  delay(1);
}
void NodeApp::acceptStart(const Header& h, const StartOptions& options) {
  replyRun_ = h.run;
  replyPending_ = true;
  replyAt_ = millis() + Config::responseDelayMs;
  if (h.run == run_ && run_) {
    result_ = Accepted;
    return;
  }
  if (state_ == Capturing || state_ == Armed || state_ == Draining) {
    result_ = Busy;
    return;
  }
  if (state_ == Fault || !sensorReady_ ||
      (APP_REQUIRE_TIME_SYNC && (!gps_.healthy() || !samplingClockHealthy()))) {
    result_ = NotReady;
    return;
  }
  if ((h.rate != 50 && h.rate != 100) ||
      (h.range != 2 && h.range != 4 && h.range != 8 && h.range != 16) || options.sf < 5 ||
      options.sf > 7 || (h.rate == 50 && options.sf != 6) ||
      (h.rate == 100 && options.sf != 5) ||
      (options.uplinkMHz != 0 &&
       (options.uplinkMHz < Config::uplinkMHz - 0.01f ||
        options.uplinkMHz > Config::uplinkMHz + 0.01f))) {
    result_ = BadSettings;
    return;
  }
  const uint64_t now = epoch(esp_timer_get_time());
  const uint64_t startUs =
      (!APP_REQUIRE_TIME_SYNC && h.epochUs == 0) ? now + 1000000 : h.epochUs;
  if (!h.run || startUs < now + 1000000) {
    result_ = TooLate;
    return;
  }
  if (sensor_.readRegister(IimRegister::WHO_AM_I) != sensor_.expectedIdentity()) {
    sensorReady_ = false;
    faultReason_ = "sensor_identity";
    state_ = Fault;
    detachSensorInterrupt();
    result_ = NotReady;
    return;
  }
  if (!sensor_.configure(h.rate, h.range)) {
    result_ = BadSettings;
    return;
  }
  run_ = h.run;
  diagValid_ = diagInvalid_ = diagNoEdge_ = diagResets_ = diagSamplesLogged_ = 0;
  diagTimestamped_ = diagTimestampFallback_ = diagTimestampAnomaly_ = 0;
  diagIntervalCount_ = diagIntervalMin_ = diagIntervalMax_ = 0;
  diagIntervalSum_ = 0;
  sensor_.diagnostic();
  rate_ = h.rate;
  range_ = h.range;
  sf_ = options.sf;
  uplink_ = Config::uplinkMHz;
  startEpoch_ = startUs;
  duration_ = options.durationSeconds;
  sequence_ = count_ = 0;
  lostSamples_ = lostPackets_ = 0;
  gap_ = overflow_ = false;
  queue_.clear();
  firstEpoch_ = lastEpoch_ = 0;
  lastFifoTimestampTicks_ = 0;
  haveLastFifoTimestamp_ = false;
  lastFifoPollMs_ = millis();
  result_ = Accepted;
  state_ = Armed;
}
void NodeApp::stop() {
  if (state_ == Fault && !sensorActive_)
    return;  // STOP must never turn a failed sensor into "ready".
  if (sensorActive_) {
    // Do not take a final FIFO reading here. A transient count failure at the
    // commanded stop boundary must not turn a completed run into a fault.
    sensor_.standby();
    detachSensorInterrupt();
    sensorActive_ = false;
    resetEdges();
  }
  flush(true);
  state_ = queue_.size() ? Draining : Waiting;
}
void NodeApp::commands() {
#ifndef APP_USB_TEST
  if (!radioReady_)
    return;
  uint8_t bytes[255];
  size_t length;
  int16_t rssi;
  float snr;
  if (Radio::receive(bytes, length, rssi, snr))
    process(bytes, length);
#endif
}
void NodeApp::process(const uint8_t* bytes, size_t length) {
  Header h;
  const uint8_t* p;
  size_t size;
  if (!decode(bytes, length, h, p, size) || (h.node != Config::nodeId && h.node != broadcast))
    return;
  ++diagRx_;
  if (h.type == Start && size == sizeof(StartOptions)) {
    StartOptions o;
    memcpy(&o, p, sizeof(o));
    const uint16_t bit = uint16_t(1) << ((Config::nodeId - 1001) % 16);
    if (h.node == broadcast && !(o.nodeMask & bit))
      return;
    acceptStart(h, o);
  } else if (h.type == Stop && (h.run == run_ || h.run == 0)) {
    ++diagStop_;
    stop();
    replyRun_ = h.run;
    result_ = Accepted;
    replyPending_ = true;
    replyAt_ = millis() + Config::responseDelayMs;
  } else if (h.type == Discover) {
    replyRun_ = h.run;
    replyPending_ = true;
    replyAt_ = millis() + Config::responseDelayMs;
  } else if (h.type == Ack && h.run == run_ && size % sizeof(AckEntry) == 0) {
    ++diagAck_;
    for (size_t i = 0; i < size; i += sizeof(AckEntry)) {
      AckEntry a;
      memcpy(&a, p + i, sizeof(a));
      if (a.node == Config::nodeId)
        queue_.acknowledge(a.firstSequence, a.bitmap);
    }
  }
}
void NodeApp::serialCommands() {
  while (Serial.available()) {
    const char c = Serial.read();
    if (c == '\r')
      continue;
    if (c != '\n') {
      if (serialLength_ + 1 < sizeof(serialLine_))
        serialLine_[serialLength_++] = c;
      else
        serialLength_ = 0;
      continue;
    }
    serialLine_[serialLength_] = 0;
    serialLength_ = 0;
    if (!strcmp(serialLine_, "DIAG")) {
      if (sensorReady_)
        sensor_.diagnostic();
      else
        Serial.println("SENSOR_ERROR,stage=sensor_not_ready");
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_SAMPLE_TEST")) {
      sensorSampleTest();
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_I2C_SCAN")) {
      if (sensorActive_ || queue_.size()) {
        Serial.println("SENSOR_I2C_SCAN,ok=0,reason=busy");
        continue;
      }
#ifdef APP_SENSOR_IIM42352
      sensor_.i2cScan();
#else
      // The formal IIM42652 driver uses vendor SPI only and has no I2C scanner.
      Serial.println("SENSOR_I2C_SCAN,ok=0,reason=unsupported_sensor");
#endif
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_LINK_TEST")) {
      // This command is read-only and intentionally remains available in
      // Fault so an unresponsive sensor can still be diagnosed at boot.
      if (sensorActive_ || queue_.size()) {
        Serial.println("SENSOR_LINK_TEST,ok=0,reason=busy");
        continue;
      }
      sensor_.linkTest();
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_RETRY")) {
      if (!SensorChecks::canRetry(state_, sensorActive_, queue_.size())) {
        Serial.println("SENSOR_RETRY,ok=0,reason=busy,reset=0");
        continue;
      }
      sensorReady_ = startSensorWithRecovery("command", false);
      // External-clock mode is preferred but not mandatory: the IIM42652 can use
      // its internal oscillator, which keeps sampling alive when the board-side
      // CLKIN pin is unusable.
      faultReason_ =
          sensorReady_ ? (radioReady_ ? "none" : "radio_init") : "sensor_startup";
      state_ = sensorReady_ && radioReady_ ? Waiting : Fault;
      Serial.printf("SENSOR_RETRY,ok=%u,state=%u,fault_reason=%s,reset=0\n",
                    sensorReady_, state_, faultReason_);
      continue;
    }
    if (!strcmp(serialLine_, "STOP")) {
      stop();
      continue;
    }
    if (!strcmp(serialLine_, "STATUS")) {
      report(Status);
      continue;
    }
    if (!strcmp(serialLine_, "GPS_SCAN")) {
      if (sensorActive_ || queue_.size()) {
        Serial.println("GPS_SCAN,ok=0,reason=busy");
        continue;
      }
      gps_.scanBaud();
      continue;
    }
    if (!strcmp(serialLine_, "GPS_DIAG")) {
      Serial.printf("GPS_DIAG,fix=%u,pps=%u,time=%u,healthy=%u,"
                    "pps_count=%lu,pps_interval_us=%lu,age_ms=%lu,"
                    "pps_bad=%lu,timer_jumps=%lu,last_pps_epoch_us=%llu\n",
                    gps_.fixValid() ? 1 : 0, gps_.ppsValid() ? 1 : 0,
                    gps_.timeValid() ? 1 : 0, gps_.healthy() ? 1 : 0,
                    (unsigned long)gps_.ppsCount(),
                    (unsigned long)gps_.ppsIntervalUs(),
                    (unsigned long)gps_.timebaseAgeMs(),
                    (unsigned long)gps_.ppsBadCount(),
                    (unsigned long)gps_.localTimerJumpCount(),
                    (unsigned long long)gps_.epochUsAtLastPps());
      continue;
    }
    unsigned long long when = 0;
    unsigned rate = 0, range = 4;
    unsigned long run = 0, duration = 0;
    int fields =
        sscanf(serialLine_, "START_AT %llu %u %u %lu %lu", &when, &rate, &range, &run, &duration);
    if (fields >= 2) {
      Header h = {};
      h.run = run ? run : esp_random();
      h.epochUs = when;
      h.rate = rate;
      h.range = range;
      const uint8_t sf = rate == 100 ? 5 : 6;
      StartOptions options = {Config::uplinkMHz, uint32_t(duration), sf, 0, 0xffff};
      acceptStart(h, options);
    } else
      Serial.println(
          "COMMANDS: DIAG; SENSOR_RETRY; SENSOR_SAMPLE_TEST; SENSOR_LINK_TEST; SENSOR_I2C_SCAN; GPS_DIAG; GPS_SCAN; START_AT epoch_us 50|100 [range_g] [run_id] [duration_s]; STOP; STATUS");
  }
}
void NodeApp::acquire() {
  // Poll at 5 ms instead of hammering FIFO_COUNT and WHO_AM_I on every loop
  // pass. 50 Hz needs only one new record every 20 ms; 5 ms still catches
  // bursts while cutting register traffic by an order of magnitude.
  const uint32_t nowMs = millis();
  if (nowMs - lastFifoPollMs_ < 5) {
    return;
  }
  lastFifoPollMs_ = nowMs;
  const uint16_t available = sensor_.available();
  const bool fifoReadOk = sensor_.lastFifoReadOk();
  if (!sensor_.fifoCountValid()) {
    const uint16_t invalidBytes = sensor_.lastFifoBytes();
    Serial.printf(
        "SENSOR_FAULT_DETAIL,reason=%s,available=%u,count_raw=%u,"
        "count_attempts=%u,count_ok=%u,read_ok=%u,consecutive=%u,"
        "fifo_errors=%u,header=0x%02X,packet_bytes=%u,fifo_bytes=%u,"
        "timestamp_raw=%u,timestamp_valid=%u\n",
        sensor_.lastFifoErrorReason(), available, sensor_.lastFifoCountRaw(),
        sensor_.lastFifoCountAttempts(), sensor_.lastFifoCountOk() ? 1 : 0,
        fifoReadOk ? 1 : 0, sensor_.fifoConsecutiveFailures(),
        sensor_.fifoErrorTotal(), sensor_.lastHeader(),
        sensor_.fifoPacketBytes(), invalidBytes, sensor_.lastFifoTimestamp(),
        sensor_.lastFifoTimestampValid() ? 1 : 0);
    Serial.printf("SENSOR_FAULT,invalid_fifo_bytes=%u,capture_blocked=1\n",
                  invalidBytes);
    ++diagResets_;
    gap_ = true;
    sensorReady_ = false;
    sensor_.standby();
    sensorActive_ = false;
    flush(true);
    faultReason_ = "sensor_fifo";
    state_ = Fault;
    detachSensorInterrupt();
    return;
  }

  if (!fifoReadOk) {
    ++diagResets_;
    gap_ = true;
    haveLastFifoTimestamp_ = false;
    if (diagResets_ <= 8) {
      Serial.printf(
          "FIFO_RECOVER,reason=%s,available=%u,count_raw=%u,attempts=%u,"
          "consecutive=%u,total=%u,header=0x%02X\n",
          sensor_.lastFifoErrorReason(), available, sensor_.lastFifoCountRaw(),
          sensor_.lastFifoCountAttempts(), sensor_.fifoConsecutiveFailures(),
          sensor_.fifoErrorTotal(), sensor_.lastHeader());
    }
    if (available == 0) {
      ++lostSamples_;
      return;
    }
  }

  // The tick rate depends on the active clock mode: 32 kHz CLKIN in RTC mode,
  // or the sensor's internal 16 us/1 us step otherwise.
  const uint16_t expectedTicks = static_cast<uint16_t>(
      sensor_.timestampTicksPerSecond() / rate_);
  const uint64_t nominalIntervalUs = 1000000ULL / rate_;
  for (uint16_t i = 0; i < available; ++i) {
    AccelDriver::FifoSample item = {};
    const bool valid = sensor_.read(item);
    const uint64_t local = esp_timer_get_time();
    if (!valid) {
      ++diagInvalid_;
      if (diagInvalid_ <= 8) {
        Serial.printf("FIFO_INVALID,available=%u,index=%u\n", available, i);
      }
      gap_ = true;
      ++lostSamples_;
      continue;
    }

    // FIFO timestamp is a wrapping 16-bit 32 kHz counter value, not a
    // per-record interval. Subtract consecutive counter values with uint16
    // wrap semantics to recover the true ODR interval.
    uint16_t timestampDeltaTicks = 0;
    const uint16_t previousTimestampTicks = lastFifoTimestampTicks_;
    bool timestampPlausible = false;
    if (item.timed) {
      if (!haveLastFifoTimestamp_) {
        timestampPlausible = true;
        timestampDeltaTicks = expectedTicks;
      } else {
        timestampDeltaTicks =
            static_cast<uint16_t>(item.timestamp - lastFifoTimestampTicks_);
        timestampPlausible =
            timestampDeltaTicks >= expectedTicks / 2 &&
            timestampDeltaTicks <= expectedTicks * 8;
      }
      haveLastFifoTimestamp_ = true;
      lastFifoTimestampTicks_ = item.timestamp;
    } else {
      haveLastFifoTimestamp_ = false;
    }

    uint64_t intervalUs = nominalIntervalUs;
    if (timestampPlausible) {
      intervalUs = sensor_.timestampUs(timestampDeltaTicks);
      ++diagTimestamped_;
    } else {
      ++diagTimestampFallback_;
      if (item.timed) {
        ++diagTimestampAnomaly_;
        if (diagTimestampAnomaly_ <= 8) {
          Serial.printf(
              "SENSOR_TIMESTAMP_ANOMALY,raw=%u,previous=%u,delta=%u,"
              "expected=%u,header=0x%02X\n",
              item.timestamp, previousTimestampTicks, timestampDeltaTicks,
              expectedTicks, item.header);
        }
      }
    }

    const uint64_t previousEpoch = lastEpoch_;
    const uint64_t t =
        previousEpoch ? previousEpoch + intervalUs : epoch(local);
    if (previousEpoch && t > previousEpoch) {
      const uint32_t dt = static_cast<uint32_t>(t - previousEpoch);
      if (!diagIntervalCount_ || dt < diagIntervalMin_)
        diagIntervalMin_ = dt;
      if (dt > diagIntervalMax_)
        diagIntervalMax_ = dt;
      diagIntervalSum_ += dt;
      ++diagIntervalCount_;
    }
    ++diagValid_;
    const auto sampleIndex = diagSamplesLogged_++;
    if (sampleIndex < 24 || diagValid_ % 500 == 0) {
      Serial.printf(
          "SAMPLE_DIAG,index=%lu,header=0x%02X,packet_bytes=%u,count_raw=%u,"
          "ts_raw=%u,timed=%u,ts_us=%llu,t=%llu,dt=%lld\n",
          (unsigned long)sampleIndex, item.header, item.packetBytes,
          sensor_.lastFifoCountRaw(), item.timestamp, item.timed ? 1 : 0,
          (unsigned long long)(timestampPlausible ? intervalUs : 0),
          (unsigned long long)t,
          (long long)(previousEpoch ? int64_t(t) - int64_t(previousEpoch) : 0));
    }
    if (!t || t < startEpoch_) {
      continue;
    }
    if (lastEpoch_ && (t <= lastEpoch_ || t - lastEpoch_ > 1500000 / rate_)) {
      gap_ = true;
      flush(true);
    }
    if (count_ &&
        llabs(int64_t(t) - int64_t(firstEpoch_ +
                                   uint64_t(count_) * 1000000 / rate_)) >
            500000 / rate_) {
      flush(true);
    }
    if (!count_) {
      firstEpoch_ = t;
    }
    lastEpoch_ = t;
    samples_[count_++] = {item.accel.x, item.accel.y, item.accel.z};
#ifdef APP_USB_TEST
    Serial.printf("SAMPLE,%u,%lu,%llu,%d,%d,%d\n", Config::nodeId,
                  (unsigned long)run_, (unsigned long long)t, item.accel.x,
                  item.accel.y, item.accel.z);
#endif
    if (count_ == 25) {
      flush();
    }
  }
}

void NodeApp::flush(bool tail) {
  if (!count_)
    return;
#ifndef APP_USB_TEST
  Header h = {};
  h.type = Data;
  h.node = Config::nodeId;
  h.flags = flags() | (tail ? Tail : 0);
  h.run = run_;
  h.sequence = sequence_++;
  h.epochUs = firstEpoch_;
  h.rate = rate_;
  h.count = count_;
  h.range = range_;
  uint8_t packet[180];
  size_t length = encode(h, samples_, count_ * sizeof(Sample), packet);
  if (!queue_.add(packet, length, h.sequence)) {
    ++lostPackets_;
    overflow_ = true;
  }
#endif
  count_ = 0;
}
void NodeApp::report(uint8_t type) {
  Header h = {};
  h.type = type;
  h.node = Config::nodeId;
  h.flags = flags();
  h.run = replyRun_;
  h.rate = rate_;
  h.range = range_;
  h.epochUs = (gps_.healthy() || !APP_REQUIRE_TIME_SYNC) ? epoch(esp_timer_get_time()) : 0;
  StatusPayload p = {clock_.hz(),   clock_.ppm(),    lostSamples_, lostPackets_,
                     queue_.size(), uint8_t(state_), result_};
  uint8_t packet[64];
  size_t length = encode(h, &p, sizeof(p), packet);
#ifndef APP_USB_TEST
  if (!Radio::send(packet, length))
    return;
#else
  Serial.printf("CONFIRM,id=%u,run=%lu,result=%u,state=%u\n", Config::nodeId, (unsigned long)h.run,
                result_, state_);
#endif
  replyPending_ = false;
}
void NodeApp::communicate() {
#ifdef APP_USB_TEST
  if (replyPending_ && int32_t(millis() - replyAt_) >= 0)
    report(Confirm);
  return;
#else
  if (!radioReady_ || Radio::transmitting())
    return;
  const uint64_t now = epoch(esp_timer_get_time());
  uint32_t phase = 0;
  if ((state_ == Capturing || state_ == Draining) && now >= startEpoch_)
    phase = (now - startEpoch_) % Config::cycleUs;
  const bool active = state_ == Capturing || state_ == Draining;
  if (!active) {
    if (replyPending_ && int32_t(millis() - replyAt_) >= 0) {
      if (Radio::channel(uplink_, sf_))
        report(Confirm);
    } else if (int32_t(millis() - lastStatus_) >= 10000) {
      replyRun_ = run_;
      if (Radio::channel(uplink_, sf_))
        report(Status);
      lastStatus_ = millis();
    } else {
      Radio::channel(Config::controlMHz, Config::spreadingFactor);
    }
    return;
  }
  if (phase >= Config::normalUplinkEndUs && phase < Config::downlinkEndUs) {
    ++diagWindow_;
    if (!Radio::channel(Config::controlMHz, Config::spreadingFactor))
      return;
    return;
  }
  if (phase >= Config::downlinkEndUs && phase < Config::responseEndUs) {
    if (replyPending_ && int32_t(millis() - replyAt_) >= 0 && Radio::channel(uplink_, sf_))
      report(Confirm);
    return;
  }
  const bool retry = phase >= Config::responseEndUs;
  auto* item = queue_.next(millis(), retry);
  if (!item)
    return;
  // Leave enough airtime before the common control window.
  const uint32_t airtimeUs = sf_ == 5 ? 120000 : sf_ == 6 ? 190000 : 330000;
  if (!retry && phase + airtimeUs >= Config::normalUplinkEndUs)
    return;
  if (retry && phase + airtimeUs >= Config::cycleUs)
    return;
  if (Radio::channel(uplink_, sf_) && Radio::send(item->bytes, item->length)) {
    item->sent = true;
    item->lastSent = millis();
  }
#endif
}
