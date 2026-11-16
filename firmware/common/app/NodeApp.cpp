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
portMUX_TYPE edgeMux = portMUX_INITIALIZER_UNLOCKED;
volatile uint64_t edgeTimes[256];
volatile uint16_t edgeHead = 0, edgeTail = 0;
volatile uint32_t edgeOverruns = 0, edgeTotal = 0;
volatile uint64_t edgeLastUs = 0;
void IRAM_ATTR dataReady() {
  const uint64_t now = esp_timer_get_time();
  portENTER_CRITICAL_ISR(&edgeMux);
  // 50 Hz is 20 ms and 100 Hz is 10 ms apart. Ringing closer than 2 ms is
  // electrical noise, not a new sample.
  if (now - edgeLastUs >= 2000) {
    edgeLastUs = now;
    uint16_t next = (edgeHead + 1) % 256;
    if (next == edgeTail)
      ++edgeOverruns;
    else {
      edgeTimes[edgeHead] = now;
      edgeHead = next;
    }
    ++edgeTotal;
  }
  portEXIT_CRITICAL_ISR(&edgeMux);
}
bool popEdge(uint64_t& time) {
  portENTER_CRITICAL(&edgeMux);
  bool ok = edgeTail != edgeHead;
  if (ok) {
    time = edgeTimes[edgeTail];
    edgeTail = (edgeTail + 1) % 256;
  }
  portEXIT_CRITICAL(&edgeMux);
  return ok;
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
  portENTER_CRITICAL(&edgeMux);
  edgeHead = edgeTail = 0;
  edgeOverruns = 0;
  portEXIT_CRITICAL(&edgeMux);
}
void NodeApp::begin() {
  Serial.begin(Config::serialBaud);
  delay(5000);  // Allow the USB diagnostic reader to attach after upload.
  Board::begin();
  Serial.printf("BOOT,version=%s,board=%s,sensor=%s,id=%u,uplink=%.1f,clock=32000,packet=180\n",
                APP_FIRMWARE_VERSION, Board::name, sensor_.name(), Config::nodeId,
                Config::uplinkMHz);
  clockOutputReady_ = clock_.begin(Board::clockOut);
  if (clockOutputReady_) {
    // Let the LEDC output and PCNT input path settle before measuring.
    delay(100);
    clockWatchEdges_ = clock_.edgeCount();
    clockWatchMs_ = millis();
  }
  Serial.printf("CLOCK_OUTPUT,ready=%u\n", clockOutputReady_ ? 1 : 0);
  gps_.setPpsObserver(SampleClock::onPps);
#if defined(APP_BOARD_EORA) && defined(APP_SENSOR_IIM42652)
  sensorReady_ = initializeSensor(false);
#else
  sensorReady_ = initializeSensor(true);
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
  state_ = clockOutputReady_ && sensorReady_ && radioReady_ ? Waiting : Fault;
  Serial.printf("READY,sensor=%u,radio=%u,clock_output=%u,clock_measurement=wait_60_pps\n",
                sensorReady_, radioReady_, clockOutputReady_);
  lastStatus_ = millis() + ((Config::nodeId - 1001) % 16) * 120;
}
bool NodeApp::initializeSensor(bool resetDevice) {
  // Rebind only the sensor SPI bus; do not restart the clock, GNSS, or radio.
  detachSensorInterrupt();
  resetEdges();
  // Internal pull-down keeps a floating INT1 line from generating a false edge storm.
  pinMode(Board::sensorInt, INPUT_PULLDOWN);
  // The sensor requires a live CLKIN in RTC mode. Verify the pin actually
  // toggles before vendor init, and re-route once if it does not.
  bool clockAlive = clockOutputReady_ && clock_.verifyOutput(10, 240, 400);
  if (!clockAlive && clockOutputReady_) {
    Serial.println("CLOCK_VERIFY_FAILED,recover=1");
    clockAlive = clock_.recover() && clock_.verifyOutput(10, 240, 400);
    clockOutputReady_ = clockAlive;
  }
  const bool initialized = clockAlive && sensor_.begin(resetDevice);
  bool passed = false;
  SensorChecks::StartupCheck last = {};
  if (initialized) {
    // A marginal INT1 line can fail one window and pass the next. Retry the
    // whole start/measure cycle before declaring the sensor faulty.
    for (uint8_t attempt = 1; attempt <= 3 && !passed; ++attempt) {
      SensorChecks::StartupCheck check = {};
      detachSensorInterrupt();
      resetEdges();
      sensor_.flush();
      attachInterrupt(digitalPinToInterrupt(Board::sensorInt), dataReady, RISING);
      intAttached_ = true;
      sensor_.start();
      // Ignore the FIFO/INT1 transient created while the sensor switches into
      // low-noise mode. Measure only a stable window after the transient.
      delay(100);
      resetEdges();
      const uint32_t before = edgeTotal;
      const uint64_t clockBefore = clock_.edgeCount();
      delay(120);
      check.dataReadyEdges = edgeTotal - before;
      check.clockEdges = clock_.edgeCount() - clockBefore;
      check.fifoSamples = sensor_.available();
      check.fifoCountValid = sensor_.fifoCountValid();
      if (check.fifoSamples && check.fifoCountValid) {
        AccelSample sample = {};
        check.sampleValid = sensor_.read(sample);
        Serial.printf("SENSOR_SAMPLE,valid=%u,x=%d,y=%d,z=%d\n", check.sampleValid,
                      sample.x, sample.y, sample.z);
      }
      check.identityValid = sensor_.readRegister(IimRegister::WHO_AM_I) ==
                            sensor_.expectedIdentity();
      sensor_.standby();
      detachSensorInterrupt();
      resetEdges();
      passed = SensorChecks::startupPassed(check);
      last = check;
      Serial.printf("SENSOR_CHECK_ATTEMPT,attempt=%u,drdy=%lu,fifo_samples=%u,"
                    "clock_edges=%llu,sample_valid=%u,identity_ok=%u,ok=%u\n",
                    attempt, (unsigned long)check.dataReadyEdges, check.fifoSamples,
                    (unsigned long long)check.clockEdges, check.sampleValid,
                    check.identityValid, passed ? 1 : 0);
      if (!passed)
        delay(50);
    }
  }
  Serial.printf("SENSOR_CHECK,init=%u,executed=%u,drdy=%lu,fifo_samples=%u,"
                "clock_edges=%llu,sample_valid=%u,identity_ok=%u,fifo_bytes=%u,"
                "int1_level=%u,ok=%u\n", initialized, initialized,
                (unsigned long)last.dataReadyEdges, last.fifoSamples,
                (unsigned long long)last.clockEdges, last.sampleValid, last.identityValid,
                sensor_.lastFifoBytes(), digitalRead(Board::sensorInt), passed);
  if (!passed) {
    detachSensorInterrupt();
    resetEdges();
    Serial.println("SENSOR_FAULT,drdy_interrupt_disabled=1,capture_blocked=1");
  }
  return passed;
}
void NodeApp::sensorSampleTest() {
  if (!SensorChecks::canRetry(state_, sensorActive_, queue_.size()) ||
      !clockOutputReady_) {
    Serial.println("SENSOR_SAMPLE_TEST,ok=0,reason=not_idle");
    return;
  }
  detachSensorInterrupt();
  resetEdges();
  sensorReady_ = false;
  state_ = Fault;
  const bool isolated = clock_.setOutputEnabled(false);
  Serial.printf("SENSOR_SAMPLE_TEST_BEGIN,clock=internal,rate=50,range_g=4,"
                "target=20,synchronized=0,sensor_reset=0,clkin_output_high_z=%u\n",
                isolated);
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
                      (unsigned long long)esp_timer_get_time(),sample.x,sample.y,sample.z);
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
  // Restore the clock buffer before selecting external RTC mode again.
  clockOutputReady_ = clock_.setOutputEnabled(true);
  // Ignore the output-matrix reconnect transient.
  delay(100);
  const uint64_t restoredBefore = clock_.edgeCount();
  delayMicroseconds(2000);
  const uint64_t restoredEdges = clock_.edgeCount() - restoredBefore;
  clockOutputReady_ = clockOutputReady_ && restoredEdges >= 32 && restoredEdges <= 128;
  Serial.printf("SENSOR_CLOCK_RESTORE,output_enabled=%u,edges_2ms=%llu,"
                "pps_recalibration=1\n", clockOutputReady_,
                (unsigned long long)restoredEdges);
  // The internal-clock diagnostic never authorizes synchronized acquisition.
  sensorReady_ = initializeSensor(false);
  state_ = clockOutputReady_ && sensorReady_ && radioReady_ ? Waiting : Fault;
  Serial.printf("SENSOR_SAMPLE_TEST_RESTORE,external_clock=1,sensor_ready=%u,state=%u\n",
                sensorReady_,state_);
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
  return (gps_.healthy() ? GpsHealthy : 0) | (clock_.healthy() ? ClockHealthy : 0) |
         (gap_ ? SampleGap : 0) | (overflow_ ? QueueOverflow : 0);
}
void NodeApp::tick() {
  gps_.update();
  clock_.update(gps_.healthy());
  if (gps_.healthy()) {
    anchorEpoch_ = gps_.epochUsAtLastPps();
    anchorLocal_ = gps_.localUs64AtLastPps();
  }
#ifndef APP_USB_TEST
  if (radioReady_)
    Radio::update();
#endif
  // Clock watchdog: the LEDC loopback must keep producing ~32 kHz. Two bad
  // 1 s windows in a row trigger one full route restore.
  if (clockOutputReady_ && state_ != WireProtocol::Fault &&
      millis() - clockWatchMs_ >= 1000) {
    const uint64_t edgesNow = clock_.edgeCount();
    const uint64_t delta = edgesNow - clockWatchEdges_;
    clockWatchEdges_ = edgesNow;
    clockWatchMs_ = millis();
    if (delta < 24000 || delta > 40000) {
      ++clockWatchFaults_;
      Serial.printf("CLOCK_WATCHDOG,edges_1s=%llu,faults=%u\n",
                    (unsigned long long)delta, clockWatchFaults_);
      if (clockWatchFaults_ >= 2) {
        const bool recovered = clock_.recover() && clock_.verifyOutput(10, 240, 400);
        Serial.printf("CLOCK_RECOVER,ok=%u\n", recovered ? 1 : 0);
        clockWatchFaults_ = 0;
        if (!recovered) {
          clockOutputReady_ = false;
          sensorReady_ = false;
          state_ = WireProtocol::Fault;
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
    if (APP_REQUIRE_TIME_SYNC && (!gps_.healthy() || !clock_.healthy())) {
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
    const uint64_t edgesNow = clock_.edgeCount();
    Serial.printf("FLOW,edge_delta=%lu,valid=%lu,invalid=%lu,no_edge=%lu,resets=%lu,fifo_bytes=%u,rx=%lu,ack=%lu,stop=%lu,window=%lu,clock_delta=%llu,phase=%lu\n",
      (unsigned long)(edgeTotal - diagEdges_), (unsigned long)diagValid_, (unsigned long)diagInvalid_,
      (unsigned long)diagNoEdge_, (unsigned long)diagResets_, sensor_.lastFifoBytes(),
      (unsigned long)diagRx_, (unsigned long)diagAck_, (unsigned long)diagStop_, (unsigned long)diagWindow_,
      (unsigned long long)(edgesNow - diagLastClock_),
      (unsigned long)(now >= startEpoch_ ? (now - startEpoch_) % Config::cycleUs : 0));
    diagEdges_ = edgeTotal; diagLastClock_ = edgesNow;
    Serial.printf(
        "STATUS,id=%u,state=%u,gps=%u,clock_hz=%.4f,ppm=%.2f,clock_ok=%u,queue=%u,lost_samples=%lu,"
        "lost_packets=%lu\n",
        Config::nodeId, state_, gps_.healthy(), clock_.hz(), clock_.ppm(), clock_.healthy(),
        queue_.size(), (unsigned long)lostSamples_, (unsigned long)lostPackets_);
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
      (APP_REQUIRE_TIME_SYNC && (!gps_.healthy() || !clock_.healthy()))) {
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
  result_ = Accepted;
  state_ = Armed;
}
void NodeApp::stop() {
  if (state_ == Fault && !sensorActive_)
    return;  // STOP must never turn a failed sensor into "ready".
  if (sensorActive_) {
    acquire();
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
      if (clockOutputReady_)
        sensor_.diagnostic();
      else
        Serial.println("SENSOR_ERROR,stage=clock_output");
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_SAMPLE_TEST")) {
      sensorSampleTest();
      continue;
    }
    if (!strcmp(serialLine_, "SENSOR_LINK_TEST")) {
      if (!clockOutputReady_ || !SensorChecks::canRetry(state_, sensorActive_, queue_.size())) {
        Serial.println("SENSOR_LINK_TEST,ok=0,reason=not_idle");
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
      sensorReady_ = initializeSensor(false);
      state_ = clockOutputReady_ && sensorReady_ && radioReady_ ? Waiting : Fault;
      Serial.printf("SENSOR_RETRY,ok=%u,state=%u,reset=0\n", sensorReady_, state_);
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
          "COMMANDS: DIAG; SENSOR_RETRY; SENSOR_SAMPLE_TEST; SENSOR_LINK_TEST; START_AT epoch_us 50|100 [range_g] [run_id] [duration_s]; STOP; STATUS");
  }
}
void NodeApp::acquire() {
  uint32_t overruns;
  portENTER_CRITICAL(&edgeMux);
  overruns = edgeOverruns;
  edgeOverruns = 0;
  portEXIT_CRITICAL(&edgeMux);
  const uint16_t available = sensor_.available();
  if (!sensor_.fifoCountValid()) {
    ++diagResets_;
    gap_ = true;
    sensorReady_ = false;
    sensor_.standby();
    sensorActive_ = false;
    flush(true);
    state_ = Fault;
    detachSensorInterrupt();
    Serial.printf("SENSOR_FAULT,invalid_fifo_bytes=%u,capture_blocked=1\n", sensor_.lastFifoBytes());
    return;
  }
  if (overruns || available > 240) {
    ++diagResets_;
    if (diagResets_ <= 8) Serial.printf("FIFO_RESET,overruns=%lu,bytes=%u\n", (unsigned long)overruns, sensor_.lastFifoBytes());
    // IRQ queue overruns count electrical edges, not sensor samples. Count
    // only the bounded FIFO records actually discarded; mark the gap.
    lostSamples_ += available;
    gap_ = true;
    flush(true);
    sensor_.flush();
    resetEdges();
    return;
  }
  for (uint16_t i = 0; i < available; ++i) {
    AccelSample s;
    const bool valid = sensor_.read(s);
    uint64_t local;
    if (!popEdge(local)) {
      ++diagNoEdge_;
      gap_ = true;
      ++lostSamples_;
      continue;
    }
    if (!valid) {
      ++diagInvalid_;
      if (diagInvalid_ <= 8) Serial.printf("FIFO_INVALID,header=0x%02X,bytes=%u,available=%u,index=%u\n", sensor_.lastHeader(), sensor_.lastFifoBytes(), available, i);
      gap_ = true;
      ++lostSamples_;
      continue;
    }
    ++diagValid_;
    const uint64_t t = epoch(local);
    if (diagSamplesLogged_++ < 16) Serial.printf("SAMPLE_DIAG,header=0x%02X,bytes=%u,t=%llu,dt=%lld,count=%u\n", sensor_.lastHeader(), sensor_.lastFifoBytes(), (unsigned long long)t, (long long)(lastEpoch_ ? int64_t(t)-int64_t(lastEpoch_) : 0), count_);
    if (!t || t < startEpoch_)
      continue;
    if (lastEpoch_ && (t <= lastEpoch_ || t - lastEpoch_ > 1500000 / rate_)) {
      gap_ = true;
      flush(true);
    }
    if (count_ && llabs(int64_t(t) - int64_t(firstEpoch_ + uint64_t(count_) * 1000000 / rate_)) >
                      500000 / rate_)
      flush(true);
    if (!count_)
      firstEpoch_ = t;
    lastEpoch_ = t;
    samples_[count_++] = {s.x, s.y, s.z};
#ifdef APP_USB_TEST
    Serial.printf("SAMPLE,%u,%lu,%llu,%d,%d,%d\n", Config::nodeId, (unsigned long)run_,
                  (unsigned long long)t, s.x, s.y, s.z);
#endif
    if (count_ == 25)
      flush();
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
