#include "NodeApp.h"

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
void IRAM_ATTR dataReady() {
  const uint64_t now = esp_timer_get_time();
  portENTER_CRITICAL_ISR(&edgeMux);
  uint16_t next = (edgeHead + 1) % 256;
  if (next == edgeTail)
    ++edgeOverruns;
  else {
    edgeTimes[edgeHead] = now;
    edgeHead = next;
  }
  ++edgeTotal;
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
void NodeApp::resetEdges() {
  portENTER_CRITICAL(&edgeMux);
  edgeHead = edgeTail = 0;
  edgeOverruns = 0;
  portEXIT_CRITICAL(&edgeMux);
}
void NodeApp::begin() {
  Serial.begin(Config::serialBaud);
  delay(1000);
  Board::begin();
  Serial.printf("BOOT,version=%s,board=%s,sensor=%s,id=%u,clock=32000,packet=180\n",
                APP_FIRMWARE_VERSION, Board::name, sensor_.name(), Config::nodeId);
  const bool clockOk = clock_.begin(Board::clockOut);
  gps_.setPpsObserver(SampleClock::onPps);
  pinMode(Board::sensorInt, INPUT);
  attachInterrupt(digitalPinToInterrupt(Board::sensorInt), dataReady, RISING);
  if (clockOk && sensor_.begin()) {
    const uint32_t before = edgeTotal;
    sensor_.start();
    delay(120);
    sensorReady_ = edgeTotal > before && sensor_.available() > 0;
    sensor_.standby();
    resetEdges();
  }
  gps_.begin(Board::gpsRx, Board::gpsTx, Board::gpsPps, Config::gpsBaud);
#ifndef APP_USB_TEST
  radioReady_ = Radio::begin();
#else
  radioReady_ = true;
#endif
  state_ = clockOk && sensorReady_ && radioReady_ ? Waiting : Fault;
  Serial.printf("READY,sensor=%u,radio=%u,clock_output=%u,clock_measurement=wait_60_pps\n",
                sensorReady_, radioReady_, clockOk);
  lastStatus_ = millis() + ((Config::nodeId - 1001) % 16) * 120;
}
uint64_t NodeApp::epoch(uint64_t local) const {
  if (!anchorEpoch_)
    return 0;
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
  serialCommands();
  commands();
  const uint64_t now = epoch(esp_timer_get_time());
  if (state_ == Armed && now && now + Config::warmupUs >= startEpoch_) {
    if (!gps_.healthy() || !clock_.healthy()) {
      result_ = NotReady;
      state_ = Waiting;
      replyPending_ = true;
      replyAt_ = millis();
    } else {
      sensor_.flush();
      resetEdges();
      sensor_.start();
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
  replyAt_ = millis() + ((Config::nodeId - 1001) % 16) * 110;
  if (h.run == run_ && run_) {
    result_ = Accepted;
    return;
  }
  if (state_ == Capturing || state_ == Armed || state_ == Draining) {
    result_ = Busy;
    return;
  }
  if (state_ == Fault || !sensorReady_ || !gps_.healthy() || !clock_.healthy()) {
    result_ = NotReady;
    return;
  }
  if ((h.rate != 50 && h.rate != 100) ||
      (h.range != 2 && h.range != 4 && h.range != 8 && h.range != 16) || options.sf < 5 ||
      options.sf > 7 ||
      (options.uplinkMHz != 0 && (options.uplinkMHz < 470 || options.uplinkMHz > 493))) {
    result_ = BadSettings;
    return;
  }
  const uint64_t now = epoch(esp_timer_get_time());
  if (!h.run || h.epochUs < now + 1000000) {
    result_ = TooLate;
    return;
  }
  if (!sensor_.configure(h.rate, h.range)) {
    result_ = BadSettings;
    return;
  }
  run_ = h.run;
  rate_ = h.rate;
  range_ = h.range;
  sf_ = options.sf;
  uplink_ = options.uplinkMHz ? options.uplinkMHz : Config::uplinkMHz;
  startEpoch_ = h.epochUs;
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
  if (sensorActive_) {
    acquire();
    sensor_.standby();
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
  if (h.type == Start && size == sizeof(StartOptions)) {
    StartOptions o;
    memcpy(&o, p, sizeof(o));
    const uint16_t bit = uint16_t(1) << ((Config::nodeId - 1001) % 16);
    if (h.node == broadcast && !(o.nodeMask & bit))
      return;
    acceptStart(h, o);
  } else if (h.type == Stop && (h.run == run_ || h.run == 0)) {
    stop();
    replyRun_ = h.run;
    result_ = Accepted;
    replyPending_ = true;
    replyAt_ = millis() + ((Config::nodeId - 1001) % 16) * 110;
  } else if (h.type == Discover) {
    replyRun_ = h.run;
    replyPending_ = true;
    replyAt_ = millis() + ((Config::nodeId - 1001) % 16) * 110;
  } else if (h.type == Ack && h.run == run_ && size % sizeof(AckEntry) == 0) {
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
      StartOptions options = {Config::uplinkMHz, uint32_t(duration), 6, 0, 0xffff};
      acceptStart(h, options);
    } else
      Serial.println(
          "COMMANDS: START_AT epoch_us 50|100 [range_g] [run_id] [duration_s]; STOP; STATUS");
  }
}
void NodeApp::acquire() {
  uint32_t overruns;
  portENTER_CRITICAL(&edgeMux);
  overruns = edgeOverruns;
  edgeOverruns = 0;
  portEXIT_CRITICAL(&edgeMux);
  if (overruns || sensor_.available() > 240) {
    lostSamples_ += overruns ? overruns : sensor_.available();
    gap_ = true;
    flush(true);
    sensor_.flush();
    resetEdges();
    return;
  }
  uint16_t available = sensor_.available();
  for (uint16_t i = 0; i < available; ++i) {
    AccelSample s;
    const bool valid = sensor_.read(s);
    uint64_t local;
    if (!popEdge(local)) {
      gap_ = true;
      ++lostSamples_;
      continue;
    }
    if (!valid) {
      gap_ = true;
      ++lostSamples_;
      continue;
    }
    const uint64_t t = epoch(local);
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
  h.epochUs = gps_.healthy() ? epoch(esp_timer_get_time()) : 0;
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
    phase = (now - startEpoch_) % 5000000;
  const bool active = state_ == Capturing || state_ == Draining;
  const bool control = !active || (phase >= 4000000 && phase < 4500000);
  if (control) {
    if (!Radio::channel(Config::controlMHz, Config::spreadingFactor))
      return;
    if (replyPending_ && int32_t(millis() - replyAt_) >= 0)
      report(Confirm);
    else if (!active && int32_t(millis() - lastStatus_) >= 10000) {
      replyRun_ = run_;
      report(Status);
      lastStatus_ = millis();
    }
    return;
  }
  const bool retry = phase >= 4500000;
  auto* item = queue_.next(millis(), retry);
  if (!item)
    return;
  // Leave enough airtime before the common control window.
  const uint32_t airtimeUs = sf_ == 5 ? 120000 : sf_ == 6 ? 190000 : 330000;
  if (!retry && phase + airtimeUs >= 4000000)
    return;
  if (retry && phase + airtimeUs >= 5000000)
    return;
  if (Radio::channel(uplink_, sf_) && Radio::send(item->bytes, item->length)) {
    item->sent = true;
    item->lastSent = millis();
  }
#endif
}
