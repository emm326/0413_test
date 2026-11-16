#include "SampleClock.h"

#include <driver/gpio.h>
#include <driver/ledc.h>
#include <hal/gpio_ll.h>
#include <driver/pcnt.h>
#include <soc/pcnt_struct.h>

#include "Config.h"
namespace {
constexpr ledc_mode_t kSpeedMode = LEDC_LOW_SPEED_MODE;
constexpr ledc_timer_t kTimer = LEDC_TIMER_0;
constexpr ledc_channel_t kChannel = LEDC_CHANNEL_0;
constexpr int kCounterHighLimit = 30000;
// 32 kHz over 10 ms is 320 edges. The window accepts 240..400 so a floating
// line (hundreds of kHz of noise) or a stopped line (zero) both fail.
constexpr uint32_t kSelfTestMs = 10;
constexpr uint32_t kSelfTestMin = 240;
constexpr uint32_t kSelfTestMax = 400;

portMUX_TYPE counterMux = portMUX_INITIALIZER_UNLOCKED;
portMUX_TYPE ppsMux = portMUX_INITIALIZER_UNLOCKED;
volatile uint64_t counterBase = 0, ppsEdges = 0, ppsLocal = 0;
volatile uint32_t ppsSerial = 0;
bool counterConfigured = false;
bool overflowIsrInstalled = false;
}  // namespace
uint64_t IRAM_ATTR SampleClock::edges() {
  portENTER_CRITICAL_ISR(&counterMux);
  uint32_t before, after, value;
  do {
    before = PCNT.int_raw.val & 1;
    value = PCNT.cnt_unit[0].val & 0xffff;
    after = PCNT.int_raw.val & 1;
  } while (before != after);
  if (after) {
    counterBase += kCounterHighLimit;
    PCNT.int_clr.val = 1;
  }
  const uint64_t total = counterBase + value;
  portEXIT_CRITICAL_ISR(&counterMux);
  return total;
}
void IRAM_ATTR SampleClock::onOverflow(void*) {
  portENTER_CRITICAL_ISR(&counterMux);
  if (PCNT.int_raw.val & 1) {
    counterBase += kCounterHighLimit;
    PCNT.int_clr.val = 1;
  }
  portEXIT_CRITICAL_ISR(&counterMux);
}
void IRAM_ATTR SampleClock::onPps(uint64_t localUs) {
  const uint64_t count = edges();
  portENTER_CRITICAL_ISR(&ppsMux);
  ppsEdges = count;
  ppsLocal = localUs;
  ++ppsSerial;
  portEXIT_CRITICAL_ISR(&ppsMux);
}
bool SampleClock::configureCounter(int pin) {
  if (!counterConfigured) {
    pcnt_config_t counter = {};
    counter.pulse_gpio_num = pin;
    counter.ctrl_gpio_num = PCNT_PIN_NOT_USED;
    counter.pos_mode = PCNT_COUNT_INC;
    counter.neg_mode = PCNT_COUNT_DIS;
    counter.lctrl_mode = PCNT_MODE_KEEP;
    counter.hctrl_mode = PCNT_MODE_KEEP;
    counter.counter_h_lim = kCounterHighLimit;
    counter.counter_l_lim = -1;
    counter.unit = PCNT_UNIT_0;
    counter.channel = PCNT_CHANNEL_0;
    if (pcnt_unit_config(&counter) != ESP_OK)
      return false;
    counterConfigured = true;
  }
  if (pcnt_counter_pause(PCNT_UNIT_0) != ESP_OK || pcnt_counter_clear(PCNT_UNIT_0) != ESP_OK)
    return false;
  if (pcnt_event_enable(PCNT_UNIT_0, PCNT_EVT_H_LIM) != ESP_OK)
    return false;
  if (!overflowIsrInstalled) {
    if (pcnt_isr_register(onOverflow, nullptr, ESP_INTR_FLAG_IRAM, nullptr) != ESP_OK)
      return false;
    overflowIsrInstalled = true;
  }
  return pcnt_intr_enable(PCNT_UNIT_0) == ESP_OK;
}
bool SampleClock::attachOutput(int pin) {
  // ledc_channel_config drives the pad and routes the LEDC signal. Re-running it
  // is the only restore path that reproduces the known-good boot sequence;
  // gpio_ll_output_disable detaches the ESP32-S3 output matrix and must not be
  // used on this pin.
  ledc_channel_config_t channel = {};
  channel.gpio_num = pin;
  channel.speed_mode = kSpeedMode;
  channel.channel = kChannel;
  channel.timer_sel = kTimer;
  channel.intr_type = LEDC_INTR_DISABLE;
  channel.duty = 1;
  channel.hpoint = 0;
  if (ledc_channel_config(&channel) != ESP_OK)
    return false;
  // Keep the input path enabled so the same pin feeds PCNT for verification.
  gpio_ll_input_enable(&GPIO, static_cast<gpio_num_t>(pin));
  if (ledc_timer_resume(kSpeedMode, kTimer) != ESP_OK)
    return false;
  return ledc_update_duty(kSpeedMode, kChannel) == ESP_OK;
}
bool SampleClock::begin(int pin) {
  if (pin < 0)
    return false;
  ledc_timer_config_t timer = {};
  timer.speed_mode = kSpeedMode;
  timer.duty_resolution = LEDC_TIMER_1_BIT;
  timer.timer_num = kTimer;
  timer.freq_hz = Config::clockHz;
  timer.clk_cfg = LEDC_USE_XTAL_CLK;
  if (ledc_timer_config(&timer) != ESP_OK)
    return false;
  if (!configureCounter(pin))
    return false;
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    if (!attachOutput(pin)) {
      Serial.printf("CLOCK_SELFTEST,attempt=%u,attach=0\n", attempt);
      delay(5);
      continue;
    }
    if (pcnt_counter_resume(PCNT_UNIT_0) != ESP_OK)
      return false;
    delay(2);
    const uint64_t before = edges();
    delay(kSelfTestMs);
    const uint64_t delta = edges() - before;
    const bool ok = delta >= kSelfTestMin && delta <= kSelfTestMax;
    Serial.printf("CLOCK_SELFTEST,attempt=%u,edges_10ms=%llu,ok=%u\n", attempt,
                  (unsigned long long)delta, ok ? 1 : 0);
    if (ok) {
      outputPin_ = pin;
      return true;
    }
    pcnt_counter_pause(PCNT_UNIT_0);
    ledc_timer_pause(kSpeedMode, kTimer);
    delay(5);
  }
  return false;
}
bool SampleClock::verifyOutput(uint32_t windowMs, uint32_t minEdges, uint32_t maxEdges) {
  if (outputPin_ < 0 || windowMs == 0)
    return false;
  const uint64_t before = edges();
  delay(windowMs);
  const uint64_t delta = edges() - before;
  return delta >= minEdges && delta <= maxEdges;
}
bool SampleClock::recover() {
  if (outputPin_ < 0)
    return false;
  if (!attachOutput(outputPin_))
    return false;
  return pcnt_counter_resume(PCNT_UNIT_0) == ESP_OK;
}
bool SampleClock::setOutputEnabled(bool enabled) {
  if (outputPin_ < 0)
    return false;
  if (enabled) {
    if (!attachOutput(outputPin_))
      return false;
    if (pcnt_counter_resume(PCNT_UNIT_0) != ESP_OK)
      return false;
  } else {
    // Stop the carrier but keep the GPIO matrix route attached. A paused LEDC
    // output holds its last level instead of floating the CLKIN line.
    if (ledc_timer_pause(kSpeedMode, kTimer) != ESP_OK)
      return false;
  }
  measured_ = false;
  seconds_ = 0;
  lastLocal_ = 0;
  portENTER_CRITICAL(&ppsMux);
  consumed_ = ppsSerial;
  portEXIT_CRITICAL(&ppsMux);
  return true;
}
void SampleClock::update(bool ppsHealthy) {
  uint32_t serial;
  uint64_t count, local;
  portENTER_CRITICAL(&ppsMux);
  serial = ppsSerial;
  count = ppsEdges;
  local = ppsLocal;
  portEXIT_CRITICAL(&ppsMux);
  if (!ppsHealthy) {
    seconds_ = 0;
    lastLocal_ = 0;
    measured_ = false;
    consumed_ = serial;
    return;
  }
  if (serial == consumed_)
    return;
  const bool consecutive = serial == consumed_ + 1;
  consumed_ = serial;
  if (!lastLocal_ || !consecutive || local - lastLocal_ < 900000 || local - lastLocal_ > 1100000) {
    firstEdges_ = count;
    seconds_ = 0;
    measured_ = false;
    lastLocal_ = local;
    return;
  }
  lastLocal_ = local;
  if (++seconds_ >= 60) {
    measuredHz_ = float(count - firstEdges_) / seconds_;
    ppm_ = (measuredHz_ / Config::clockHz - 1.0f) * 1000000.0f;
    measured_ = true;
    firstEdges_ = count;
    seconds_ = 0;
  }
}
