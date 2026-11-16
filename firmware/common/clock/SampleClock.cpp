#include "SampleClock.h"

#include <driver/gpio.h>
#include <driver/mcpwm.h>
#include <driver/pcnt.h>
#include <hal/gpio_ll.h>
#include <soc/pcnt_struct.h>

#include "Config.h"
namespace {
// The 32 kHz CLKIN carrier comes from MCPWM unit 0/timer 0. The APB clock is
// 80 MHz, so 32 kHz is an exact divide-by-2500. This replaces the earlier LEDC
// setup, whose pause/resume sequence left the output randomly stopped.
constexpr mcpwm_unit_t kUnit = MCPWM_UNIT_0;
constexpr mcpwm_timer_t kTimer = MCPWM_TIMER_0;
constexpr mcpwm_generator_t kGenerator = MCPWM_GEN_A;
constexpr mcpwm_io_signals_t kSignal = MCPWM0A;
constexpr int kCounterHighLimit = 30000;
// 32 kHz over 50 ms is 1600 edges. The window accepts 1200..2000 so a
// floating line, a stopped line, and a partially started carrier all fail.
constexpr uint32_t kSelfTestMs = 50;
constexpr uint32_t kSelfTestMin = 1200;
constexpr uint32_t kSelfTestMax = 2000;

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

uint32_t SampleClock::pollEdges(uint32_t windowUs) {
  if (outputPin_ < 0 || windowUs == 0) {
    return 0;
  }
  const gpio_num_t pin = static_cast<gpio_num_t>(outputPin_);
  int last = gpio_get_level(pin);
  uint32_t edgesSeen = 0;
  const uint32_t started = micros();
  while (static_cast<uint32_t>(micros() - started) < windowUs) {
    const int level = gpio_get_level(pin);
    if (last == 0 && level != 0) {
      ++edgesSeen;
    }
    last = level;
  }
  return edgesSeen;
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
  if (pcnt_counter_pause(PCNT_UNIT_0) != ESP_OK ||
      pcnt_counter_clear(PCNT_UNIT_0) != ESP_OK)
    return false;
  // A stale latched H_LIM flag would make every edges() call add 30000.
  PCNT.int_clr.val = 1;
  if (pcnt_event_enable(PCNT_UNIT_0, PCNT_EVT_H_LIM) != ESP_OK)
    return false;
  if (!overflowIsrInstalled) {
    if (pcnt_isr_register(onOverflow, nullptr, ESP_INTR_FLAG_IRAM, nullptr) != ESP_OK)
      return false;
    overflowIsrInstalled = true;
  }
  return pcnt_intr_enable(PCNT_UNIT_0) == ESP_OK;
}

bool SampleClock::startCarrier(int pin) {
  // mcpwm_gpio_init() routes the generator to the pad and switches the pad to
  // output, which also clears the input enable. Re-enable the input path so
  // the same pin still feeds PCNT for closed-loop verification.
  if (mcpwm_gpio_init(kUnit, kSignal, pin) != ESP_OK)
    return false;
  gpio_ll_input_enable(&GPIO, static_cast<gpio_num_t>(pin));
  mcpwm_config_t config = {};
  config.frequency = Config::clockHz;
  config.cmpr_a = 50.0f;
  config.cmpr_b = 0.0f;
  config.duty_mode = MCPWM_DUTY_MODE_0;
  config.counter_mode = MCPWM_UP_COUNTER;
  // mcpwm_init() applies the frequency/duty and starts the timer. Running it
  // on every start/recovery rebuilds the complete timer state instead of
  // relying on a resume flag left over from a previous run.
  if (mcpwm_init(kUnit, kTimer, &config) != ESP_OK)
    return false;
  return mcpwm_start(kUnit, kTimer) == ESP_OK;
}

bool SampleClock::restartCarrier() {
  if (outputPin_ < 0)
    return false;
  if (pcnt_counter_pause(PCNT_UNIT_0) != ESP_OK)
    return false;
  if (pcnt_counter_clear(PCNT_UNIT_0) != ESP_OK)
    return false;
  PCNT.int_clr.val = 1;
  mcpwm_stop(kUnit, kTimer);
  delayMicroseconds(200);
  const bool started = startCarrier(outputPin_);
  // Resume the counter even when the carrier rebuild fails. Leaving PCNT
  // paused would make every later clock reading look like a dead output.
  if (pcnt_counter_resume(PCNT_UNIT_0) != ESP_OK)
    return false;
  delay(2);
  return started;
}

bool SampleClock::begin(int pin) {
  if (pin < 0)
    return false;
  outputPin_ = pin;
  // Let MCPWM own the pad from the start. A prior GPIO output test left the
  // pad direction in INPUT_OUTPUT and could conflict with the timer route.
  padOk_ = false;
  if (!configureCounter(pin))
    return false;
  for (uint8_t attempt = 1; attempt <= 3; ++attempt) {
    if (!startCarrier(pin)) {
      Serial.printf("CLOCK_SELFTEST,attempt=%u,start=0\n", attempt);
      delay(5);
      continue;
    }
    if (pcnt_counter_resume(PCNT_UNIT_0) != ESP_OK)
      return false;
    delay(2);
    const uint64_t before = edges();
    const uint32_t polled = pollEdges(kSelfTestMs * 1000U);
    const uint64_t delta = edges() - before;
    const bool pcntOk = delta >= kSelfTestMin && delta <= kSelfTestMax;
    const bool pollOk = polled >= kSelfTestMin && polled <= kSelfTestMax;
    const bool ok = pcntOk || pollOk;
    Serial.printf("CLOCK_SELFTEST,attempt=%u,pcnt_%lums=%llu,"
                  "poll_%lums=%lu,pcnt_ok=%u,poll_ok=%u,ok=%u\n",
                  attempt, (unsigned long)kSelfTestMs,
                  (unsigned long long)delta, (unsigned long)kSelfTestMs,
                  (unsigned long)polled, pcntOk ? 1 : 0, pollOk ? 1 : 0,
                  ok ? 1 : 0);
    if (ok) {
      padOk_ = true;
      return true;
    }
    mcpwm_stop(kUnit, kTimer);
    pcnt_counter_pause(PCNT_UNIT_0);
    delay(5);
  }
  return false;
}

uint32_t SampleClock::measureEdges(uint32_t windowMs) {
  if (outputPin_ < 0 || windowMs == 0)
    return 0;
  // Sample the same pad with both paths over one window: PCNT in hardware and
  // a direct GPIO read loop in software. Return the plausible value so a
  // marginal PCNT route cannot fail an otherwise live carrier.
  const uint64_t before = edges();
  const uint32_t polled = pollEdges(windowMs * 1000U);
  const uint64_t delta = edges() - before;
  const uint32_t nominal = (Config::clockHz / 1000U) * windowMs;
  const uint32_t deltaError =
      static_cast<uint32_t>(delta) > nominal
          ? static_cast<uint32_t>(delta) - nominal
          : nominal - static_cast<uint32_t>(delta);
  const uint32_t pollError = polled > nominal ? polled - nominal : nominal - polled;
  // Return whichever path is closer to the expected edge count. PCNT can
  // undercount while the CPU busy-polls the same pad, so "first plausible"
  // selection would hide a bad hardware count behind a good GPIO count.
  return deltaError <= pollError ? static_cast<uint32_t>(delta) : polled;
}

bool SampleClock::verifyOutput(uint32_t windowMs, uint32_t minEdges,
                               uint32_t maxEdges) {
  if (outputPin_ < 0 || windowMs == 0)
    return false;
  const uint64_t before = edges();
  delay(windowMs);
  const uint64_t delta = edges() - before;
  if (delta >= minEdges && delta <= maxEdges) {
    return true;
  }
  // PCNT can miss the carrier on some resets even though GPIO36 is still
  // toggling. Poll the pad directly as an independent presence check.
  const uint32_t polled = pollEdges(windowMs * 1000U);
  return polled >= minEdges && polled <= maxEdges;
}

bool SampleClock::recover() {
  if (outputPin_ < 0)
    return false;
  // Rebuild the whole carrier and restart the count snapshot. The caller runs
  // one verification window afterwards.
  return restartCarrier();
}

bool SampleClock::setOutputEnabled(bool enabled) {
  if (outputPin_ < 0)
    return false;
  if (enabled) {
    if (!startCarrier(outputPin_))
      return false;
    if (pcnt_counter_resume(PCNT_UNIT_0) != ESP_OK)
      return false;
    delay(2);
  } else {
    // Force a defined low level before stopping so the sensor CLKIN input is
    // not left floating while the diagnostic uses the internal clock.
    mcpwm_set_signal_low(kUnit, kTimer, kGenerator);
    mcpwm_stop(kUnit, kTimer);
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
  if (!lastLocal_ || !consecutive || local - lastLocal_ < 900000 ||
      local - lastLocal_ > 1100000) {
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
