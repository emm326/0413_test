#include "SampleClock.h"

#include <driver/gpio.h>
#include <driver/ledc.h>
#include <driver/pcnt.h>
#include <soc/pcnt_struct.h>

#include "Config.h"
namespace {
portMUX_TYPE counterMux = portMUX_INITIALIZER_UNLOCKED;
portMUX_TYPE ppsMux = portMUX_INITIALIZER_UNLOCKED;
volatile uint64_t counterBase = 0, ppsEdges = 0, ppsLocal = 0;
volatile uint32_t ppsSerial = 0;
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
    counterBase += 30000;
    PCNT.int_clr.val = 1;
  }
  const uint64_t total = counterBase + value;
  portEXIT_CRITICAL_ISR(&counterMux);
  return total;
}
void IRAM_ATTR SampleClock::onOverflow(void*) {
  portENTER_CRITICAL_ISR(&counterMux);
  if (PCNT.int_raw.val & 1) {
    counterBase += 30000;
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
bool SampleClock::begin(int pin) {
  ledc_timer_config_t timer = {};
  timer.speed_mode = LEDC_LOW_SPEED_MODE;
  timer.duty_resolution = LEDC_TIMER_1_BIT;
  timer.timer_num = LEDC_TIMER_0;
  timer.freq_hz = Config::clockHz;
  timer.clk_cfg = LEDC_USE_XTAL_CLK;
  if (ledc_timer_config(&timer) != ESP_OK)
    return false;
  ledc_channel_config_t channel = {};
  channel.gpio_num = pin;
  channel.speed_mode = LEDC_LOW_SPEED_MODE;
  channel.channel = LEDC_CHANNEL_0;
  channel.timer_sel = LEDC_TIMER_0;
  channel.duty = 1;
  if (ledc_channel_config(&channel) != ESP_OK)
    return false;
  pcnt_config_t counter = {};
  counter.pulse_gpio_num = pin;
  counter.ctrl_gpio_num = PCNT_PIN_NOT_USED;
  counter.pos_mode = PCNT_COUNT_INC;
  counter.neg_mode = PCNT_COUNT_DIS;
  counter.lctrl_mode = PCNT_MODE_KEEP;
  counter.hctrl_mode = PCNT_MODE_KEEP;
  counter.counter_h_lim = 30000;
  counter.counter_l_lim = -1;
  counter.unit = PCNT_UNIT_0;
  counter.channel = PCNT_CHANNEL_0;
  if (pcnt_unit_config(&counter) != ESP_OK)
    return false;
  gpio_set_direction(static_cast<gpio_num_t>(pin), GPIO_MODE_INPUT_OUTPUT);
  if (pcnt_counter_pause(PCNT_UNIT_0) != ESP_OK || pcnt_counter_clear(PCNT_UNIT_0) != ESP_OK)
    return false;
  if (pcnt_event_enable(PCNT_UNIT_0, PCNT_EVT_H_LIM) != ESP_OK)
    return false;
  if (pcnt_isr_register(onOverflow, nullptr, ESP_INTR_FLAG_IRAM, nullptr) != ESP_OK)
    return false;
  return pcnt_intr_enable(PCNT_UNIT_0) == ESP_OK && pcnt_counter_resume(PCNT_UNIT_0) == ESP_OK;
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
