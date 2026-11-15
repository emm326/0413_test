#pragma once
using gpio_num_t=int;
enum gpio_pull_mode_t { GPIO_PULLUP_ONLY=0,GPIO_PULLDOWN_ONLY=1,GPIO_FLOATING=3 };
int gpio_set_pull_mode(gpio_num_t pin,gpio_pull_mode_t mode);
