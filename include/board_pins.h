#pragma once

#include <Arduino.h>

// Heltec WiFi LoRa 32 V3 / HTIT-WB32LA_V3 板载 SX1262 默认引脚
#ifndef PIN_LORA_NSS
#define PIN_LORA_NSS SS
#endif

#ifndef PIN_LORA_SCK
#define PIN_LORA_SCK SCK
#endif

#ifndef PIN_LORA_MISO
#define PIN_LORA_MISO MISO
#endif

#ifndef PIN_LORA_MOSI
#define PIN_LORA_MOSI MOSI
#endif

#ifndef PIN_LORA_RST
#define PIN_LORA_RST RST_LoRa
#endif

#ifndef PIN_LORA_BUSY
#define PIN_LORA_BUSY BUSY_LoRa
#endif

#ifndef PIN_LORA_DIO1
#define PIN_LORA_DIO1 DIO0
#endif

// 板载 OLED / 用户按键
#ifndef PIN_OLED_SDA
#define PIN_OLED_SDA SDA_OLED
#endif

#ifndef PIN_OLED_SCL
#define PIN_OLED_SCL SCL_OLED
#endif

#ifndef PIN_OLED_RST
#define PIN_OLED_RST RST_OLED
#endif

#ifndef PIN_VEXT_CTRL
#define PIN_VEXT_CTRL Vext
#endif

#ifndef PIN_USER_BUTTON
#define PIN_USER_BUTTON 0
#endif

// ADXL345 当前改为 I2C 接线
// Heltec -> ADXL345:
// GPIO42 -> SCL
// GPIO41 -> SDA
// GPIO3  -> INT1(可选)
//
// 默认 I2C 地址设为 0x53，对应 SDO/ALT ADDRESS 为低电平。
// 如果你的模块把 SDO 拉高，需要改成 0x1D。
#ifndef PIN_ADXL345_SCL
#define PIN_ADXL345_SCL 42
#endif

#ifndef PIN_ADXL345_SDA
#define PIN_ADXL345_SDA 41
#endif

#ifndef PIN_ADXL345_INT1
#define PIN_ADXL345_INT1 3
#endif

#ifndef PIN_ADXL345_INT2
#define PIN_ADXL345_INT2 -1
#endif

#ifndef ADXL345_I2C_ADDRESS
#define ADXL345_I2C_ADDRESS 0x53
#endif
