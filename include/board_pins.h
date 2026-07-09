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

// IIM-42352 + LC76G + DS3231 多节点 GPS/PPS 同步方案
// IIM-42352 使用 Header J2 上可用杜邦线连接的独立 SPI 引脚。
// GPIO9/10/11 是板载 LoRa 的内部 SPI 连线，不适合外接 IIM-42352。
#ifndef PIN_IIM42352_SCK
#define PIN_IIM42352_SCK 47
#endif

#ifndef PIN_IIM42352_MOSI
#define PIN_IIM42352_MOSI 48
#endif

#ifndef PIN_IIM42352_MISO
#define PIN_IIM42352_MISO 33
#endif

#ifndef PIN_IIM42352_CS
#define PIN_IIM42352_CS 34
#endif

#ifndef PIN_IIM42352_INT1
#define PIN_IIM42352_INT1 26
#endif

#ifndef PIN_IIM42352_INT2
#define PIN_IIM42352_INT2 -1
#endif

#ifndef PIN_IIM42352_CLKOUT
#define PIN_IIM42352_CLKOUT 3
#endif

#ifndef PIN_GPS_RX
#define PIN_GPS_RX 5
#endif

#ifndef PIN_GPS_TX
#define PIN_GPS_TX 6
#endif

#ifndef PIN_GPS_PPS
#define PIN_GPS_PPS 4
#endif

#ifndef PIN_DS3231_SDA
#define PIN_DS3231_SDA 41
#endif

#ifndef PIN_DS3231_SCL
#define PIN_DS3231_SCL 42
#endif

#ifndef DS3231_I2C_ADDRESS
#define DS3231_I2C_ADDRESS 0x68
#endif
