# HTIT-WB32LA 采集系统功耗估算

## 1. 结论先行

本文按当前工程代码和官方器件资料估算，不是电流表实测值。计算条件为：

- HTIT-WB32LA / Heltec WiFi LoRa 32 V3；
- ESP32-S3 的 Wi-Fi、Bluetooth 未启用，CPU 持续运行；
- IIM 实际采样率约 `51.2 Hz`，每包 `24` 个三轴样本；
- GPS 连续跟踪并每秒输出 PPS；
- OLED 常亮；
- LoRa 为 `470 MHz / BW 250 kHz / SF7 / CR 4/5 / 14 dBm / 8-symbol preamble`；
- 所有外接模块由板上 `3.3 V` 供电。

在这些条件下，一个 TX 节点的合理工程估算是：

| 模式 | 估算功率 | 说明 |
|---|---:|---|
| USB 普通采样，不开 GPS | 约 `0.17-0.22 W @ USB 5 V` | LoRa 不初始化，LC76G 不初始化 |
| USB GPS/PPS 同步采样 | 约 `0.31-0.36 W @ USB 5 V` | 包含 CP2102；OLED 内容不同会使结果约再变化 `±0.05 W` |
| LoRa TX 节点，正在监听 | 约 `0.185 W @ 3.3 V rail` | SX1262 处于连续 RX |
| LoRa TX 节点，14 dBm 发射瞬间 | 约 `0.254 W @ 3.3 V rail` | 不是整机长期平均值 |
| LoRa TX 节点，51.2 Hz 长期平均 | 约 `0.205-0.210 W @ 3.3 V rail` | 下限按只发有效数据；上限考虑当前空轮询/NO_DATA |
| 单个 LoRa TX + 2800 mAh 电池 | 典型约 `44.5 h`，工程建议按 `34-40 h` | 理想上限 49.4 h；按 3.7 V 锂电池、电源效率及容量折减计算 |
| LoRa RX 节点，长期平均 | 约 `0.110 W @ 3.3 V rail` | 不含电脑 USB-UART 的 5 V 功耗 |
| LoRa RX 节点连接电脑采集 | 约 `0.22-0.27 W @ USB 5 V` | 包含 CP2102，OLED 常亮 |

上述值适合做电源和电池容量的初步设计。建议给瞬态、电源转换损耗、GPS 天线和模块板载器件留余量：

- 单个 TX 节点电源按至少 `5 V / 150 mA`，即 `0.75 W` 设计；
- 如果使用电池，按实测平均电流再加 `30%` 余量；
- 最终论文或硬件验收应在 USB 5 V 输入端或电池输入端串接功率计实测。

## 2. 型号核对

工程文件和接线说明使用名称 `IIM-42352`，但驱动中的 `WHO_AM_I_EXPECTED=0x6D`、`PWR_MGMT0`、`ACCEL_CONFIG0`、`INTF_CONFIG5` 等寄存器与 TDK 官方 **IIM-42652** 数据手册 DS-000440 对应。官方当前也未列出 IIM-42352 产品页。

因此本文暂按 **IIM-42652** 计算。请检查传感器 IC 或模块丝印；若实物并非 IIM-42652，传感器功耗项必须替换。

## 3. 官方数据与采用值

功率统一用：

```text
P = V * I
E = P * t
P_average = sum(P_state * t_state) / T
```

| 模块 | 当前工作状态 | 本文采用电流 | 3.3 V 功率 | 依据与限制 |
|---|---|---:|---:|---|
| ESP32-S3 | Wi-Fi/BLE 关闭，240 MHz 持续运行 | `13.2 mA` | `43.56 mW` | Espressif ESP32-S3 数据手册的 modem-sleep、CPU 240 MHz 典型值。Arduino 循环、SPI、字符串格式化和 USB/UART 活动会使板上实测高于此值 |
| SX1262 RX | 连续接收 | `4.2 mA` | `13.86 mW` | Semtech SX1261/2 官方页和数据手册的 active receive 典型值 |
| SX1262 TX | `+14 dBm` | `24.9 mA` | `82.17 mW` | Semtech SX1261/2 数据手册中 DC-DC 配置、+14 dBm 附近典型值；具体 Heltec 射频匹配会有小幅差异 |
| IIM-42652 | 加速度计 low-noise，陀螺仪关闭 | `0.57 mA` | `1.88 mW` | 代码写 `PWR_MGMT0=0x03`，即 accel LN、gyro off；采用 TDK DS-000440 的典型值 |
| LC76G | 连续定位/跟踪 | `28 mA` | `92.4 mW` | 采用 LC76G 系列典型连续跟踪量级；LC76G 有不同后缀，冷启动、双频配置、有源天线和模块载板会改变电流，实物后缀未确认前应视为估算 |
| DS3231 | VCC 工作、32 kHz 输出开启 | `0.20 mA` | `0.66 mW` | Analog Devices/Maxim DS3231 active supply current 典型值；32K 上拉的动态电流很小 |
| 0.96 inch OLED | 常亮文本界面 | `10 mA` 估算 | `33 mW` | SSD1306 控制器本身不是主要负载，OLED 面板电流随点亮像素和亮度变化；工程计算范围建议用 `5-20 mA` |
| CP2102 | USB-UART 持续传输 | `10-20 mA @ 5 V` | `50-100 mW @ 5 V` | Heltec 官方说明 V3 集成 CP2102；其电流取决于具体 CP2102 版本、串口活动和外围电路 |

注意：器件数据手册的典型电流不能直接等同于开发板 USB 口实测电流。稳压器静态电流、充电管理、LED、上拉电阻、模块载板和 OLED 图案均会产生差异。

## 4. USB 模式

### 4.1 代码实际启用的模块

`APP_TX_USB_STREAM_TEST` 下代码不执行 `initLoRa()`，所以 SX1262 不进入连续 RX，LoRa 功耗可近似忽略。

- 普通 USB 测试：ESP32-S3 + IIM-42652 + DS3231 + OLED + CP2102；GPS 不初始化。
- USB GPS 同步测试：在上述基础上增加 LC76G UART/PPS 连续工作。

### 4.2 USB GPS 同步模式逐模块

先计算 3.3 V 负载：

```text
ESP32-S3       13.20 mA
IIM-42652       0.57 mA
LC76G          28.00 mA
DS3231          0.20 mA
OLED           10.00 mA  (估算)
--------------------------------
3.3 V rail     51.97 mA
P_3V3 = 3.3 * 51.97 mA = 171.50 mW
```

HTIT-WB32LA V3 由 USB 5 V 输入并带 CP2102。若以线性稳压器近似，输入电流量级接近 3.3 V 负载电流，再加 CP2102：

```text
I_USB ~= 51.97 mA + (10 to 20 mA)
      ~= 61.97 to 71.97 mA

P_USB ~= 5 V * I_USB
      ~= 0.310 to 0.360 W
```

若 OLED 接近全亮，或 ESP32 因大量 `Serial.printf()` 比数据手册基准多消耗约 10-20 mA，USB 口实测可能接近 `0.40-0.50 W`。因此报告中应把 `0.31-0.36 W` 写成“基于典型值的下偏估算”，而不是保证值。

### 4.3 普通 USB 测试

普通 USB 测试不初始化 GPS，减去 LC76G 的约 `28 mA`：

```text
I_USB ~= 23.97 mA + (10 to 20 mA CP2102)
      ~= 33.97 to 43.97 mA

P_USB ~= 0.170 to 0.220 W
```

### 4.4 每个采样周期的能量

`50 Hz` 配置在 DS3231 32.768 kHz 外部时钟下实际约 `51.2 Hz`：

```text
T_sample = 1 / 51.2 = 19.53125 ms
```

USB GPS 同步模式按 USB 输入功率中值 `0.335 W`：

```text
E_sample_USB = 0.335 W * 19.53125 ms
             = 6.54 mJ / sample period
```

这是整个节点在一个采样周期内消耗的能量，不是 IIM 单独采一次样本所需能量。

## 5. LoRa 空中时间

### 5.1 数据包大小

工程协议：

```text
VibrationFrameHeader = 16 bytes
24 samples * 3 axes * int16 = 144 bytes
CRC16 = 2 bytes
LoRa application payload = 162 bytes
```

控制帧为：

```text
ControlFrame = 24 bytes
CRC16 = 2 bytes
LoRa application payload = 26 bytes
```

### 5.2 LoRa airtime 公式

当前 `SF=7`、`BW=250 kHz`：

```text
T_symbol = 2^SF / BW
         = 128 / 250000
         = 0.512 ms
```

采用显式头、CRC 开、低数据率优化关、CR=4/5。按 Semtech LoRa airtime 公式：

| 帧 | Payload | Payload symbols | 总 airtime |
|---|---:|---:|---:|
| 24 样本数据帧 | 162 B | 243 | `130.688 ms` |
| POLL / NO_DATA / START_AT | 26 B | 48 | `30.848 ms` |

每个 TX 每秒产生：

```text
R_packet = 51.2 / 24 = 2.1333 packet/s
```

只计算有效数据时，每个 TX 的 SX1262 发射占空比为：

```text
D_TX_data = 2.1333 * 130.688 ms
          = 278.80 ms/s
          = 27.88%
```

### 5.3 六包 burst

当前 `burst=6`，每包后代码 `delay(2 ms)`：

```text
T_burst = 6 * 130.688 ms + 6 * 2 ms
        = 796.128 ms
```

其中 SX1262 真正处于 TX 的时间为 `784.128 ms`。一次满 6 包 burst 的射频芯片 TX 能量约为：

```text
E_radio_burst = 3.3 V * 24.9 mA * 0.784128 s
              = 64.44 mJ
```

但 51.2 Hz、24 样本/包时，产生 6 包需要：

```text
6 * 24 / 51.2 = 2.8125 s
```

所以只有队列已有积压时才会出现连续满 burst；稳态轮询通常不会每次都有 6 包。

## 6. LoRa TX 节点功率

TX 节点除 SX1262 外的常开负载：

```text
I_base = ESP32 + IIM + GPS + DS3231 + OLED
       = 13.2 + 0.57 + 28 + 0.20 + 10
       = 51.97 mA
```

### 6.1 各无线期间

| 期间 | 电流 | 功率 @ 3.3 V |
|---|---:|---:|
| 等待 POLL、SX1262 RX | `51.97 + 4.2 = 56.17 mA` | `185.36 mW` |
| 发送 DATA/NO_DATA、14 dBm | `51.97 + 24.9 = 76.87 mA` | `253.67 mW` |
| LoRa 芯片待机 | 约 `51.97 mA + SX1262 standby` | 约 `171.5 mW` |

### 6.2 只按有效数据的平均值

```text
I_radio_avg = 24.9 mA * 0.2788 + 4.2 mA * (1 - 0.2788)
            = 9.97 mA

I_node_avg  = 51.97 + 9.97
            = 61.94 mA

P_node_avg  = 3.3 V * 61.94 mA
            = 204.4 mW
```

每个 `19.53125 ms` 采样周期：

```text
E_sample_LoRa ~= 0.2044 W * 19.53125 ms
              ~= 3.99 mJ
```

### 6.3 当前连续轮询的影响

RX 代码在收到 DATA 或 NO_DATA 后立即轮询下一个节点，没有固定轮询周期。队列为空时，TX 仍会返回一个 26-byte `NO_DATA`。因此实际射频功耗会高于“只发有效数据”的下限。

忽略 MCU 处理和射频切换时间，用两节点信道 airtime 做稳态近似：

- 全网有效数据 `4.2667 packet/s`；
- 每次查询有一个 `30.848 ms` POLL；
- 无数据查询再产生一个 `30.848 ms` NO_DATA；
- 估算全网约 `9.3 poll/s`，每个 TX 约 `4.65 poll/s`；
- 每个 TX 约 `2.13 DATA/s + 2.52 NO_DATA/s`；
- 每个 TX 的射频 TX 占空比约 `35.6%`。

于是：

```text
I_radio_avg ~= 24.9 mA * 0.356 + 4.2 mA * 0.644
            ~= 11.57 mA

P_TX_node_current_polling
  ~= 3.3 V * (51.97 + 11.57) mA
  ~= 209.7 mW
```

这只是 airtime 模型。串口日志、5 ms 循环延时、RadioLib 切换和丢包会改变实际比例。要降低功耗和信道占用，应给轮询增加与数据产生率匹配的节拍，或让 `NO_DATA` 后对该节点退避一段时间。

## 7. 单个无线 TX 节点总功率与 2800 mAh 电池续航

### 7.1 单个 TX 节点包含的负载

无线传输模式下，一个完整 TX 节点包含：

- HTIT-WB32LA 上的 ESP32-S3；
- 板载 SX1262 LoRa 芯片；
- 板载 0.96-inch OLED；
- IIM-42652 加速度传感器；
- LC76G GPS/PPS；
- DS3231 及其 32.768 kHz 输出；
- 板上稳压器、充电管理和指示器件。

CP2102 主要由 USB 5 V 一侧供电。节点脱离 USB、只使用锂电池无线工作时，不把持续工作的 USB-UART 功耗加入主要负载，但板级静态损耗仍需通过效率和余量体现。

### 7.2 按当前连续轮询计算总功率

根据第 6.3 节，当前两节点连续轮询时，每个 TX 的 SX1262 估算约 `35.6%` 时间发射、`64.4%` 时间接收。

常开模块电流：

```text
I_base = I_ESP32 + I_IIM + I_GPS + I_DS3231 + I_OLED
       = 13.20 + 0.57 + 28.00 + 0.20 + 10.00
       = 51.97 mA
```

LoRa 芯片平均电流：

```text
I_SX1262_avg = I_TX * D_TX + I_RX * D_RX
             = 24.9 mA * 0.356 + 4.2 mA * 0.644
             = 11.57 mA
```

因此单个无线 TX 节点的 3.3 V 负载总电流和总功率为：

```text
I_TX_node_avg = I_base + I_SX1262_avg
              = 51.97 + 11.57
              = 63.54 mA

P_TX_node_avg = 3.3 V * 63.54 mA
              = 209.68 mW
              ~= 0.210 W
```

所以本文采用的单个 TX 无线模式长期平均总功率为：

```text
约 0.21 W @ 3.3 V rail
```

不同无线期间的瞬时值为：

| 工作期间 | TX 节点总电流 | TX 节点总功率 |
|---|---:|---:|
| 等待 POLL、LoRa 接收 | `56.17 mA` | `185.36 mW` |
| LoRa 14 dBm 发射 | `76.87 mA` | `253.67 mW` |
| 当前轮询方式长期平均 | `63.54 mA` | `209.68 mW` |

这里的 `0.21 W` 是 3.3 V 负载侧功率。电池侧还要承担稳压器、保护电路和板载电源管理损耗，因此电池实际输出功率会略高。

### 7.3 2800 mAh 电池的能量

假设使用常见单节锂聚合物电池：

```text
标称电压 V_bat = 3.7 V
标称容量 C_bat = 2800 mAh = 2.8 Ah
```

电池标称能量不能用 `3.3 V * 2.8 Ah` 计算，因为 `2800 mAh` 是电池侧容量，应使用电池标称电压：

```text
E_bat_nominal = V_bat * C_bat
              = 3.7 V * 2.8 Ah
              = 10.36 Wh
```

这表示理想情况下电池储存约 `10.36 Wh` 能量。单节锂电池实际电压会从满电约 `4.2 V` 逐渐下降到保护截止电压附近，使用 `3.7 V` 是常用的标称能量计算方法。

### 7.4 不考虑转换损耗的理论上限

如果假设电池能量可以 100% 转换到 3.3 V 负载：

```text
t_ideal = E_bat_nominal / P_TX_node_avg
        = 10.36 Wh / 0.20968 W
        = 49.41 h
        = 2.06 days
```

因此 `49.4 h` 只是完全忽略稳压损耗、容量衰减和截止电压的数学上限，不应作为实际续航承诺。

### 7.5 考虑 90% 电源效率的典型续航

将电池到 3.3 V 负载的综合效率暂按 `90%` 估算：

```text
eta_power = 0.90

E_load_available = E_bat_nominal * eta_power
                 = 10.36 Wh * 0.90
                 = 9.324 Wh

t_typical = E_load_available / P_TX_node_avg
          = 9.324 Wh / 0.20968 W
          = 44.47 h
          = 1.85 days
```

也可以先换算成电池平均电流：

```text
I_bat_avg = P_TX_node_avg / (V_bat * eta_power)
          = 0.20968 W / (3.7 V * 0.90)
          = 62.97 mA

t_typical = 2800 mAh / 62.97 mA
          = 44.47 h
```

两种算法结果一致。因此，在电池容量真实达到 2800 mAh、环境温度正常且电源综合效率约 90% 时，单个无线 TX 节点理论典型续航约为：

```text
44.5 小时，约 1 天 20 小时
```

### 7.6 工程保守续航

实际可用容量通常会受电池品质、老化、低温、大电流压降和保护板截止电压影响。若只按标称容量的 `80%` 可用，同时电源效率取 `85-90%`：

```text
t_conservative_low
  = 10.36 Wh * 0.80 * 0.85 / 0.20968 W
  = 33.59 h

t_conservative_high
  = 10.36 Wh * 0.80 * 0.90 / 0.20968 W
  = 35.57 h
```

如果 ESP32、OLED、GPS 模块载板和板上 LED 的实际功耗使整机负载达到 `0.25 W`，按 90% 效率：

```text
t_0.25W = 10.36 Wh * 0.90 / 0.25 W
        = 37.30 h
```

因此建议采用以下口径：

- 数学理想上限：约 `49.4 h`；
- 典型估算：约 `44.5 h`；
- 工程设计值：按 `34-40 h`，即约 `1.4-1.7 天`；
- 如果要求保证连续运行 24 小时，2800 mAh 在当前估算下有余量；
- 如果要求保证连续运行 48 小时，2800 mAh 不足，建议至少 `4000-5000 mAh`，并通过实测确认。

### 7.7 依据与适用条件

续航计算依据为：

1. 前文各芯片官方数据手册的典型工作电流；
2. 当前代码中的 `14 dBm / SF7 / BW 250 kHz / 24 samples/packet`；
3. 当前两节点轮询模型估算的 `35.6%` SX1262 TX 占空比；
4. Heltec 官方说明 WiFi LoRa 32 V3 带单节 Li-Po 电池管理；
5. 电池能量公式 `Wh = nominal voltage * Ah`；
6. 90% 转换效率和 80% 可用容量是工程假设，不是 Heltec 或电池厂商的保证参数。

如果电池标签只写 `2800 mAh` 而没有制造商、型号、额定能量和放电曲线，应优先以 USB 功率计或电池侧电流记录仪实测。最终续航应使用：

```text
t_actual = measured_usable_battery_Wh / measured_average_input_W
```

## 8. LoRa RX 节点功率

RX 节点没有 IIM、LC76G 和 DS3231，主要负载是 ESP32-S3、OLED 和 SX1262：

| RX 期间 | 电流 | 功率 @ 3.3 V |
|---|---:|---:|
| SX1262 接收响应 | `13.2 + 10 + 4.2 = 27.4 mA` | `90.42 mW` |
| SX1262 发送 POLL | `13.2 + 10 + 24.9 = 48.1 mA` | `158.73 mW` |

按上一节当前连续轮询的 airtime 近似，RX 约 `28.7%` 时间发送 POLL，约 `71.3%` 时间接收响应：

```text
I_RX_node_avg = 48.1 mA * 0.287 + 27.4 mA * 0.713
              = 33.34 mA

P_RX_node_avg = 3.3 V * 33.34 mA
              = 110.0 mW
```

实际 RX 通过 USB 向电脑输出 CSV 数据，因此还要计入 CP2102 和 5 V 到 3.3 V 的压差损耗：

```text
P_RX_USB ~= 5 V * (33.34 + 10 to 20) mA
         ~= 0.217 to 0.267 W
```

## 9. 误差来源和实测方法

以下项目无法仅靠官方数据手册精确得到：

1. LC76G 的准确后缀、当前星座/频点、天线是无源还是有源；
2. OLED 实际点亮像素和 contrast；
3. ESP32-S3 实际 CPU 频率与 Arduino/串口负载；
4. HTIT-WB32LA V3 的稳压器、充电管理、LED 和 CP2102 实际电流；
5. LoRa 丢包、空轮询次数和每次 burst 的真实包数。

推荐测量：

- USB 模式：在 USB 5 V 线上使用 USB 功率计，分别记录等待、采样、持续输出三个阶段；
- 电池模式：在电池正极串接 Joulescope、Nordic PPK2 或带记录功能的电流表；
- 同时在固件日志中统计 `DATA`、`NO_DATA`、`POLL` 数量，用实际计数代替第 6.3 节的估算占空比；
- 至少采集 60 秒平均值，并记录 TX 发射尖峰最大电流。

## 10. 官方资料

1. Heltec Automation, WiFi LoRa 32 官方说明（V3 为 ESP32-S3N8 + SX1262，集成 0.96-inch OLED、Li-Po 管理和 CP2102）：https://docs.heltec.org/en/node/esp32/wifi_lora_32/index.html
2. Heltec WiFi LoRa 32 V3 官方资源目录：https://resource.heltec.cn/download/WiFi_LoRa_32_V3
3. Espressif, ESP32-S3 Series Datasheet：https://www.espressif.com/sites/default/files/documentation/esp32-s3_datasheet_en.pdf
4. Semtech, SX1262 官方产品页及 SX1261/SX1262 Datasheet：https://www.semtech.com/products/wireless-rf/lora-connect/sx1262
5. TDK InvenSense, DS-000440 IIM-42652 Datasheet：https://d17t6iyxenbwp1.cloudfront.net/s3fs-public/2026-03/ds-000440-iim-42652-datasheet.pdf
6. Quectel, LC76G series 官方产品页：https://www.quectel.com/product/gnss-lc76g-series/
7. Analog Devices/Maxim, DS3231 Datasheet：https://www.analog.com/media/en/technical-documentation/data-sheets/DS3231.pdf
8. Silicon Labs, CP2102/CP2109 Datasheet：https://www.silabs.com/documents/public/data-sheets/CP2102-9.pdf
