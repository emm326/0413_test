# GPS Disciplined CLKIN 硬件接线与同步方案

## 概述

用 ESP32 LEDC 外设在 GPIO3 产生 32768Hz 方波替代 DS3231 32K 输出，驱动 IIM-42352 的 CLKIN 引脚。通过数字锁相环（DPLL）将此 CLKOUT 频率锁定到 GPS PPS，使所有节点的 IIM-42352 采样网格都锁相到同一个 GPS 秒边界。

预期同步误差：**50-200 µs**（对比当前方案 1.5-2.2 ms，提升 10-40 倍）。

## 硬件改动（仅一根线）

### 当前接线 (DS3231 模式)
```
DS3231 32K 引脚 ──→ IIM-42352 Pin 9 (CLKIN)
```

### 改动后 (GPS 驯服模式)
```
ESP32 GPIO3 ──→ IIM-42352 Pin 9 (CLKIN)
DS3231 32K 引脚  悬空（不再接任何东西）
```

### 完整接线对照

| 信号 | ESP32-S3 GPIO | 目标 |
|------|--------------|------|
| GPS PPS | GPIO4 | LC76G PPS 输出 |
| GPS RX | GPIO5 | LC76G TX |
| GPS TX | GPIO6 | LC76G RX |
| **CLKOUT** | **GPIO3** | **IIM-42352 Pin 9 (CLKIN)** ← 改动 |
| IIM SCK | GPIO47 | IIM-42352 SCK |
| IIM MOSI | GPIO48 | IIM-42352 MOSI |
| IIM MISO | GPIO33 | IIM-42352 MISO |
| IIM CS | GPIO34 | IIM-42352 CS |
| IIM INT1 | GPIO26 | IIM-42352 INT1 |
| DS3231 SDA | GPIO41 | DS3231 SDA |
| DS3231 SCL | GPIO42 | DS3231 SCL |
| 用户按键 | GPIO0 | 板载按键 |

DS3231 保留在 I2C 总线上不动，仅 32K 输出引脚断开。

## 物理操作

1. **拔掉**：DS3231 模块的 32K 引脚 → IIM-42352 breakout 板上标记为 CLKIN/INT2（Pin 9）的杜邦线
2. **接上**：一根杜邦线从 Heltec J2 排针的 GPIO3 → IIM-42352 上刚才空出来的 CLKIN/INT2 引脚

## 电压兼容性

| 信号 | 电压 | 说明 |
|------|------|------|
| ESP32 GPIO3 (LEDC CLKOUT) | 3.3V CMOS | ESP32-S3 推挽输出 |
| IIM-42352 VDDIO | 3.3V | Breakout 板供电 |
| DS3231 32K (原方案) | 3.3V | 推挽输出 |
| IIM-42352 CLKIN 输入阻抗 | >1MΩ / ~pF | CMOS 输入 |

三个电压完全相同，均为 3.3V 电平，可直接连接无电平转换。

## 固件编译

### platformio.ini 已有环境

| 环境名 | 说明 |
|--------|------|
| `heltec_wifi_lora_32_V3_iim_gps_clk_sync_tx_1001` | 节点 1001，GPS 驯服 CLKIN + DPLL |
| `heltec_wifi_lora_32_V3_iim_gps_clk_sync_tx_1002` | 节点 1002，GPS 驯服 CLKIN + DPLL |

编译：
```bash
pio run -e heltec_wifi_lora_32_V3_iim_gps_clk_sync_tx_1001
pio run -e heltec_wifi_lora_32_V3_iim_gps_clk_sync_tx_1002
```

### 启用的宏

| 宏 | 作用 |
|----|------|
| `APP_IIM42352_GPS_DISCIPLINED_CLKIN=1` | ESP32 LEDC GPIO3 产生 32768Hz CLKOUT 替代 DS3231 |
| `APP_IIM42352_CLKOUT_DPLL=1` | DPLL 锁相环，PPS 边沿自动校正 CLKOUT 频率 |
| `APP_REQUIRE_GPS_TIMEBASE` | TX 仅在 GPS PPS 健康时采集，启动对齐 PPS 边沿 |
| `APP_USE_DS3231_32K_TIMESTAMPS` | 保持 32.768kHz 采样率转换（频率相同，仍适用） |

### 新增自定义环境

如需其他节点 ID，在 platformio.ini 添加：
```ini
[env:heltec_wifi_lora_32_V3_iim_gps_clk_sync_tx_1003]
platform = espressif32
board = heltec_wifi_lora_32_V3
framework = arduino
monitor_speed = ${common.monitor_speed}
lib_deps =
  ${common.lib_deps}
build_flags =
  ${common.iim_gps_clk_sync_tx_build_flags}
  -D APP_DEVICE_ID_VALUE=1003
```

## 同步启动流程

1. **GPS 锁定**：等待 GPS PPS 稳定（healthy=1）
2. **CLKOUT 输出**：ESP32 GPIO3 输出 32768Hz 方波，DPLL 锁定到 PPS
3. **发送 START_AT**：PC/RX 发送 START_AT 指令指定未来 epoch 时刻
4. **PPS 门控**：到达目标秒后，第一个 PPS 边沿触发启动
5. **相位归零**：`clkSync.restart()` 将 CLKOUT 相位重置到 PPS 边沿
6. **IIM 重启**：`adxl.restartAccel()` 重新上电 IIM-42352
7. **预热丢弃**：丢弃前 1000ms（`IIM42352_SYNC_WARMUP_MS`）样本

### 时序图

```
PPS:        │                 │                 │
            │  武装窗口       │  同步启动        │
            │  (START_AT)     │                  │
            │                 ▼                  │
CLKOUT:     ──────────────────|███████████████████ (相位归零)
                               │
IIM-42352:  ──────────────────|─20ms上电─|══ 采样 ══ (预热期丢弃)
                               │
节点1001:                     ▼ 采样网格相位 = 0
节点1002:                     ▼ 采样网格相位 = 0  ← 对齐
```

## 串口诊断输出

启用后会新增 CLKSYNC 行：

```
CLKSYNC,dev,pps_epoch_us,clk_hz,pps_interval_us,phase_error_us,first_int1_offset_us,healthy,dpll_ppm,expected_cycles,stable
```

重点指标：
- `clk_hz`：当前 CLKOUT 频率（DPLL 校正后，接近 32768）
- `phase_error_us`：CLKOUT 相对于 PPS 的相位误差（µs）
- `dpll_ppm`：DPLL 累积频率校正量（ppm）
- `stable`：相位误差 <10µs 且连续 5+ PPS 周期时置 1
- `first_int1_offset_us`：启动后首个 INT1 距 PPS 的偏移（µs）

## DPLL 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `APP_IIM42352_CLKOUT_DPLL_MAX_PPM` | 200 | DPLL 频率校正范围 ±200ppm |
| `APP_IIM42352_CLKOUT_DPLL_PHASE_GAIN_PPM_PER_US` | 2 | P 控制器增益 |
| `APP_IIM42352_CLKOUT_DPLL_STABLE_PHASE_US` | 10 | 稳定判据：相位误差 < 10µs |
| `APP_IIM42352_CLKOUT_DPLL_STABLE_COUNT` | 5 | 稳定判据：连续 5 个 PPS 周期 |

可在 platformio.ini 中覆盖（如 `-D APP_IIM42352_CLKOUT_DPLL_MAX_PPM_VALUE=300L`）。

## 预期同步精度

| 误差源 | 量级 |
|--------|------|
| PPS ISR 延迟抖动 | 1-5 µs |
| CLKOUT DPLL 稳态相位误差 | <10 µs |
| IIM-42352 上电时序差异 | 100-500 µs（预热期丢弃） |
| ESP32 晶振 PPS 间漂移（10ppm × 1s） | <10 µs |
| IIM-42352 CLKIN 分频确定性 | 无累积误差 |
| **预期总同步误差** | **~50-200 µs** |

## 验证方法

### 示波器（黄金标准）

```
CH1: 节点1001 GPS PPS (GPIO4)
CH2: 节点1002 GPS PPS (GPIO4)
CH3: 节点1001 IIM INT1 (GPIO26)
CH4: 节点1002 IIM INT1 (GPIO26)
触发 CH1，观察 CH3/CH4 首个 INT1 脉冲时间差
```

### USB 双串口采集

```powershell
# 两个串口同时采集
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\serial_rx_to_file.py `
  --ports COM6,COM7 --output data_sync_test --rotate-minutes 30 `
  --sample-rate-hz 50 --start-delay-seconds 10 --with-header
```

### 时间误差分析

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\sensor_time_error_analysis.py `
  --sensor-a data_sync_test\sensor1001\<date>\<file>.csv `
  --sensor-b data_sync_test\sensor1002\<date>\<file>.csv `
  --actual-rate-hz 51.2 --write-aligned
```

预期 MAE 从 ~1.5ms 降至 <0.5ms，中位误差 ~50-200µs。
