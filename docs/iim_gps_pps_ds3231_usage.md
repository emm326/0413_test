# HTIT-WB32LA + IIM-42352 + LC76G + DS3231 多节点同步采集说明

当前工程默认切换到 P2P GPS/PPS 同步方案。LoRaWAN 代码和环境仍保留在工程中，但默认不构建。

## 项目目标

用于多节点振动测试、振型识别和模态分析：

```text
RX：HTIT-WB32LA
TX：HTIT-WB32LA + IIM-42352 + LC76G + DS3231
```

系统流程：

```text
PC 串口发送 START_AT epoch_us sample_rate_hz delay_ms
RX 广播 START_AT
TX 等待 LC76G GPS/PPS + DS3231 32K + IIM CLKIN/INT1 正常
GPS NMEA + PPS 负责把 epoch_us 换算成本机 micros()
DS3231 32.768kHz 输入 IIM-42352 INT2/CLKIN，作为传感器采样时钟
IIM-42352 INT1 数据就绪中断给每个 FIFO 样本提供本机 micros() 时间锚点
RX 按设备编号轮询 TX
TX 被点名才发送队列头部数据
RX 成功解析后 ACK
TX 收到 ACK 后删除已发送数据
RX 用 runStartEpochUs + t0_us + 样本间隔还原 epoch_us
PC 将 RX 输出写入 CSV
```

当前振动数据帧协议版本为 `v2`，每包头部使用微秒偏移 `t0_us`，不是旧版毫秒偏移 `t0_ms`。RX 和所有 TX 必须一起重新烧录，不能混用旧固件。

## 固件环境

默认构建：

```text
heltec_wifi_lora_32_V3_rx
heltec_wifi_lora_32_V3_iim_gps_tx_1001
heltec_wifi_lora_32_V3_iim_gps_tx_1002
```

已预留：

```text
heltec_wifi_lora_32_V3_iim_gps_tx_1001 ... 1010
```

旧 P2P ADXL345 环境和 LoRaWAN 环境仍在 `platformio.ini` 中保留，用于回退测试。

## 接线

### IIM-42352 杜邦线 SPI

```text
IIM-42352 VCC  -> HTIT-WB32LA 3V3，J3 物理脚 2 或 3
IIM-42352 GND  -> HTIT-WB32LA GND，J2 物理脚 1 或 J3 物理脚 1
IIM-42352 SCK  -> HTIT-WB32LA GPIO47，J2 物理脚 13
IIM-42352 MOSI -> HTIT-WB32LA GPIO48，J2 物理脚 14
IIM-42352 MISO -> HTIT-WB32LA GPIO33，J2 物理脚 12
IIM-42352 CSB  -> HTIT-WB32LA GPIO34，J2 物理脚 11
IIM-42352 INT1 -> HTIT-WB32LA GPIO26，J2 物理脚 15，必须接，作为 data-ready 时间戳中断
IIM-42352 INT2 -> DS3231 32K，不接 ESP32，作为 CLKIN
```

IIM-42352 模块底部引脚从左到右是：

```text
VCC  GND  SCK  MOSI  MISO  CSB  INT1  INT2
```

`GPIO9/GPIO10/GPIO11` 是板载 LoRa 芯片的内部 SPI 连线，手册引脚图中没有引到可插杜邦线的排针，不要用于外接 IIM-42352。固件中 LoRa 继续使用板载内部 SPI，IIM-42352 使用上表这组独立 SPI。

### LC76G

```text
LC76G VCC -> HTIT-WB32LA 3V3
LC76G GND -> HTIT-WB32LA GND
LC76G TX  -> HTIT-WB32LA GPIO5，J3 物理脚 16
LC76G RX  -> HTIT-WB32LA GPIO6，J3 物理脚 17
LC76G PPS -> HTIT-WB32LA GPIO4，J3 物理脚 15，只接 ESP32，不直接接 IIM-42352
LC76G SCL/SDA/RST -> 暂不接
```

### DS3231

```text
DS3231 VCC -> HTIT-WB32LA 3V3
DS3231 GND -> HTIT-WB32LA GND
DS3231 SDA -> HTIT-WB32LA GPIO41，J3 物理脚 8
DS3231 SCL -> HTIT-WB32LA GPIO42，J3 物理脚 7
DS3231 32K -> IIM-42352 INT2 / CLKIN，作为 IIM-42352 外部采样时钟
DS3231 SQW -> 暂不接
```

DS3231 和 32K 上拉都必须是 `3.3V`。

## SPI 不通时转 IIC

只有在 SPI 检查供电、GND、CS、SCK、MOSI、MISO、WHO_AM_I 后仍不通时，再切 IIC 四线备用：

```text
IIM-42352 VCC  -> HTIT-WB32LA 3V3
IIM-42352 GND  -> HTIT-WB32LA GND
IIM-42352 SCK  -> HTIT-WB32LA GPIO42 / SCL
IIM-42352 MOSI -> HTIT-WB32LA GPIO41 / SDA
```

如果模块需要手动设置 IIC 模式和地址：

```text
IIM-42352 CSB  -> 3V3
IIM-42352 MISO -> 3V3
```

IIC 扫描理想结果：

```text
0x68 -> DS3231
0x69 -> IIM-42352
```

如果 IIM-42352 固定为 `0x68`，会和 DS3231 冲突，应优先回到 SPI。

## 采样率

电脑脚本和固件支持：

```text
50 / 100 / 200 / 400 Hz
```

使用 DS3231 `32.768kHz` 作为外部时钟时，CSV 时间戳按实际采样率计算：

```text
50  -> 51.2 Hz
100 -> 102.4 Hz
200 -> 204.8 Hz
400 -> 409.6 Hz
```

RX 串口输出的时间戳已经是绝对采样 `epoch_us`，电脑脚本直接格式化为 CSV 时间。

固件启动时会配置并读回 IIM-42352 寄存器：

```text
IIM-42352 clk_in=1,int1_drdy=1,INTF_CONFIG1=0x..,INTF_CONFIG5=0x..,INT_SOURCE0=0x..,INT_CONFIG1=0x..
```

必须看到 `clk_in=1,int1_drdy=1`。如果出现 `IIM INT1 not seen yet` 或 `IIM INT1 timestamp missing`，说明 INT1 没有稳定提供 data-ready 中断，优先检查 INT1 连线、IIM 寄存器读回和 DS3231 32K 输入。

## IIM INT1 诊断

USB 测试固件默认使用 `INPUT_PULLUP + CHANGE` 诊断 INT1，启动日志会显示：

```text
IIM-42352 INT1 capture: pin=26,pullup=1,mode=CHANGE,level=...
```

运行中如果出现 `IIM INT1 timestamp missing`，重点看新增字段：

| 现象 | 含义 | 下一步 |
| --- | --- | --- |
| `fifo>0` 且 `raw_edges=0` | FIFO 有样本，但 GPIO26 没抓到任何 INT1 边沿 | 查 IIM `INT_CONFIG/INT_SOURCE0` 输出配置、INT1 电平模式、上拉/开漏 |
| `raw_edges` 持续增长，但 `int1_missing` 仍接近样本数 | GPIO26 有边沿，但时间戳队列和 FIFO 样本没有匹配上 | 查 ISR 队列容量、清空时机、FIFO 读取批量 |
| `raw_edges` 增长且 `int1_missing` 明显下降 | 原来的 `RISING` 不适合当前 INT1 极性/模式 | 对比 `CHANGE/FALLING/RISING`，固定最稳定模式 |

USB 报告里的 `ratio` 按设置的 `duration-seconds` 计算。如果测试中途点 Stop，例如只跑约 132 秒但 duration 仍是 600 秒，`ratio` 约为 `132/600=22%`，这不代表 TX 丢了 78%。

## 操作流程

1. 烧录 RX：
   ```powershell
   pio run -e heltec_wifi_lora_32_V3_rx -t upload --upload-port COM6
   ```

2. 烧录 TX1001：
   ```powershell
   pio run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001 -t upload --upload-port COM7
   ```

3. 烧录 TX1002：
   ```powershell
   pio run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002 -t upload --upload-port COM8
   ```

4. 启动电脑采集脚本：
   ```powershell
   python tools\serial_rx_to_file.py --port COM6 --output data --rotate-minutes 30 --with-header --sample-rate-hz 100 --start-delay-seconds 5
   ```

5. 观察 TX 串口或屏幕：
   ```text
   WAIT GPS/PPS：GPS 或 PPS 还没有稳定
   WAIT START：时间基准已准备好，等待 RX 广播 START_AT
   ARMED：已收到 START_AT，等待开始采样时刻
   RADIO READY / WAIT ACK：采样和轮询上传中
   ```

6. TX 正常启动时应看到：
   ```text
   DS3231 init ok: ... 32k=1 ...
   LC76G UART pins: RX=5 TX=6 PPS=4 baud=115200
   IIM-42352 clk_in=1,int1_drdy=1,...
   IIM-42352 range=+-4g, spi+fifo+DS3231 CLKIN+INT1 DRDY mode
   ```

默认不会再连续打印 `GPS NMEA:`。只有临时加 `-D APP_DEBUG_GPS_NMEA` 编译开关时才会打印完整 NMEA。

## 现场验证顺序

1. 只接 TX1001，验证 DS3231、GPS/PPS、IIM WHO_AM_I、FIFO。
2. 用示波器或逻辑分析仪测 DS3231 32K，应为 `32768 Hz`。
3. 测 IIM-42352 INT1，100Hz 档应约 `9.765625 ms` 一次，即实际 `102.4 Hz`。
4. 用 GPS PPS 做计数窗口，5 秒内 INT1 应约 `512` 个脉冲。
5. 采样率先用 `50Hz`，确认 CSV 时间连续。
6. 再加入 TX1002，做同一振动源互相关检查。
7. 按 `2 -> 4 -> 6 -> 10` 节点扩展。
8. `200/400Hz` 是压力测试，重点看 `fifo overflow`、`timestamp missing`、`TX queue full`、`TX ack timeout`。
