# HTIT-WB32LA + IIM-42352 + LC76G + DS3231 多节点同步采集说明

功耗、LoRa airtime、USB/无线模式逐模块估算见 `docs/power_consumption_estimate.md`。

## GPS-disciplined CLKIN 硬件采样同步模式

当前 IIM-42352 GPS 同步 TX 固件新增 `APP_IIM42352_GPS_DISCIPLINED_CLKIN=1` 模式，用于把 raw 模式真实采样边沿尽量同步到 GPS 秒边界。

新模式下接线按下面执行：

```text
断开 DS3231 32K -> IIM-42352 INT2/CLKIN
ESP32 GPIO3 -> IIM-42352 INT2/CLKIN
GPS PPS -> ESP32 GPIO4
IIM-42352 INT1 -> ESP32 GPIO26
DS3231 SDA/SCL 保留，但 DS3231 32K 不再作为 IIM 采样时钟
```

不要把 GPS PPS 直接接到 IIM-42352 CLKIN。PPS 是 1Hz 秒脉冲，而 IIM-42352 CLKIN 需要约 31-50kHz 外部时钟；固件现在由 ESP32 在 GPIO3 输出 `32768Hz` 方波给 IIM CLKIN。

采样启动时，`START_AT` 会对齐到未来 GPS 整秒边界。到达边界后 TX 会重启 GPIO3 的 32768Hz CLKOUT、重启 IIM 加速度采样、清 FIFO/INT1 队列，并丢弃首 1 秒样本作为同步和滤波稳定时间。USB GPS 同步测试会额外输出：

```text
CLKSYNC,dev,pps_epoch_us,clk_hz,pps_interval_us,phase_error_us,first_int1_offset_us,healthy
```

重点看：

```text
clk_hz=32768
healthy=1
first_int1_offset_us 两个 TX 的差值是否稳定小于 500-1000us
raw Analyze time error 的 nearest_mean_ms / nearest_p95_ms 是否降到 1ms 左右或以内
```

如果 raw 误差仍明显大于 1ms，说明 ESP32 本地生成 32768Hz 还不能满足导师要求的硬件同步精度，下一步应改为外接 GPS 锁定 32768Hz 时钟或公共时钟分配小板。

当前工程使用 P2P GPS/PPS 同步方案。

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
GPS NMEA + PPS 建立 UTC epoch_us 与 TX 本机 micros() 的双向映射
DS3231 32.768kHz 输入 IIM-42352 INT2/CLKIN，作为传感器采样时钟
IIM-42352 INT1 数据就绪中断给每个 FIFO 样本提供本机 micros() 时间锚点
TX 采样过程中每个 GPS PPS 周期刷新一次 UTC <-> 本机 micros() 映射
RX 按设备编号轮询 TX
TX 被点名才发送队列头部数据
TX 在本轮 grant 内连续发送最多 burst 个队列包
TX 发送成功后立即从队列删除该包
RX 在同一节点窗口内连续接收 DATA，不再每包 ACK
TX 用最新 GPS/PPS 映射把包首样本 micros() 换算成 t0_us
RX 用 runStartEpochUs + t0_us + 样本间隔还原 epoch_us
PC 将 RX 输出写入 CSV
```

当前振动数据帧协议版本为 `v2`，每包头部使用微秒偏移 `t0_us`，不是旧版毫秒偏移 `t0_ms`。RX 和所有 TX 必须一起重新烧录，不能混用旧固件。

## GPS/PPS 周期性时间校准

当前 TX 固件不是只在开始时校准一次。启动采样时，`START_AT epoch_us` 仍用于让不同 TX 在同一个绝对时间点开始；采样过程中，TX 会持续读取 LC76G 的 NMEA 时间和 PPS 脉冲。

每当 TX 拿到一组有效的 NMEA 秒时间和 PPS 边沿，就会刷新一次映射关系：

```text
GPS epoch_us at PPS <-> TX local micros() at PPS
```

之后每个包的首样本时间按下面方式计算：

```text
sample_epoch_us = latest_pps_epoch_us + (sample_local_micros - latest_pps_local_micros)
t0_us = sample_epoch_us - START_AT_epoch_us
```

也就是说，`t0_us` 不再只依赖开始时那一次 `START_AT` 本地偏移，而是会尽量使用最新 GPS/PPS 时间锚点。这样可以向导师说明：系统启动时做同步启动，运行过程中继续用 PPS 周期性刷新 TX 本地时间与 GPS 绝对时间的关系。

TX 串口中应能看到类似日志：

```text
GPS sync: dev=1001,pps_epoch_us=1782712863000000,pps_local_us=123456789,pps_interval_us=1000000,age_ms=0
```

含义：

```text
pps_epoch_us     GPS 绝对时间轴上的这个 PPS 时刻
pps_local_us     TX 端 ESP32 捕获这个 PPS 时的本机 micros()
pps_interval_us  相邻两个 PPS 的本机间隔，理想约 1000000 us
age_ms           当前使用的 GPS/PPS 时间锚点距现在多久
```

如果 GPS/NMEA 暂时中断但之前已有有效锚点，TX 会继续用最后一次有效映射给样本打时间，同时打印：

```text
GPS sync stale: dev=1001,age_ms=...,pps_interval_us=...
```

这表示时间基准已经变旧，需要检查 GPS 天线、NMEA 串口或 PPS 信号，但固件不会立刻停止采样。

### USB GPS 同步验证

普通 USB 测试只验证采样链路，不验证 GPS 时间同步。要验证两个 TX 是否落在同一 GPS 时间轴，需要烧录 GPS 同步 USB 测试固件：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test -t upload --upload-port COM14
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test -t upload --upload-port COM6
```

然后运行：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\tx_usb_capacity_test.py --gps-sync --ports COM14,COM6 --device-ids 1001,1002 --baud 460800 --sample-rate-hz 50 --duration-seconds 600 --start-delay-seconds 10 --samples-per-packet 24 --output data_tx_usb_gps_sync
```

GPS 同步 USB 固件会额外输出：

```text
TXG,dev_id,pps_epoch_us,pps_local_us,pps_interval_us,age_ms
```

工具报告中重点看：

```text
gps_sync_seen=1
gps_sync_stale_count=0
first_epoch_delta_ms=...
last_epoch_delta_ms=...
```

50Hz 使用 DS3231 32.768kHz 时实际采样率约 `51.2Hz`，一个采样周期约 `19.53125ms`。如果 `first_epoch_delta_ms` 小于一个采样周期，说明两个 TX 的首样本已经可以放到同一个 GPS 时间轴上进行后处理对齐。

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

USB 测试环境分两类：

```text
heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_test
heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_test
```

用于验证 TX 采样、INT1、FIFO、USB 串口输出能力，不要求 GPS/PPS。

```text
heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test
heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test
```

用于验证 TX USB 输出也落在 GPS/UTC 统一时间轴上，必须等待 GPS/PPS 稳定。

旧 P2P ADXL345 环境仍在 `platformio.ini` 中保留，用于回退测试。

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

## 量程选择

IIM-42352 现在支持在每次实验开始前选择加速度量程：`±4g / ±8g / ±16g`。默认仍是 `±4g`，旧实验流程不需要改命令也能继续使用。

三档建议如下：

```text
±4g：分辨率最高，适合小振动、环境振动、桥梁微弱响应。
±8g：折中，适合普通敲击、人工激励或幅值不确定的测试。
±16g：不容易饱和，适合冲击较大场景，但小信号分辨率降低。
```

固件端会在 `START_AT` 开始采样前把量程写入 IIM-42352 `ACCEL_CONFIG0` 的量程位；PC 采集脚本会用相同量程自动设置 CSV 幅值换算比例：

```text
4g  -> 0.0001220703125 g/count
8g  -> 0.0002441406250 g/count
16g -> 0.0004882812500 g/count
```

USB GPS 同步测试示例：

```powershell
python tools/tx_usb_capacity_test.py --ports COM7,COM8 --device-ids 1001,1002 --sample-rate-hz 50 --range-g 8 --csv-timebase raw
```

P2P RX 采集示例：

```powershell
python tools/serial_rx_to_file.py --port COM6 --output data --rotate-minutes 30 --with-header --sample-rate-hz 50 --range-g 8
```

量程改变只影响幅值比例，不影响 GPS/PPS 时间同步、DS3231 32.768kHz 导致的 `51.2Hz / 102.4Hz` 修正，也不改变 `raw` / `aligned` 时间轴逻辑。一次 run 开始后不支持中途动态切换量程；要换量程，应重新发送 `START_AT` 或重新开始采集。

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
