# HTIT-WB32LA 单接收端多发送端 P2P 轮询 ACK 方案说明

本文记录当前工程新增的“不使用网关，仅用一块 HTIT-WB32LA 作为接收端，接收 10 个发送端”的 P2P LoRa 方案。当前日期：2026-05-21。

## 1. 项目当前定位

本工程现在同时保留两条通信路线：

- 原始 P2P LoRa 路线：`多个 TX -> 一个 HTIT-WB32LA RX -> 电脑串口 -> CSV`
- LoRaWAN 路线：`TX -> LoRaWAN 网关 -> LoRaWAN 服务端 -> 电脑脚本 -> CSV`

本次新增和推荐使用的是第一条 P2P 路线，适合“不使用网关设备，只用 HTIT-WB32LA 接收端”的场景。

核心目标：

- 一个 RX 接收 10 个 TX。
- 每个 TX 有独立设备编号：`1001` 到 `1010`。
- RX 按设备编号轮询，避免多个发送端同时发包。
- TX 未收到 ACK 时不删除数据包。
- CSV 时间栏使用采样发生时的时间，不使用电脑收到数据的时间。
- 电脑端按设备编号自动分目录保存 CSV。

## 2. 本次新增能力

### 2.1 10 个 P2P 发送端环境

`platformio.ini` 新增了 10 个 P2P TX 环境：

```text
heltec_wifi_lora_32_V3_tx_1001
heltec_wifi_lora_32_V3_tx_1002
heltec_wifi_lora_32_V3_tx_1003
heltec_wifi_lora_32_V3_tx_1004
heltec_wifi_lora_32_V3_tx_1005
heltec_wifi_lora_32_V3_tx_1006
heltec_wifi_lora_32_V3_tx_1007
heltec_wifi_lora_32_V3_tx_1008
heltec_wifi_lora_32_V3_tx_1009
heltec_wifi_lora_32_V3_tx_1010
```

这些环境都使用：

```text
ROLE_TX
APP_LORA_FREQUENCY_MHZ_VALUE=470.0f
APP_TX_STARTUP_JITTER_MAX_MS=0
APP_TX_PACKET_JITTER_MAX_MS=0
```

随机延迟已经取消，避免因为大 `delay()` 阻塞采样导致 ADXL345 FIFO 溢出。

### 2.2 RX 轮询 TX

RX 按设备编号轮询：

```text
1001 -> 1002 -> 1003 -> ... -> 1010 -> 1001 ...
```

新增控制帧：

```text
START_AT  采集开始时间广播
POLL      RX 点名某个 TX
DATA      TX 发送振动数据包
ACK       RX 确认收到指定 dev_id + seq
NO_DATA   TX 告诉 RX 当前没有待发送数据
```

TX 只有收到发给自己的 `POLL` 后才发送数据。RX 成功解码 DATA 后回 ACK。TX 只有收到匹配 ACK 后才删除队列头部数据包。

### 2.3 简化时间锚定

电脑端脚本启动后会自动给 RX 串口发送：

```text
START_AT <epoch_us> <sample_rate_hz> <delay_ms>
```

例如：

```text
START_AT 1774089141854600 100 5000
```

含义：

- `epoch_us`：电脑当前时间加未来延迟后的绝对开始时间，单位微秒。
- `sample_rate_hz`：本次采集采样率，例如 `25`、`50`、`100`、`200`。
- `delay_ms`：从 RX 当前收到命令后，到真正开始采样的延迟。

RX 会在开始前重复广播 START_AT。TX 收到后：

- 清空旧发送队列。
- 设置本次采集开始时间。
- 等到开始时刻才启动采样。
- 后续数据包里的 `t0_ms` 表示“距离本次采集开始的毫秒偏移”。

RX 收到 DATA 后计算每个样本的绝对采样时间：

```text
sample_epoch_us = run_start_epoch_us + t0_ms * 1000 + sample_index * sample_period_us
```

不同采样率对应的样本间隔：

```text
25 Hz  -> 40000 us
50 Hz  -> 20000 us
100 Hz -> 10000 us
200 Hz -> 5000 us
```

电脑脚本收到 RX 输出的 `epoch_us` 后直接格式化为 CSV 时间，例如：

```text
2026/4/18 19:52:21.8546
```

因此即使数据包在 TX 队列中等待几秒或几分钟后才发出，CSV 中的时间仍然是采样时刻。

## 3. 当前主要文件

核心固件：

```text
src/main.cpp
src/protocol.cpp
src/lora_transport.cpp
include/config.h
include/protocol.h
```

电脑端 CSV 工具：

```text
tools/serial_rx_to_file.py
```

新增本文档：

```text
docs/p2p_polling_ack_start_anchor_usage.md
```

## 4. 优点

### 4.1 不需要网关

只需要：

- 1 块 HTIT-WB32LA 作为 RX。
- 最多 10 块 HTIT-WB32LA 作为 TX。
- 1 台电脑运行串口采集脚本。

适合现场没有 LoRaWAN 网关、只想快速建立多节点采集系统的场景。

### 4.2 设备编号清晰

每个 TX 固件环境固定一个编号：

```text
TX1001 -> APP_DEVICE_ID_VALUE=1001
TX1002 -> APP_DEVICE_ID_VALUE=1002
...
TX1010 -> APP_DEVICE_ID_VALUE=1010
```

RX 串口输出会带设备编号，电脑端会按设备编号分目录保存。

### 4.3 避免同频同时发送

RX 主动轮询，TX 被点名才发，避免 10 个 TX 自由发送时严重撞包。

### 4.4 ACK 后才删除

TX 发送队列里的数据只有在收到 RX 的 ACK 后才删除。短时间无线失败不会立刻丢掉队列头部数据。

### 4.5 CSV 时间更接近真实采样时间

时间不再依赖“电脑收到串口数据的时间”。CSV 时间由统一开始时间和采样偏移计算，适合后续振动数据对齐分析。

## 5. 缺点和限制

### 5.1 单 RX 空口吞吐有限

一个 SX1262 接收端同一时刻只能接收一个 LoRa 包。10 个发送端在 `100 Hz` 或 `200 Hz` 下，原始三轴数据量很大，可能超过单个 RX 的实时接收能力。

表现：

- TX 队列深度持续增加。
- 串口出现 `TX queue full`。
- 高采样率下历史数据越积越多。

### 5.2 当前 TX 队列是 RAM 队列

当前 TX 队列容量默认：

```text
APP_TX_QUEUE_PACKET_CAPACITY_VALUE=16
```

队列满时会丢弃新打包的数据，并打印：

```text
TX queue full
```

如果现场必须保证 100/200 Hz 下长时间不丢数据，需要后续增加 SD 卡或 Flash 文件系统缓存。不过内置 Flash 不适合长期高频写入，优先建议 SD 卡。

### 5.3 START_AT 不是高精度硬件同步

本方案是简化时间锚定，适合毫秒级和常规采样时间对齐。它不是 GPS、PPS、外部时钟这种硬同步方案。

如果需要亚毫秒级多节点同步，需要额外硬件同步方案。

### 5.4 每次切换采样率需要重新开始一轮采集

一次采集 run 建议使用固定采样率。如果要从 `50 Hz` 切到 `100 Hz`，应重新运行电脑采集脚本或重新发送 START_AT。

## 6. 烧录流程

### 6.1 烧录 RX

假设 RX 是 `COM6`：

```powershell
pio run -e heltec_wifi_lora_32_V3_rx -t upload --upload-port COM6
```

如果当前终端没有 `pio` 命令，可使用：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_rx -t upload --upload-port COM6
```

### 6.2 烧录 TX

示例：

```powershell
pio run -e heltec_wifi_lora_32_V3_tx_1001 -t upload --upload-port COM7
pio run -e heltec_wifi_lora_32_V3_tx_1002 -t upload --upload-port COM8
pio run -e heltec_wifi_lora_32_V3_tx_1003 -t upload --upload-port COM9
pio run -e heltec_wifi_lora_32_V3_tx_1010 -t upload --upload-port COM10
```

按实际 COM 口替换。

### 6.3 推荐先少量节点验证

建议顺序：

1. 只烧录并打开 RX。
2. 只加入 TX1001。
3. 确认 CSV 中生成 `sensor1001`。
4. 再加入 TX1002。
5. 最后逐步加到 10 个 TX。

不要第一次就同时打开 10 个节点和 200 Hz。

## 7. 采集流程

### 7.1 命令行采集

假设 RX 串口是 `COM6`，输出目录是 `data`，采样率是 `100 Hz`：

```powershell
python tools\serial_rx_to_file.py --port COM6 --output data --rotate-minutes 30 --with-header --sample-rate-hz 100 --start-delay-seconds 5
```

脚本默认会：

- 打开 RX 串口。
- 等待 `2s`，避免板子串口打开后复位还没完成。
- 生成未来 `5s` 的开始时间。
- 向 RX 发送 `START_AT`。
- 解析 RX 输出的绝对 `epoch_us`。
- 写入 CSV。

如果不想自动发送 START_AT，可加：

```powershell
--no-start-at
```

### 7.2 GUI 采集

运行：

```powershell
python tools\serial_rx_to_file.py --gui
```

GUI 中新增了：

- `Send START_AT`
- `Start Delay s`
- `Sample Hz`
- `Boot Wait s`

### 7.3 输出目录

当使用：

```powershell
--output data --rotate-minutes 30
```

输出示例：

```text
data/sensor1001/20260521/153000_to_155959.csv
data/sensor1002/20260521/153000_to_155959.csv
...
data/sensor1010/20260521/153000_to_155959.csv
```

CSV 内容：

```text
datetime,ax,ay,az
2026/4/18 19:52:21.8546,0.631800,0.058500,0.838500
```

如需把设备编号也写入每一行，可加：

```powershell
--include-id
```

## 8. 运行时串口日志含义

RX 常见日志：

```text
RX START_AT armed
RX START_AT broadcast
RX polling started
RX ok
RX no data
RX poll timeout
```

TX 常见日志：

```text
TX START_AT armed
TX capture started
TX queued
TX data sent
TX acked
TX ack timeout
TX queue full
```

重点关注：

- `TX ack timeout`：某次发送没有收到 ACK，会保留队列头，后续重发。
- `TX queue full`：TX 采样产生速度超过可发送速度，当前 RAM 队列已满，开始丢新包。
- `RX poll timeout`：被轮询的设备没有及时回应，可能是设备未开机、信号弱、队列为空但 NO_DATA 丢失、或参数不一致。

## 9. 当前验证状态

已完成编译验证：

```powershell
python -m py_compile tools\serial_rx_to_file.py
pio run -e heltec_wifi_lora_32_V3_rx
pio run -e heltec_wifi_lora_32_V3_tx_1001
pio run -e heltec_wifi_lora_32_V3_tx_1010
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1001
```

结果均已通过。

## 10. 推荐现场测试顺序

### 第一阶段：单节点

```powershell
python tools\serial_rx_to_file.py --port COM6 --output data --rotate-minutes 30 --with-header --sample-rate-hz 25 --start-delay-seconds 5
```

只打开 TX1001，确认：

- RX 有 `RX ok`。
- TX 有 `TX acked`。
- 生成 `data/sensor1001/...csv`。
- CSV 时间连续递增。

### 第二阶段：两个节点

打开 TX1001 和 TX1002，确认：

- `sensor1001` 和 `sensor1002` 都有文件。
- 两个设备都能收到 ACK。
- `TX queue full` 不出现或很少出现。

### 第三阶段：10 个节点低采样率

先用 `25 Hz`，再试 `50 Hz`。

重点观察：

- 每个设备是否都有数据。
- 队列是否持续增长。
- 是否出现大量 `TX queue full`。

### 第四阶段：高采样率压力测试

再测试 `100 Hz` 和 `200 Hz`。如果 10 个节点同时高采样率运行，出现队列增长属于预期风险。

处理建议：

- 降低采样率。
- 减少同时在线节点数。
- 增加接收端数量并分组。
- 后续增加发送端 SD 卡缓存。

## 11. 后续优化方向

建议优先级：

1. 增加 TX 队列深度统计并写入质量日志。
2. 给电脑端 CSV 增加 `quality.csv`，记录每个设备 ACK、超时、重发和队列状态。
3. 增加 SD 卡缓存，解决高采样率下 RAM 队列容量不足问题。
4. 根据实测数据调整 `P2P_POLL_RESPONSE_TIMEOUT_MS`。
5. 如果 10 节点 100/200 Hz 必须实时完整传输，考虑多 RX 分组或重新使用 LoRaWAN 网关。

