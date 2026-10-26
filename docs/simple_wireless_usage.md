# 简化一发一收：IIM-42352 → LoRa → USB 串口

这是当前默认构建方案。发送端固定为节点 1001，接收端只接收该节点。
发送端上电后自动初始化 IIM-42352、采集并发送；接收端上电后持续监听。

## 固定参数

- 开发板：Heltec WiFi LoRa 32 V3。
- 传感器：IIM-42352，SPI 接线沿用 `include/board_pins.h`。
- 采样：内部时钟，名义 50 Hz，±4 g，每 10 个样本发送一包。
- 无线：470 MHz、BW 250 kHz、SF7、CR 4/5，沿用项目无线参数。
- USB 串口：460800 波特率。
- 不初始化 GNSS、PPS、DS3231，不等待卫星定位或 START_AT，也不执行多节点轮询。
- 使用 FIFO 保留无线发送期间采集的样本，数据包包含设备号、序号、本地采样时间及 CRC。
- 这版使用串口查看状态，没有驱动 OLED。

IIM 的 RTC_MODE 被清除，采样不依赖外部 32 kHz。PIN9 保留为 CLKIN 输入，
以避免原接线中的 DS3231 输出与传感器 INT2 输出相互驱动。

## 文件

- `src/simple_wireless.cpp`：发送端、接收端入口。
- `include/simple_wireless_protocol.h`、`src/simple_wireless_protocol.cpp`：简化链路数据包。
- `lib/IIM42352Driver/src/IIM42352Driver.cpp`：新增内部时钟初始化；原 `begin()` 默认仍采用外部时钟。
- `platformio.ini`：两个 `simple` 环境。

## 编译与烧录

在项目根目录 PowerShell 中运行。COM7 为发送端，COM6 为接收端。
两端必须同时使用下面这套 simple 固件，简化数据包与原 GPS/PPS 固件不同。

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run

& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_simple_tx -t upload --upload-port COM7

& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_simple_rx -t upload --upload-port COM6
```

烧录前关闭占用串口的程序。上电后传感器初始化通过即开始采集，无须手动启动命令。

## 上位机保存 CSV

```powershell
python tools\serial_rx_to_file.py --port COM6 --baud 460800 --output data_simple_wireless --rotate-minutes 5 --sample-rate-hz 50 --range-g 4 --csv-timebase raw --no-start-at --with-header
```

GUI 仍可使用：

```powershell
python tools\serial_rx_to_file.py --simple-gui
```

选择 COM6、460800、50 Hz、4 g、raw，取消勾选 Send START_AT，点击 Start Capture。
采样率和量程在本固件中固定，GUI 的设置不会修改发送端。不要选择 pps-aligned。
GUI 打开采集窗口只控制电脑保存数据，发送端在通电且传感器正常时已经采集。

接收端数据行格式为：

```text
1001,发送端本地采样毫秒数,ax原始值,ay原始值,az原始值
```

上位机将原始值按 ±4 g 换算成 g，生成 `datetime,ax,ay,az` 四列 CSV。
CSV 日期时间以电脑首次接收时刻为基准，再叠加发送端样本时间间隔；
它是估算的时间轴，不代表 GPS/UTC 同步时间，也不能用于微秒级同步验证。
无线重传队列和 ACK 不在这版协议中，空口丢失的数据包不会补发。

## 日志含义

发送端正常启动：

```text
ROLE=SIMPLE_TX dev=1001 sample_hz=50 range_g=4 clock=internal
RADIO_READY
SENSOR_READY who=0x6D clock=internal sample_hz=50 range_g=4
TX_SENT dev=1001 seq=0 samples=10 status=0
```

接收端收到样本时打印 `RX_DATA` 和五列原始数据。

传感器初始化失败时，TX 每 2 秒重试，并在无线可用时发送故障状态；
RX 显示 `RX_SENSOR_ERROR ... reason=init_failed who=0xFF` 等信息，不生成伪造样本。
传感器持续无 FIFO 数据也会报告故障并重新初始化。

## 验证范围

简化 TX、简化 RX 和原 IIM GPS TX 均完成编译验证。
软件测试覆盖数据和故障包、CRC/长度/容量校验、64 位本地时间戳、内部时钟及原外部时钟模式。
按本次要求暂停硬件检查，这套新固件尚未进行实机采集验证。
