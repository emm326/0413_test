# GPS/PPS 单节点无线采集

本文适用于保留的 GPS/PPS 方案。当前默认的简化一发一收版本见 `docs/simple_wireless_usage.md`。

GPS/PPS 方案链路：IIM-42352 + GPS/PPS + DS3231 32K 节点 1001 → LoRa RX → 电脑。

`platformio.ini` 的 P2P 首节点为 1001、节点数为 1，RX 只向 1001 发起轮询。
该方案需显式选择 RX 和 IIM GPS TX 1001 环境；默认构建已切换到 simple TX/RX。

## 烧录

在项目根目录的 PowerShell 中运行。以下假设 TX 为 COM5、RX 为 COM6，
请用实际串口替换。烧录前关闭占用串口的上位机和串口监视器。

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" device list
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001 -t upload --upload-port COM5
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_rx -t upload --upload-port COM6
```

RX 和 TX 都应重新烧录。RX 启动日志应显示 `P2P cfg: nodes=1`，
不应再出现 `RX poll timeout: dev=1002`。

## 上位机

```powershell
python tools\serial_rx_to_file.py --gui
```

关闭旧窗口并重新启动上位机。选择 RX 串口 COM6、460800 波特率、50 Hz、±4 g，
勾选 Send START_AT。保存周期可设为 5 分钟。默认 raw 保存接收到的真实 UTC 时间戳。
节点固件需要 GPS/PPS 和外部时钟就绪，待就绪后点击 Start Capture。

直接命令行采集并每 5 分钟保存一个 CSV：

```powershell
python tools\serial_rx_to_file.py --port COM6 --baud 460800 --output 1026test --rotate-minutes 5 --sample-rate-hz 50 --range-g 4 --csv-timebase raw --with-header
```

CSV 路径为 `1026test/sensor1001/<UTC日期>/<时间段>.csv`，列为
`datetime,ax,ay,az`。命令行按 Ctrl+C 停止。

上位机也支持单节点 pps-aligned，默认设备仅为 1001：

```powershell
python tools\serial_rx_to_file.py --port COM6 --baud 460800 --output data_single_pps --rotate-minutes 5 --sample-rate-hz 50 --range-g 4 --csv-timebase pps-aligned --with-header
```

该模式按 UTC 秒缓存并生成逻辑时间网格；下一秒数据到达或停止采集时写出，
不会等待 1002。需要真实采样时间时使用 raw。

如果以后恢复双节点，除恢复 RX/TX 无线节点数配置外，命令行须显式传入
`--pps-aligned-device-ids 1001,1002`。

## 持续超时

原日志中 1001 和 1002 均超时。改为单节点只会取消对 1002 的轮询，
不能据此确认 1001 已建立通信。如果重新烧录后仍显示
`RX poll timeout: dev=1001`，需要查看 TX 串口日志，确认无线 TX 固件、
GPS/PPS 健康状态、传感器及 DS3231 时钟就绪状态，再重新开始采集。

## 无线接收恢复修复（2026-10-26）

本次保持单节点 1001 的无线协议与原接线，调整以下运行行为：

- 无线收发层每隔至少 5 ms 查询芯片 IRQ 状态，补充 DIO1 引脚检测；收到接收超时、包头错误等状态时重新进入接收模式。
- 恢复接收后保留原来的错误码，发送端也会打印 `TX radio event`，避免错误被恢复成功的状态覆盖。
- TX 主循环先处理无线控制帧；GPS UART 每次最多处理 512 字节，并保留未完成的 NMEA 语句供下一轮继续处理。

修改后需要重新烧录 TX 1001 和 RX。编译与模拟硬件故障测试已通过；这验证了异常恢复逻辑，实际无线链路仍需在两块板上验证。
