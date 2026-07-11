# 当前项目功能与使用说明

当前工程保留两条采集路径：

1. `USB + GPS/PPS`：用于双节点时间戳同步验证和采集容量测试。
2. `P2P LoRa`：用于后续无线采集，多个 TX 由一个 HTIT-WB32LA RX 轮询接收。

传感器节点使用 IIM-42352 连续采样，LC76G 的 GPS PPS 提供 UTC 秒基准，DS3231 32.768 kHz 为 IIM 提供采样时钟。详细同步原理见 `docs/sensor-sync-09-principle.md`。

## 默认设备

```text
COM6  -> sensor1001
COM12 -> sensor1002
```

## USB GPS 同步测试

构建：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test
```

烧录：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test -t upload --upload-port COM6
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test -t upload --upload-port COM12
```

启动 GUI：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\tx_usb_gps_sync_gui.py
```

命令行采集：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\tx_usb_capacity_test.py --ports COM6,COM12 --device-ids 1001,1002 --gps-sync --csv-timebase raw --output data1 --duration-seconds 60 --sample-rate-hz 50 --range-g 4
```

CSV 保持简单格式：

```text
datetime,ax,ay,az
```

## P2P 无线采集

默认 RX 与 IIM GPS TX 环境：

```text
heltec_wifi_lora_32_V3_rx
heltec_wifi_lora_32_V3_iim_gps_tx_1001
heltec_wifi_lora_32_V3_iim_gps_tx_1002
```

无线协议使用 polling、ACK/grant、burst 和 TX 队列控制。RX/TX 必须烧录同一版本固件，详细流程见：

- `docs/p2p_polling_ack_start_anchor_usage.md`
- `docs/p2p_ack_grant_burst_update.md`

## 同步误差和对齐

USB 与无线采集生成相同四列 CSV 后，统一使用：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\sensor_time_error_analysis.py --sensor-a data1\sensor1001\20260711\tx_usb_capacity.csv --sensor-b data1\sensor1002\20260711\tx_usb_capacity.csv --actual-rate-hz 51.2 --write-aligned
```

工具输出控制台误差报告，并可生成 `aligned_common_grid.csv`。当前验收标准是有效公共区间内最大匹配误差小于一个 51.2 Hz 采样周期，即 `19.53125 ms`。
