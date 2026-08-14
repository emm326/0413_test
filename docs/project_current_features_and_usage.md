# 当前项目功能与使用说明

当前工程保留两条采集路径：

1. `USB + GPS/PPS`：用于双节点时间戳同步验证和采集容量测试。
2. `P2P LoRa`：用于后续无线采集，多个 TX 由一个 HTIT-WB32LA RX 轮询接收。

传感器节点使用 IIM-42352 连续采样，LC76G 的 GPS PPS 提供 UTC 秒基准，DS3231 32.768 kHz 为 IIM 提供采样时钟。详细同步原理见 `docs/sensor-sync-09-principle.md`。

## USB PPS 校准默认设备与接线

```text
COM5 -> sensor1001
COM6 -> sensor1002

DS3231 32K -> IIM-42352 CLKIN
LC76G PPS   -> ESP32-S3 GPIO4
```

这里使用普通 `usb_gps_sync_test` 固件。不要烧录名称中含
`usb_gps_clk_sync_test` 的 GPIO3/DPLL 实验固件。IIM 连续采样，GPS PPS
只持续校准样本的 UTC 时间轴，不会每秒重启传感器，也不代表两个节点的
物理 INT1 采样沿达到微秒级同步。

## USB GPS 同步测试

构建：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test
```

烧录：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1001_usb_gps_sync_test -t upload --upload-port COM5
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e heltec_wifi_lora_32_V3_iim_gps_tx_1002_usb_gps_sync_test -t upload --upload-port COM6
```

启动默认 GUI（默认 10 分钟、±4 g、配置 50 Hz/实际 51.2 Hz）：

```powershell
python tools\tx_usb_capacity_test.py
```

命令行采集：

```powershell
python tools\tx_usb_capacity_test.py --ports COM5,COM6 --device-ids 1001,1002 --gps-sync --csv-timebase raw+pps-aligned --output data_usb_pps_sync --duration-seconds 600 --sample-rate-hz 50 --range-g 4
```

两个节点只有同时报告 GPS/PPS healthy 和如下自检状态后，主机才会发送同一个
未来整秒 `START_AT`：

```text
SENSOR_READY,1001,0x6D,1,1,int1_edges
SENSOR_READY,1002,0x6D,1,1,int1_edges
```

这表示 `WHO_AM_I=0x6D`、外部 CLKIN 已启用、INT1 DRDY 已启用且 INT1
边沿仍在增长。任何一项失败都会中止启动，不创建空 CSV。IIM 的 SPI 时钟已降为
1 MHz，以提高杜邦线连接的读写稳定性。

每个完整 UTC 秒会输出一条：

```text
PPS_ALIGN,pps_epoch_us=...,anchor_epoch_us=...,paired=...,dropped_1001=...,dropped_1002=...,missing_slots=...
```

输出目录为：

```text
data_usb_pps_sync/
  raw/sensor1001/<date>/tx_usb_capacity.csv
  raw/sensor1002/<date>/tx_usb_capacity.csv
  pps_aligned/sensor1001/<date>/tx_usb_capacity.csv
  pps_aligned/sensor1002/<date>/tx_usb_capacity.csv
  capture_report.txt
```

CSV 保持四列格式：

```text
datetime,ax,ay,az
```

`raw` 保留 GPS 校准后的真实 UTC 时间（不受电脑时区设置影响）；`pps_aligned` 每个 UTC 秒重新取共同锚点，
以 `19.53125 ms` 周期映射最近槽位。它不插值、不修改加速度值；某一节点缺少的
槽位会在两份校准 CSV 中同时跳过，因此两份文件的时间列和数据行数一致。

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
python tools\sensor_time_error_analysis.py --sensor-a data_usb_pps_sync\raw\sensor1001\<date>\tx_usb_capacity.csv --sensor-b data_usb_pps_sync\raw\sensor1002\<date>\tx_usb_capacity.csv --actual-rate-hz 51.2 --write-aligned
```

工具输出控制台误差报告，并可生成 `aligned_common_grid.csv`。当前验收标准是有效公共区间内最大匹配误差小于一个 51.2 Hz 采样周期，即 `19.53125 ms`。
