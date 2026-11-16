# E870 八节点采集项目交接文档

交接日期：2026-11-16（Asia/Hong_Kong）
仓库分支：`codex/sensor-recovery-20261114`
当前 Git 提交：`f9bda89`（fix: 修复IIM42652内部时钟采样门槛）
工作区状态：干净

## 1. 当前结论

IIM42652 传感器已经能够正常启动和正常采样。当前设备使用传感器内部时钟，室外 GNSS/PPS 正常，正式 tx 固件已经烧录到 COM8。当前没有主动采样，因为节点处于 Waiting，正在等待 START 命令。

| 功能 | 当前状态 | 结论 |
|---|---|---|
| 传感器身份 | 已通过 | WHO_AM_I=`0x6F` |
| 传感器启动自检 | 已通过 | 有效 FIFO 样本、身份和配置检查均通过 |
| 50 Hz 采样 | 已通过 | USB 60 秒和正式 tx 10 秒实测通过 |
| GNSS 定位 | 已通过 | `fix=1` |
| PPS 秒脉冲 | 已通过 | 间隔约 999980～999983 µs |
| UTC/PPS 时间映射 | 已通过 | `time=1, healthy=1` |
| 外部 32 kHz CLKIN | 不可用 | 固件自动回退到传感器内部时钟 |
| 内部时钟采样门槛 | 已修复 | `clock_ok=1, clock_source=sensor_internal` |
| LoRa 射频初始化 | 已通过 | `READY,sensor=1,radio=1` |
| LoRa 实际收包与 ACK | 待实测 | 尚无 E870 在线收包证据 |
| 8 节点扩容 | 待实测 | 单节点正式空口验收尚未完成 |

## 2. 当前设备状态

以下为 2026-11-16 交接前 COM8 实读状态：

```text
STATUS,id=1003,state=1,gps=1,clock_hz=10.4167,ppm=-999674.50,
clock_ok=1,clock_source=sensor_internal,queue=0,
lost_samples=0,lost_packets=0
```

解释：

- `state=1`：Waiting。节点已就绪，但当前没有运行采样。
- `gps=1`：GNSS/PPS 时间基准健康。
- `clock_ok=1`：当前采样时钟门槛通过。
- `clock_source=sensor_internal`：使用 IIM42652 内部时钟。
- `clock_hz` 和 `ppm` 无效：该字段仍在测量已经停用的 GPIO36 外部 32 kHz，不能用于内部时钟判定。
- `queue=0,lost_samples=0,lost_packets=0`：当前没有积压和丢失。

如果现在要开始采样，必须发送有效的 `START_AT` 命令或由 E870 上位机下发 START。仅上电不会自动开始采样。

## 3. 硬件与接线

当前设备：EoRa-S3-400TB + YBX-IIM42652，节点 1003。

| 信号 | EoRa GPIO |
|---|---:|
| IIM42652 SCK | GPIO47 |
| IIM42652 MOSI | GPIO48 |
| IIM42652 MISO | GPIO39 |
| IIM42652 CSB | GPIO40 |
| IIM42652 INT1 | GPIO38 |
| IIM42652 CLKIN | GPIO36 |
| GNSS TX → 主控 RX | GPIO16 |
| GNSS RX ← 主控 TX | GPIO15 |
| GNSS PPS → 主控 | GPIO41 |

供电与接地沿用现有接线。PPS 指示灯正常闪烁。不要自行改 GPIO、电压或接反 GNSS RX/TX。

## 4. 固件行为

- 上电先尝试 GPIO36 外部 32 kHz。
- 外部时钟不可用时关闭 CLKIN，重新用 `external_clock=0` 初始化 IIM42652。
- 内部时钟模式不要求 GPIO36 边沿，也不要求外部 60 秒频率测量。
- 内部时钟模式仍要求传感器启动自检、有效 FIFO 样本、有效 XYZ 和 WHO_AM_I。
- 采样间隔使用 FIFO 内建时间戳。
- FIFO 每 5 ms 轮询一次，批量 SPI 读取。
- 单次 FIFO 故障执行恢复；连续 3 次失败才进入 Fault。
- 正式模式仍要求 GNSS/PPS 健康，不会因为允许内部时钟而跳过 GNSS/PPS 门槛。

## 5. 已通过的实机验收

### 5.1 启动和身份

```text
SENSOR_ID,name=IIM-42652,expected=0x6F,observed=0x6F,ok=1
SENSOR_TIMESTAMP,config=0x29,rtc_mode=0,enabled=1,
resolution_bit=1,resolution_us_x1000=17,ok=1
SENSOR_CHECK,init=1,executed=1,drdy=6,fifo_samples=16,
clock_edges=0,clock_source=sensor_internal,sample_valid=1,
identity_ok=1,ok=1
READY,sensor=1,radio=1,clock_output=0,
clock_source=sensor_internal,clock_measurement=sensor_internal_fifo
```

启动自检中读到的样本为：

```text
SENSOR_SAMPLE,valid=1,x=1627,y=838,z=7160
```

### 5.2 USB 测试 run 7010

配置：50 Hz、±4 g、duration=60 秒。

```text
samples=2973
fifo_errors=0
invalid=0
lost_samples=0
ts_anomaly=0
sample_dt_mean_us=20000
sample_dt_min_us=19985
sample_dt_max_us=20002
status_gps_ok=14/14
status_clock_ok=14/14
pass=1
```

结论：传感器连续采样稳定，FIFO 错误、非法样本、丢样和时间戳异常均为 0。

### 5.3 正式 tx run 7011

配置：50 Hz、±4 g、duration=10 秒。

```text
valid=500
invalid=0
fifo_errors=0
lost_samples=0
ts_anomaly=0
```

节点自动执行 `SENSOR_STANDBY`。测试中没有在线网关 ACK，因此队列保留 20 包。这个结果证明采样和发送排队正常，不证明 E870 已收到数据。

## 6. 构建与烧录

当前正式环境：

```text
eora_s3_400_iim42652_tx
```

USB 串口测试环境：

```text
eora_s3_400_iim42652_usb_test
```

无 GNSS 台架环境：

```text
eora_s3_400_iim42652_bench
```

当前设备保留正式 `eora_s3_400_iim42652_tx`。COM8 为交接时的端口，新对话开始后必须用设备枚举重新确认，不能只凭 COM8 烧录。

构建和烧录命令：

```powershell
$env:PLATFORMIO_DISABLE_TELEMETRY='1'
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx -t upload --upload-port COM8
```

软件专项测试：

```powershell
& '.\.venv\Scripts\python.exe' -m unittest tests.test_firmware_sensor -v
```

最近结果：3/3 通过。

## 7. 串口诊断命令

当前正式固件仍提供以下串口命令：

- `STATUS`：通过 LoRa 上报状态；串口每 5 秒也输出 STATUS。
- `GPS_DIAG`：输出 GNSS/PPS 详细状态。
- `START_AT epoch_us 50 4 run_id duration_s`：启动指定运行。
- `STOP`：停止当前运行。
- `SENSOR_RETRY`：重新初始化传感器，会操作寄存器。
- `SENSOR_SAMPLE_TEST`：内部时钟直接采样诊断，会改变现场状态。
- `SENSOR_LINK_TEST`：SPI 链路诊断，会改变现场状态。

不要在不了解影响时执行 `SENSOR_RETRY`、`SENSOR_SAMPLE_TEST` 或 `SENSOR_LINK_TEST`。

## 8. 尚未完成

1. E870 网关和上位机需要在线。
2. 需要验证正式 START 的 CONFIRM、STATUS、SAMPLE 和 ACK。
3. 需要完成正式 50 Hz 60 秒空口验收。
4. 需要检查主机 CSV、到达率和补传率。
5. 单节点通过后，再按 1→2→4→8 节点扩容。
6. 8 节点 50 Hz 连续 2 小时通过后，才进入 100 Hz/SF5。

## 9. 新对话接手顺序

1. 先读本文件和根目录 [PROJECT_PLAN.md](../PROJECT_PLAN.md)。
2. 运行 `git status --short --branch`、`git log -5 --oneline`，确认基线 `f9bda89`。
3. 读取[本轮报告](changes/2026-11-16_04_内部时钟采样门槛修复.md)。
4. 识别当前 COM 口和设备，不要默认仍为 COM8。
5. 读取 STATUS 和 GPS_DIAG，确认 `gps=1,clock_ok=1,clock_source=sensor_internal`。
6. 需要验证采样时，先跑 USB 60 秒，再烧录正式 tx。
7. 需要验证 LoRa 时，先启动 E870 和上位机，再下发正式 START。
8. 不把 `clock_hz=0` 或低值当作内部时钟故障；内部模式只看传感器自检、FIFO 和错误计数。

## 10. Git 与本地资料

- 当前分支：`codex/sensor-recovery-20261114`
- 当前提交：`f9bda89`
- 关键前序提交：`cb099e7`、`1b76a12`、`4cadadd`
- 当前报告：[内部时钟采样门槛修复](changes/2026-11-16_04_内部时钟采样门槛修复.md)
- 长期接线文档：[wiring_and_flash.md](wiring_and_flash.md)
- 协议文档：[protocol_v3.md](protocol_v3.md)
- 网关操作：[e870_setup.md](e870_setup.md)

采集数据、日志、构建产物和镜像不进入 Git。以下本机日志可直接用于新对话时核对：

```text
tmp/logs/run7010_internal_clock_60s_20261116.log
tmp/logs/tx7011_internal_clock_short_20261116.log
tmp/logs/tx_boot_after_clock_fix_20261116.log
tmp/logs/handover_live_check_20261116.log
```

这些文件不提交 Git，新电脑或重新克隆仓库后可能不存在。
