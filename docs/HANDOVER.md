# E870 八节点采集项目交接文档

交接日期：2026-11-16（Asia/Hong_Kong）
仓库分支：`codex/20261116-baseline`
当前功能固件提交：`cb99db0`（fix: 增加传感器启动自动恢复与时钟回退）
交接文档提交：以 `git log -1 --oneline` 为准
工作区状态：本轮提交后干净

## 1. 当前结论

IIM42652 传感器已经能够自动启动和正常采样。启动自检最多重试 3 次，无效外部 32 kHz 在空闲状态会自动回退到传感器内部时钟。2026-11-16 18:08 再次完成一次冷启动，传感器在 `attempt=1` 通过，节点进入 `state=1`，启动后没有进入 `state=5`。正式 tx 固件保留在 COM8，当前没有主动采样，因为节点处于 Waiting。GNSS/PPS 健康状态仍会波动，正式 START 必须以实时 `GPS_DIAG` 的 `healthy=1` 为准。

| 功能 | 当前状态 | 结论 |
|---|---|---|
| 传感器身份 | 已通过 | WHO_AM_I=`0x6F` |
| 传感器启动自检 | 已通过 | 有效 FIFO 样本、身份和配置检查均通过 |
| 启动自动恢复 | 已通过 | 4 次冷启动均自动进入 `state=1` |
| 50 Hz 采样 | 已通过 | 最新台架 60 秒 2973 样，丢样和 FIFO 错误均为 0 |
| GNSS 定位 | 已定位但时间门槛待复测 | 最近读取 `fix=1`，定位有效 |
| PPS 秒脉冲 | 不稳定 | 最近读取 `pps=0`，间隔 865267 µs；也观察到 `pps=1` 且间隔约 999982 µs |
| UTC/PPS 时间映射 | 不稳定 | 最近读取 `time=0, healthy=0`，不能启动正式同步采样 |
| 外部 32 kHz CLKIN | 不可用 | 启动探测和运行时看门狗均会回退到传感器内部时钟 |
| 内部时钟采样门槛 | 已修复 | `clock_ok=1, clock_source=sensor_internal` |
| LoRa 射频初始化 | 已通过 | `READY,sensor=1,radio=1` |
| LoRa 实际收包与 ACK | 待实测 | 尚无 E870 在线收包证据 |
| 8 节点扩容 | 待实测 | 单节点正式空口验收尚未完成 |

## 2. 当前设备状态

以下为 2026-11-16 冷启动后 COM8 实读状态：

```text
STATUS,id=1003,state=1,gps=1,clock_hz=0.0000,ppm=0.00,
clock_ok=1,clock_source=sensor_internal,queue=0,
lost_samples=0,lost_packets=0,startup_attempts=1,
startup_failure=none,fault_reason=none
```

同一时段的 GNSS 详细状态为：

```text
GPS_DIAG,fix=1,pps=0,time=0,healthy=0,pps_count=43,
pps_interval_us=865267,pps_bad=12
```

解释：

- `state=1`：Waiting。节点已就绪，但当前没有运行采样。
- `gps=1`：该次 `STATUS` 输出时，GNSS/PPS 时间基准判定健康。
- `gps=0`：该次 `STATUS` 输出时，GNSS/PPS 时间基准不合格。
- `gps` 会随实时健康判定变化。正式 START 前必须用 `GPS_DIAG` 确认 `healthy=1`，不能依赖上一次 `STATUS`。
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

供电与接地沿用现有接线。PPS 指示灯此前观察为闪烁，健康判定必须以 `GPS_DIAG` 为准。不要自行改 GPIO、电压或接反 GNSS RX/TX。

## 4. 固件行为

- 上电先尝试 GPIO36 外部 32 kHz，并用 500 ms 窗口确认频率。
- 外部时钟不可用时关闭 CLKIN，重新用 `external_clock=0` 初始化 IIM42652。
- 启动自检最多执行 3 次完整初始化，每次失败后等待 300 ms。
- 外部时钟在空闲运行时连续失败，会关闭 CLKIN 并自动回退内部时钟。
- 采样进行中或队列非空时不会无提示切换时基；这种情况仍进入 Fault。
- 内部时钟模式不要求 GPIO36 边沿，也不要求外部 60 秒频率测量。
- 内部时钟模式仍要求传感器启动自检、有效 FIFO 样本、有效 XYZ 和 WHO_AM_I。
- 采样间隔使用 FIFO 内建时间戳。
- FIFO 每 5 ms 轮询一次，批量 SPI 读取。
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

### 5.4 启动自动恢复回归（2026-11-16）

连续 4 次冷启动全部成功：

```text
READY,sensor=1,radio=1,clock_output=0,
clock_source=sensor_internal,clock_measurement=sensor_internal_fifo
SENSOR_STARTUP_END,trigger=boot,ok=1,attempt=1
STATUS,state=1,clock_ok=1,startup_attempts=1,
startup_failure=none,fault_reason=none
```

台架固件再次完成 50 Hz、60 秒采样：

```text
samples=2973
dt_mean_us=20000
dt_min_us=19985
dt_max_us=20002
invalid=0
lost_samples=0
ts_anomaly=0
fifo_errors=0
state_after=1
fault_reason=none
pass=1
```

第 4 次冷启动于 18:08 完成，仍为 `attempt=1`，之后传感器 `DIAG` 返回 `who=0x6F`。

日志：

```text
tmp/logs/com8_boot_20261116_180839.log
tmp/logs/startup_retry_coldboot_1_20261116.log
tmp/logs/startup_retry_coldboot_2_20261116.log
tmp/logs/startup_retry_coldboot_3_20261116.log
tmp/logs/startup_retry_bench_50hz_60s_20261116.log
tmp/logs/startup_retry_tx_final_20261116.log
```

正式 tx 固件已恢复到 COM8，当前状态：

```text
STATUS,id=1003,state=1,gps=0,clock_ok=1,
clock_source=sensor_internal,queue=0,
lost_samples=0,lost_packets=0,startup_attempts=1,
startup_failure=none,fault_reason=none
```

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
- `SENSOR_SAMPLE_TEST`：内部时钟直接采样诊断，会改变现场状态；内部时钟模式也可执行。
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
2. 运行 `git status --short --branch`、`git log -5 --oneline`，确认基线 `e44bb6b`。
3. 读取[本轮报告](changes/2026-11-16_06_Git日期化提交与分支基线.md)。
4. 识别当前 COM 口和设备，不要默认仍为 COM8。
5. 读取 STATUS，确认 `state=1,clock_ok=1,startup_failure=none,fault_reason=none`；再单独读取 GPS_DIAG 检查 GNSS/PPS。
6. 需要验证采样时，先跑 USB 60 秒，再烧录正式 tx。
7. 需要验证 LoRa 时，先启动 E870 和上位机，再下发正式 START。
8. 不把 `clock_hz=0` 或低值当作内部时钟故障；内部模式只看传感器自检、FIFO 和错误计数。

## 10. Git 与本地资料

- 当前分支：`codex/20261116-baseline`
- 当前功能固件提交：`cb99db0`
- 关键前序提交：`96cdbe3`、`f9bda89`、`cb099e7`、`4cadadd`
- 日期化提交：从 2026-11-16 起，提交信息使用 `类型(YYYY-MM-DD): 内容`；修改报告文件以日期开头；长期文档改动更新“更新日期”。
- 当前报告：[Git 日期化提交与分支基线](changes/2026-11-16_06_Git日期化提交与分支基线.md)
- 长期接线文档：[wiring_and_flash.md](wiring_and_flash.md)
- 协议文档：[protocol_v3.md](protocol_v3.md)
- 网关操作：[e870_setup.md](e870_setup.md)

采集数据、日志、构建产物和镜像不进入 Git。以下本机日志可直接用于新对话时核对：

```text
tmp/logs/startup_retry_coldboot_1_20261116.log
tmp/logs/startup_retry_coldboot_2_20261116.log
tmp/logs/startup_retry_coldboot_3_20261116.log
tmp/logs/startup_retry_bench_50hz_60s_20261116.log
tmp/logs/startup_retry_tx_final_20261116.log
tmp/logs/run7010_internal_clock_60s_20261116.log
tmp/logs/tx7011_internal_clock_short_20261116.log
```

这些文件不提交 Git，新电脑或重新克隆仓库后可能不存在。
