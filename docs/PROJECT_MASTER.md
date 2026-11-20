# E870 八节点加速度采集项目总览

> 本文件是当前项目唯一的技术总文档。旧交接、协议、接线、网关、验收和诊断文档已合并到这里，原文可从 Git 历史查询。

- 更新日期：2026-11-20（Asia/Hong_Kong）
- 当前分支：`codex/20261117-sensor-reset-recovery`
- 本文合并前的 Git 基线：`1d166bb fix(2026-11-17): 修正E870 ACK发射时隙并完成LoRa空口验收`
- 已验证采样标签：`verified/2026-11-16-tx-50hz`
- 标签对应固件源码：`cb99db0`
- 当前固件增量：`da39b79` 增加 IIM42652 软复位轮询和外部时钟超差回退；另有 IIM42352 独立驱动修正：16 字节带时间戳包、CLKSEL 和时间戳比例；COM6 身份识别仍失败
- 当前上位机增量：`1d166bb` 增加 E870 ACK 发射提前量
- 文档范围：项目功能、器件功能、接线、协议、网关、构建烧录、实测结果、限制和恢复方法

## 1. 项目目标和当前结论

项目目标是构建一套 8 节点三轴加速度同步采集系统。每个 ESP32 节点使用 IIM-42352 或 IIM-42652 采集加速度，使用 GNSS UTC 和 PPS 建立时间基准，通过 LoRa 将数据发到一台 E870 网关，再由电脑保存为 CSV 并生成质量报告。

当前已经完成单节点端到端验证。节点 `1003` 使用 EoRa-S3-400TB 和 YBX-IIM42652，完成了启动自检、50 Hz 采样、GNSS/PPS 健康检查、470.7 MHz LoRa 上行、E870 ACK 和补传恢复。2026-11-20 使用旧共享驱动验证 Heltec V3.2 LF + IIM-42352 节点 `1002`：外部 32 kHz CLKIN、身份 `WHO_AM_I=0x6D`、启动自检和 20 条 50 Hz 加速度样本均已通过。当前已改为独立 IIM-42352 驱动，并在 2026-11-20 修正 FIFO 格式、时钟选择和时间戳换算。修正版正式固件已烧录 COM6；三次复位及独立最小通信固件均读到 `WHO_AM_I=0x00`，无法进入采样。本轮最后一次 GNSS 检查为 `fix=0,pps=0,healthy=0`，此前的 GNSS 成功结果仅代表历史测试。LoRa 和 ACK 尚未在该组合验收。8 节点并行、两小时稳定性和 100 Hz 尚未验收。

| 功能 | 当前状态 | 当前结论 |
|---|---|---|
| IIM42652 身份 | 已通过 | `WHO_AM_I=0x6F` |
| IIM42652 启动自检 | 已通过 | 软复位等待 `RESET_DONE` 后 `vendor_init rc=0` |
| 启动自动恢复 | 已通过 | 最多 3 次完整初始化，每次失败等待 300 ms |
| IIM42352 旧驱动身份 | 历史通过 | Heltec 节点 1002 实测 `WHO_AM_I=0x6D` |
| IIM42352 旧驱动启动自检 | 历史通过 | 连续 3 次复位均在首次尝试通过，外部 CLKIN 边沿 3871/120 ms |
| IIM42352 旧驱动 50 Hz 采样 | 历史通过 | `SENSOR_SAMPLE_TEST` 20/20 条有效样本，间隔约 20 ms |
| IIM42352 独立驱动 | 实机未通过 | 已修正 FIFO、CLKSEL 和时间戳；正式固件及最小通信测试仍返回 `0x00`，采样 0/20，见 12.4 |
| 外部 32 kHz CLKIN | 不可用 | 实测约 `32257.4 Hz`、`+8045 ppm`，超过 ±100 ppm 门槛 |
| 传感器内部时钟采样 | 已通过 | `clock_ok=1,clock_source=sensor_internal` |
| 50 Hz 单节点采样 | 已通过 | 60 秒有效样本 2973，FIFO 错误、非法样本和时间戳异常均为 0 |
| GNSS 定位与 PPS | 已通过 | `fix=1,pps=1,time=1,healthy=1` |
| 单节点 LoRa 上行 | 已通过 | run 7031 收到 60/60 包，样本 1489 |
| E870 ACK 与节点补传 | 已通过 | run 7030 收到 119/120 包，队列最终为 0 |
| 8 节点 50 Hz | 待实测 | 尚未执行 1→2→4→8 扩容 |
| 8 节点两小时稳定性 | 待实测 | 尚未执行 |
| 100 Hz/SF5 | 待开始 | 50 Hz 最终验收通过前禁止启用 |

## 2. 系统架构

```text
IIM-42352 / IIM-42652
          │ SPI
          ▼
ESP32-S3 节点 1001–1008
  ├─ GNSS UART：UTC
  ├─ GNSS PPS：秒边界校准
  ├─ 本地 5 秒通信周期
  └─ LoRa：470.3–471.7 MHz
          │
          ▼
E870-L470LG12-O 网关
  ├─ 8 个 125 kHz 上行信道
  └─ Semtech UDP v2，端口 1700
          │ Ethernet
          ▼
电脑上位机
  ├─ 网关配置与诊断
  ├─ 启停、状态和 ACK 调度
  ├─ CSV 保存
  └─ 到达率、补传率和延迟报告
```

系统分工如下：

- ESP32 节点负责采样、FIFO 时间戳、UTC/PPS 映射、状态机、LoRa 收发和本地队列。
- E870 负责无线接收、UDP 转发和下行发射，不负责采样流程。
- 电脑上位机负责节点确认、统一启动、ACK、补传协调、数据保存和质量统计。
- `PUSH_ACK` 和 `TX_ACK` 只证明网关网络或发射接口接受了报文，不证明节点已经收到命令。
- 只有节点返回 `CONFIRM` 或 `STATUS`，才能判断控制命令到达节点。

## 3. 硬件组合

根目录 `platformio.ini` 是唯一固件构建配置。

| 组合 | 开发板 | 传感器 | 正式环境 | 默认节点 | 实机状态 |
|---|---|---|---:|---:|---|
| A | Heltec V3.2 LF | IIM-42352 | `heltec_v32_iim42352_tx` | 1002 | 2026-11-20 曾通过启动自检和 50 Hz 采样；接入 GNSS 后传感器总线读数变为 `0x00`，待恢复；GNSS 已在 115200 收到 NMEA |
| B | Heltec V3.2 LF | IIM-42652 | `heltec_v32_iim42652_tx` | 1004 | 曾烧录 1004，自检失败；当前不作已通过 |
| C | EoRa-S3-400TB | IIM-42352 | `eora_s3_400_iim42352_tx` | 1001 | 未完成该组合实机验收 |
| D | EoRa-S3-400TB | IIM-42652 | `eora_s3_400_iim42652_tx` | 1003 | 当前已通过单节点采样和 LoRa 验收 |

IIM-42352 的预期身份为 `0x6D`。IIM-42652 的预期身份为 `0x6F`。当前正式业务只启用加速度，陀螺仪关闭。

### 3.1 当前已验证节点

- 开发板：EoRa-S3-400TB，ESP32-S3。
- 传感器模块丝印：`YBX-IIM42652`。
- 节点号：`1003`。
- 上行频率：`470.7 MHz`。
- 固件版本字符串：`3.0.1`。
- 正式环境：`eora_s3_400_iim42652_tx`。
- 设备 MAC：`90:da:72:76:1f:30`。
- 交接时串口：COM8，`460800` 波特率。
- 设备端口可能变化，烧录前必须重新枚举，不能默认仍是 COM8。

YBX 模块的额定输入电压、内部 VDDIO 和板级电平电路仍缺少厂家完整资料。当前按已确认的 3.3 V 供电使用。不得因为识别异常就改接 5 V。

### 3.2 EoRa-S3-400TB 固定接线

| 信号 | EoRa GPIO | 说明 |
|---|---:|---|
| IIM SCK | 47 | SPI 时钟 |
| IIM MOSI | 48 | 主控发往传感器 |
| IIM MISO | 39 | 传感器发回主控 |
| IIM CSB | 40 | SPI 片选 |
| IIM INT1 | 38 | FIFO/DRDY 中断输入 |
| IIM CLKIN/INT2 | 36 | 外部 32 kHz 候选时钟输出 |
| GNSS TX → 主控 RX | 16 | GNSS UART |
| GNSS RX ← 主控 TX | 15 | GNSS UART |
| GNSS PPS → 主控 | 41 | 秒脉冲输入 |
| 电源和地 | 3.3 V/GND | 所有模块共地 |

EoRa 板载 LoRa 引脚如下：

| 信号 | GPIO |
|---|---:|
| SCK / MOSI / MISO | 5 / 6 / 3 |
| CS / RST | 7 / 8 |
| BUSY / DIO1 | 34 / 33 |
| E22 电源控制 | 21 |

EoRa OLED 使用 GPIO17/18，当前不占用。固件使用串口输出状态，尚未实现 OLED 界面。

### 3.3 Heltec V3.2 固定接线

| 信号 | Heltec GPIO |
|---|---:|
| IIM SCK / MOSI / MISO / CS | 47 / 48 / 33 / 34 |
| IIM INT1 / CLKIN | 26 / 3 |
| GNSS TX → 主控 RX | 5 |
| GNSS RX ← 主控 TX | 6 |
| GNSS PPS | 4 |

Heltec 板载 CP2102 使用 UART0。EoRa 使用原生 USB CDC。两者串口配置不能互用。

2026-11-20 在 COM6 完成 Heltec V3.2 LF + IIM-42352 测试：

- 固件环境：`heltec_v32_iim42352_usb_test`，版本字符串 `3.0.1`。
- 身份读数：`SENSOR_ID,name=IIM-42352,expected=0x6D,actual=0x6D,ok=1`。
- 启动自检：连续 3 次复位均在首次尝试通过，`fifo_samples=33`、`clock_edges=3871`。
- 时钟：`clock_source=external`、`clock_output=1`。
- 状态：`STATUS,id=1002,state=1,...,fault_reason=none`。
- 采样：`SENSOR_SAMPLE_TEST_END,configured=1,samples=20,target=20,ok=1`，样本间隔约 20 ms。
- 限制：该次测试没有接入 GNSS，`gps=0`；LoRa 和 ACK 未验收。

2026-11-20 接入 GNSS 后的补充结果：

- GNSS 串口确认工作：`GPS_SCAN` 在 `115200` 收到 716 字节/2 秒、24 个 `$`、2 条 RMC，示例为 `$GNGGA,...`。
- GNSS 定位尚未建立：连续观察约 3.5 分钟后仍为 `fix=0,pps=0`，GGA 卫星数为 0，因此没有 PPS。
- 传感器当前无响应：`SENSOR_PROBE` 全部返回 `identity=0x00`，`SENSOR_I2C_SCAN,found=0`。
- 该现象发生在接入 GNSS 之后，接入前同一块板已通过身份 `0x6D`、启动自检和 20 条样本测试。
- 2026-11-20 烧录独立 IIM42352 驱动后复测，结果仍为全部 `identity=0x00`；软件 SPI 和 I2C 扫描也无响应，说明故障出现在传感器身份识别阶段。
- 当前判断：身份读数为 `0x00` 尚不能确定根因。本轮已确认并修复驱动缺陷；最小固件复测仍失败，详见 12.4。用户已确认接线正确，本轮保持接线与引脚不变。

### 3.4 GNSS 波特率诊断

固件提供 `GPS_SCAN` 命令，用于在 115200、9600、38400、57600、19200、4800 六个波特率下各接收 2 秒，输出字节数、`$` 数量和 RMC 数量。该命令只用于诊断，会暂停 GNSS 解析约 12 秒。

## 4. IIM42652 / IIM42352 功能与采样实现

### 4.1 当前采样配置

| 参数 | 当前值 |
|---|---|
| 采样率 | 50 Hz |
| 量程 | ±4 g |
| 灵敏度 | 8192 LSB/g |
| 陀螺仪 | 关闭 |
| FIFO | 启用 |
| FIFO 轮询周期 | 5 ms |
| 时间戳 | 优先使用传感器 FIFO 内建时间戳；不可用时按标称采样间隔回退 |
| 本地队列容量 | 128 包 |
| 每个 DATA 包 | 最多 25 组 XYZ |
| 正常 DATA 包长度 | 180 字节 |

原始加速度值按以下公式换算：

```text
加速度_g = 原始值 × 4 / 32768
```

启动和采样代码位于：

- `firmware/common/sensor/`
- `firmware/sensors/iim42652/`
- `firmware/sensors/iim42652_vendor/`
- `firmware/sensors/iim42352/`

IIM42652 正式驱动使用原厂 Ixm42xxx 1.1.1 高层 API。旧手写初始化不再作为正式路径。IIM42352 使用独立正式驱动，不继承旧 `IimBase`，也不复用 IIM42652 驱动文件。两套驱动向 `NodeApp` 提供相同接口。

IIM42352 独立驱动的行为如下：

- 上电后自动探测 SPI 模式 0/3 和 1 MHz/100 kHz 时钟。
- 连续读取身份 `0x6D`，确认通信稳定后再执行软复位。
- 软复位后等待 `RESET_DONE`，再配置接口、时钟、加速度计、FIFO 和中断。
- 外部 CLKIN 可用时使用 RTC 模式；外部时钟不可用时使用传感器内部时钟。
- FIFO 使用记录计数和 16 位加速度数据。使用 Packet 2 时为 16 字节，时间戳在字节 14–15；其中字节 7–12 为保留字节。无时间戳时使用 8 字节 Packet 1。
- `CLKSEL=00`，使用内部时钟时 `RTC_MODE=0`，使用外部 CLKIN 时 `RTC_MODE=1`；不再写入保留的 `CLKSEL=01`。
- 时间戳按 TDK DS-000436 第 12.7 节换算：内部时钟按 `32/30`、外部 CLKIN 按 `32.768/30` 修正标称间隔。当前 `TMST_RES=1`，标称单位为 16 µs；实际换算仍待实机 FIFO 验证。
- 每次轮询批量读取 FIFO，逐包检查包头、长度和无效样本码。
- FIFO 连续故障时重建配置；达到阈值后报告致命故障。
- 提供 `SENSOR_DIAG` 所需的身份、配置、FIFO、包头、时间戳和故障计数接口。

本轮未修改 IIM42652 驱动。4 个固件环境已编译通过；Python 回归 4/4 通过，其中 IIM42352 原生测试 5/5 通过。COM6 实机烧录后，传感器没有返回身份 `0x6D`，因此新驱动仍未通过硬件验收。

### 4.2 FIFO 和数据完整性

节点每 5 ms 查询 FIFO，批量读取可用记录。每条记录读取加速度、包头和 FIFO 时间戳。样本间隔优先使用 FIFO 时间戳计算，不使用不稳定的 INT1 边沿计时。IIM42352 的独立驱动按记录数读取 FIFO；时间戳配置成功时选择 `FIFO_PACKET_EN=11`，每包 16 字节，不支持时间戳时自动改为无时间戳的 8 字节包，并由节点使用标称间隔回退。

驱动提供以下保护：

- FIFO 计数读取重试。
- FIFO 包头和记录长度检查。
- 无效样本码检查。
- FIFO 连续故障计数。
- FIFO 状态恢复。
- 时间戳合理性检查；异常时使用标称采样间隔回退。
- 队列溢出计数。
- 采样缺口标志。

软件启动自检要求：

- 身份寄存器正确。
- FIFO 计数有效。
- 启动窗口内 FIFO 样本数量在 4–64 之间。启动窗口包含切换低噪声模式时的短暂积压，因此上限不能按稳态单次读取量设置。
- 至少读取到一组有效 XYZ。
- DRDY 数量不得形成异常中断风暴。
- 外部时钟模式还要求板端 32 kHz 回波计数合理。

### 4.3 启动自动恢复

正式固件启动时执行以下流程：

1. 尝试建立外部 32 kHz CLKIN。
2. 用 500 ms 窗口检查外部时钟是否存在。
3. 只有实测外部时钟边沿超出允许范围时，才关闭 CLKIN 并改用传感器内部时钟。启动自检因其他原因失败时，不得关闭已经正常工作的外部时钟。
4. 最多执行 3 次完整传感器启动自检。
5. 每次失败后等待 300 ms。
6. 任意一次成功，节点进入 Waiting。
7. 三次全部失败，节点进入 Fault，并记录失败阶段和原因。

IIM42652 软复位后必须轮询 `RESET_DONE`，最长约 51 ms。不能只等待 1 ms 就继续初始化，否则会出现 `vendor_init rc=-12`。

`STATUS` 会输出：

- `startup_attempts`
- `startup_failure`
- `fault_reason`
- `clock_source`
- `clock_ok`

### 4.4 时钟策略

外部时钟模式：

- ESP32 在 CLKIN 引脚输出 32.000 kHz、50% 占空比。
- 节点使用 GNSS PPS 测量 60 秒窗口内的实际频率。
- 频率偏差必须在 ±100 ppm 内。
- 外部时钟输出异常时，空闲节点可以重建 MCPWM 输出。
- 外部时钟持续失效时，空闲节点自动回退内部时钟。

内部时钟模式：

- 不要求 GPIO36 有边沿。
- 不要求 60 秒外部频率测量。
- 仍要求传感器启动自检、FIFO、有效 XYZ 和身份检查通过。
- `clock_hz=0.0000` 和 `ppm=-1000000.00` 是外部时钟已经停用后的无效展示值，不能按故障处理。

当前实测外部时钟约 32257.4 Hz、+8045 ppm，因此正式运行使用 IIM42652 内部时钟。采样中或队列非空时不会无提示切换时基；这类情况进入 Fault。

## 5. GNSS/PPS 时间基准

GNSS UART 固定 `115200`，当前只解析带有效校验的 RMC 语句。PPS 使用上升沿输入。

时间基准健康条件如下：

- RMC 定位状态为有效。
- GNSS UTC 时间有效，且最近 NMEA 时间不超过 2 秒。
- PPS 数量至少 2。
- 最近 PPS 间隔在 0.9–1.1 秒之间。
- UTC/PPS 映射年龄不超过 3 秒。

节点把目标 UTC 微秒换算成本地 `esp_timer` 时间，再用 PPS 更新映射。正式 START 前必须重新读取 `GPS_DIAG`，确认 `healthy=1`。

已知边界：

- GNSS/PPS 状态可能短时波动。
- `STATUS.gps` 是输出当时的状态，不保证下一次 START 时仍健康。
- `pps_bad` 是累计异常次数，不直接表示当前失败。
- PPS 指示灯不能代替 `GPS_DIAG`。
- 室内或无天线的测试不得绕过正式时间门槛。

## 6. 节点固件状态机

| 状态 | 数值 | 含义 |
|---|---:|---|
| Boot | 0 | 启动和初始化 |
| Waiting | 1 | 待机，等待 START 或诊断命令 |
| Armed | 2 | 已接受 START，等待预约 UTC |
| Capturing | 3 | 正在采样 |
| Draining | 4 | 已停止采样，等待尾包和 ACK 清空 |
| Fault | 5 | 硬件或时钟故障 |

正常流程：

1. 上电后完成传感器、GNSS、LoRa 和时钟初始化。
2. 节点进入 Waiting，不自动采样。
3. 上位机发送未来 UTC 的 START。
4. 节点返回 CONFIRM，在预约时间进入 Capturing。
5. 节点按 5 秒周期发送 DATA。
6. 固定时长到达或收到 STOP 后停止采样。
7. 节点进入 Draining，继续发送尾包和补传。
8. 全部队列被网关确认后回到 Waiting。

### 6.1 串口命令

串口固定 `460800` 波特率。

| 命令 | 功能 | 注意 |
|---|---|---|
| `STATUS` | 请求并输出状态 | 每 5 秒也会自动输出 STATUS |
| `GPS_DIAG` | 输出 GNSS/PPS 详细状态 | START 前必须检查 |
| `GPS_SCAN` | 扫描 GNSS 常见波特率并统计收到的字节和 RMC 语句 | 只读诊断；会暂停 GNSS 解析约 12 秒 |
| `DIAG` | 输出传感器寄存器诊断 | 只读 |
| `SENSOR_RETRY` | 重新执行完整传感器启动 | 会操作寄存器；只在空闲时执行 |
| `SENSOR_LINK_TEST` | 软件 SPI 只读链路对照 | 会改变现场状态；只在空闲时执行 |
| `SENSOR_SAMPLE_TEST` | 内部时钟直接采样诊断 | 会改变现场状态；不是正式同步采集 |
| `START_AT <epoch_us> 50 4 <run> <duration>` | 本地启动指定运行 | 正式测试优先使用上位机 |
| `STOP` | 停止当前采样 | 进入 Draining，不等于队列已清空 |

`START_AT` 的 rate 只允许 50 或 100，range 支持 2、4、8、16 g。duration 为 0 时表示手动停止。正式生产流程必须优先使用上位机，以获得 CONFIRM、ACK 和质量报告。

## 7. LoRa 协议 v3

协议采用小端字节序、CRC16-CCITT-FALSE（poly=`0x1021`、init=`0xFFFF`）和固定魔数 `0xA55A`。版本号为 3，最大报文 255 字节。

### 7.1 包结构

| 偏移 | 字段 | 类型 | 说明 |
|---:|---|---|---|
| 0 | magic | uint16 | 固定 `0xA55A` |
| 2 | version | uint8 | 固定 3 |
| 3 | type | uint8 | 报文类型 |
| 4 | node | uint16 | 节点号；广播为 `0xFFFF` |
| 6 | flags | uint16 | 健康、缺口、溢出和尾包标志 |
| 8 | run | uint32 | 非零运行编号 |
| 12 | sequence | uint32 | DATA 包序号 |
| 16 | epoch_us | uint64 | DATA 首样本 UTC 微秒 |
| 24 | rate_hz | uint16 | 50 或 100 |
| 26 | sample_count | uint8 | DATA 样本数，1–25 |
| 27 | range_g | uint8 | 2、4、8 或 16 |
| 28 | payload | 可变 | 类型相关载荷 |
| 末 2 字节 | CRC | uint16 | CRC 低字节在前 |

### 7.2 报文类型

| 类型 | 数值 | 功能 |
|---|---:|---|
| DATA | 1 | 加速度数据和运行状态 |
| START | 2 | 预约启动 |
| STOP | 3 | 停止当前运行 |
| CONFIRM | 4 | 节点确认启动、停止或诊断请求 |
| STATUS | 5 | 节点状态上报 |
| DISCOVER | 6 | 查询节点 |
| ACK | 7 | 网关确认 DATA 序号 |

### 7.3 状态标志

| 位 | 名称 | 含义 |
|---:|---|---|
| bit0 | GNSS 健康 | UTC/PPS 当前可用 |
| bit1 | 时钟健康 | 采样时钟门槛通过 |
| bit2 | 采样缺口 | 检测到样本缺口 |
| bit3 | 队列溢出 | 本地发送队列曾溢出 |
| bit4 | 尾包 | 运行结束后的最后 DATA |

### 7.4 DATA 和 ACK

- DATA 最多携带 25 组 `int16 XYZ`，正常长度 180 字节。
- 首样本时间由包头 `epoch_us` 提供。
- 后续样本时间按 `首样本 UTC + i × 1000000 / rate` 还原。
- ACK 条目为 `node, firstSequence, bitmap`，每项 10 字节。
- 一个 ACK 包最多携带 8 个节点条目。
- 每个节点使用 32 位 bitmap 确认序号。
- 网关只确认已经写入 CSV 的序号，不能确认未收到的包。
- 重复 DATA 也再次确认，但不能重复写入 CSV。

### 7.5 无线参数

| 参数 | 当前值 |
|---|---|
| 上行频率 | 1001 至 1008 对应 470.3 至 471.7 MHz，间隔 0.2 MHz |
| 控制下行 | 470.3 MHz |
| 50 Hz 调制 | SF6、125 kHz、编码率 4/5 |
| 100 Hz 调制 | SF5、125 kHz，尚未解锁 |
| 同步字 | `0x12` |
| 前导码 | 12 |
| 发射功率 | 14 dBm |
| 网关链路 | Semtech UDP v2，端口 1700 |

待机和控制窗口统一监听 470.3 MHz。节点发送数据或回复 CONFIRM/STATUS 时切回自己的上行频率。

## 8. 五秒通信周期与 ACK 时隙

| 相位 | 节点行为 |
|---|---|
| 0–4.000 s | 在自身上行信道发送 DATA |
| 4.000–4.250 s | 在 470.3 MHz 监听 ACK、START、STOP 和状态请求 |
| 4.250–4.450 s | 回到上行信道发送 CONFIRM 或 STATUS |
| 4.450–5.000 s | 补传未确认 DATA |

E870 的 `TX_ACK` 通常在下发后 0–31 ms 返回，但实际空口发射约晚 400 ms。当前上位机使用以下补偿：

- `ACK_TX_LEAD_US=400000`
- ACK 请求窗口：每个周期的 3.68–3.82 s
- 预计空口到达：4.08–4.22 s
- 节点监听窗口：4.000–4.250 s

实测可用下发带约 3.60–3.86 s。更换网关型号或网关固件后必须重新标定发射延迟，不能直接沿用 400 ms。

START 和 STOP 也遵守控制窗口。主机必须收齐全部目标节点的成功 CONFIRM 才允许正式启动。启动前缺少确认时，上位机会取消运行并发送 STOP。

## 9. E870-L470LG12-O 网关

### 9.1 设备能力

- 厂家标称工作频率：470–510 MHz。
- 设备为半双工。
- 设备使用 SX1302/SX1250 平台。
- 原厂 CN470 配置提供 8 个上行信道。
- 网关 EUI：`0000000000000923`。
- 当前网络：网关 `192.168.1.10/24`，电脑 `192.168.1.22`。
- UDP 上下行端口均为 1700。
- USB2 管理串口波特率：`115200`。
- 网线管理支持 SSH，首次连接必须核对主机指纹。

厂家标称 470–510 MHz 不等于每个频点、功率和滤波通路已经实机合格。项目已经在 470.7 MHz 上行和 470.3 MHz ACK 下完成单节点实测。

### 9.2 配置管理

网关配置由两个层次组成：

- 原厂配置和校准：必须完整备份，不得覆盖。
- 项目配置：写入独立的 `desktop_*.json` 或项目配置文件。

配置必须保留：

- SX1250 射频类型。
- 原厂校准数据。
- 原厂功率表。
- 合法 TX 范围。
- 原厂备份和 SHA256 清单。

完整备份和当前运行配置位于 `gateway/state/`。该目录被 Git 忽略，不进入提交。

### 9.3 当前网关操作

1. 网关接好 LoRa 天线和 8–28 V 电源。
2. 首次使用 USB2 串口连接，确认设备身份和当前配置。
3. 备份原厂配置。
4. 预览网口、无线和转发差异；不要直接修改原厂 JSON 数字。
5. 应用网口设置后重新读取配置，再应用无线和转发设置。
6. 重启 `gw` 服务并确认服务状态。
7. 检查 UDP `PUSH_DATA`、`PULL_DATA`、`TX_ACK` 和网关日志。

恢复时先停止 `gw`，恢复原厂软链接和配置，再启动服务。`gw stop` 只停止网关服务，不停止节点采样。

## 10. 电脑端软件

根目录双击 `启动E870上位机.cmd` 可打开三页桌面工作台：

- 网关配置
- 采集控制
- 诊断与测试

桌面软件使用项目 `.venv`。迁移到新电脑时，必须建立 Python 3.12 环境并安装 `requirements-desktop.txt`，不能只复制启动脚本。

### 10.1 采集流程

1. 启动接收服务，确认网关 PULL 心跳正常。
2. 选择目标节点 1001–1008。
3. 查询节点状态。
4. 检查全部目标节点处于 Waiting。
5. 检查 GNSS/PPS、传感器和时钟健康。
6. 下发未来 UTC 开始时间。
7. 收齐全部目标节点 CONFIRM。
8. 采集期间监控到达率、延迟、队列和错误计数。
9. 停止后等待全部节点回到 Waiting 且 `queue=0`。
10. 核对 CSV 和质量报告。

### 10.2 数据输出

```text
data/e870/run_<run>/sensor<node>/acceleration.csv
data/e870/run_<run>/sensor<node>/packets.csv
data/e870/capture_report.json
```

质量报告包含：

- 包到达率
- 样本数
- 重传率
- 重复包
- 最终缺失率
- 最大延迟
- 10 秒内到达比例
- 网关无效包
- 节点最终状态

### 10.3 命令行运行

```powershell
.\.venv\Scripts\python.exe -m host --gateway-config gateway/state/active.local.json
```

纯接收模式：

```powershell
.\.venv\Scripts\python.exe -m host --headless --gateway-config gateway/state/active.local.json
```

默认目标为 1001–1008。可用 `--nodes 1003` 缩小测试范围。

## 11. 构建、烧录和软件测试

### 11.1 固件环境

| 用途 | 环境 |
|---|---|
| 当前正式节点 | `eora_s3_400_iim42652_tx` |
| 当前 USB 测试 | `eora_s3_400_iim42652_usb_test` |
| 当前无 GNSS 台架 | `eora_s3_400_iim42652_bench` |
| IIM42652 原厂驱动最小启动测试 | `eora_s3_400_iim42652_bringup` |
| 其余正式组合 | `heltec_v32_iim42352_tx`、`heltec_v32_iim42652_tx`、`eora_s3_400_iim42352_tx` |
| 其余 USB 组合 | `heltec_v32_iim42352_usb_test`、`heltec_v32_iim42652_usb_test`、`eora_s3_400_iim42352_usb_test` |

固定依赖：

- PlatformIO Espressif32 `6.12.0`
- Arduino framework
- RadioLib `7.6.0`
- EoRa 构建配置使用 4 MB Flash 和 2 MB QSPI PSRAM
- 串口监视器 `460800`

### 11.2 编译和烧录

```powershell
$env:PLATFORMIO_DISABLE_TELEMETRY='1'
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx -t upload --upload-port COM8
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" device monitor -p COM8 -b 460800
```

烧录前必须用 `pio device list` 或设备管理器确认实际串口，不能只凭旧记录使用 COM8。

### 11.3 节点号部署

同一硬件组合可以部署为任意节点 1001–1008。烧录前使用环境变量指定唯一节点号：

```powershell
$env:APP_NODE_ID='1005'
pio run -e eora_s3_400_iim42652_tx -t upload --upload-port COM5
Remove-Item Env:APP_NODE_ID
```

节点号超出 1001–1008 时构建直接失败。频率由节点号自动计算，不需要改源码。

### 11.4 软件测试

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

当前主要测试集：

- `tests.test_firmware_sensor`
- `tests.test_e870_8_node`
- `tests.test_desktop_control`
- `tests.test_e870_v3`
- `tests.test_gateway_diagnostics`
- `tests.test_gateway_management`

2026-11-20 IIM42352 独立驱动回归结果：

- `tests.test_firmware_sensor`：4/4 通过。
- 原生测试覆盖 10 字节定时 FIFO 包、8 字节无时间戳回退包、时间戳换算、非法配置和 FIFO 计数超限。
- `heltec_v32_iim42352_usb_test`、`heltec_v32_iim42352_tx`、`heltec_v32_iim42652_tx`、`eora_s3_400_iim42652_tx` 全部编译成功。
- 2026-11-20 COM6 实机：GNSS `fix=1,pps=1,time=1,healthy=1`，PPS 间隔 `1000007 us`，`pps_bad=0`。
- IIM42352：硬 SPI 四种组合、软件 SPI 两种线序和 I2C 扫描均无响应；`SENSOR_SAMPLE_TEST_END,configured=0,samples=0,ok=0`。
- 因此当前结论是传感器模块未在总线上响应，不是 SPI 模式、速率或驱动解析问题。下一步应先实测模块 VCC/GND 电压和 CS 电平。

编译和软件测试通过只证明源码路径可用，不能代替实机烧录、采样和无线电验收。

## 12. 实测结果

### 12.1 已验证 60 秒传感器采样

2026-11-16 的正式 tx 测试使用 run 7010，条件为 50 Hz、±4 g、60 秒。

| 指标 | 结果 |
|---|---:|
| 有效样本 | 2973 |
| 非法样本 | 0 |
| FIFO 错误 | 0 |
| 丢样 | 0 |
| 时间戳异常 | 0 |
| 平均样本间隔 | 19999–20000 µs |
| 最小间隔 | 19985 µs |
| 最大间隔 | 20002 µs |
| GNSS 健康 | 14/14 次状态输出健康 |
| 时钟门槛 | 14/14 次通过 |
| 最终结果 | 通过 |

该测试证明传感器 FIFO、采样节拍、内部时钟采样门槛和 60 秒稳定性满足单节点 50 Hz 要求。

### 12.2 2026-11-17 单节点 LoRa 验收

| 测试 | run 7028 | run 7030 | run 7031 |
|---|---:|---:|---:|
| 时长 | 30 秒 | 60 秒 | 30 秒 |
| 唯一 DATA | 60/60 | 119/120 | 60/60 |
| 包到达率 | 100% | 99.2% | 100% |
| 样本数 | 1487 | 2971 | 1489 |
| 重传包 | 0 | 0 | 0 |
| 重复包 | 0 | 0 | 0 |
| 网关无效包 | 0 | 0 | 0 |
| RSSI | -22 dBm | -22 dBm | -23 dBm |
| SNR | 10.1 dB | 10.3 dB | 10.2 dB |
| 最终状态 | Waiting | Waiting | Waiting |
| 最终队列 | 0 | 0 | 0 |
| 结果 | 通过 | 通过 | 通过 |

run 7031 是本机最新补充验证数据，位于 `tmp/e870_lora_ack_20261117/data/run_7031`。该数据不进入 Git。

run 7031 的其他指标：

- 10 秒内到达率：100%。
- 最大端到端延迟：634.795 ms。
- `lost_samples=0`。
- `lost_packets=0`。
- `fifo_errors=0`。
- `ts_anomaly=0`。
- 样本间隔平均 20.00 ms。
- GPS/PPS 测试前后均为 `healthy=1`。
- 网关 EUI 为 `0000000000000923`。

### 12.3 补传恢复

run 7029 在采集期间停止网关约 45 秒，节点积压 112 包。网关恢复后只确认已经写入 CSV 的序号，队列变化为：

```text
112 -> 88 -> 49 -> 16 -> 1 -> 0
```

最终 119 个唯一包全部收齐，节点 `lost_samples=0`、`lost_packets=0`，证明补传路径可用。

### 12.4 Heltec + IIM-42352 驱动复核与烧录（2026-11-20）

本轮结论：驱动缺陷已修正，编译和烧录成功，但传感器身份读取仍失败，不能宣布采样通过。用户确认现有接线正确，所有测试保持 3.3 节的引脚配置。

代码依据为本机保存的 TDK IIM-42352 数据手册 DS-000436，Revision 1.1：第 6.1/6.2/6.4 节（包结构、包头、使能），第 12.7 节（时间戳比例），第 14.1/14.30 节（复位和时钟选择）。已查看第 36 页原始表格。此前 10 字节包测试使用了错误的模拟数据，不能证明真实 FIFO 格式正确。

修正内容：

- `FIFO_CONFIG1[1:0]` 按 `FIFO_PACKET_EN` 解释。带时间戳选择 Packet 2，配置为 `0x67`，长度 16 字节；无时间戳回退到 Packet 1，配置为 `0x65`，长度 8 字节。bit 3 为 FSYNC 选择，不用于决定是否存在普通 ODR 时间戳。
- Packet 2 跳过 6 个保留字节，并从字节 14–15 读取时间戳；拒绝旧实现假定的 `0x48` 包头和不匹配的包格式。
- `CLKSEL` 改为 `00`，保留 RTC_MODE 对内部/外部时钟的选择。
- 时间戳使用本器件的比例换算；修正诊断中 `resolution_us_x1000` 的单位。
- 软复位必须同时观察到 `RESET_DONE` 和正确身份，避免只有身份正确时误报复位成功。
- `SENSOR_LINK_TEST` 只使用用户确认的线序，不再将 MISO 切成输出进行交换线序测试；增加 MISO 弱上拉/弱下拉读数。

编译与软件结果：

| 检查 | 结果 |
|---|---|
| `heltec_v32_iim42352_tx` | 编译通过 |
| `heltec_v32_iim42352_usb_test` | 编译通过 |
| `heltec_v32_iim42652_tx` | 编译通过 |
| `eora_s3_400_iim42652_tx` | 编译通过 |
| `python -m unittest tests.test_firmware_sensor -v` | 4/4 通过 |
| IIM42352 原生用例 | 5/5 通过：真实 16 字节包、8 字节回退、非法配置/计数、错误包头、复位完成及零身份拒绝；包含时钟与时间戳比例校验 |

实机结果：

| 检查 | 观察 |
|---|---|
| 正式固件烧录 | COM6 写入成功，各区域哈希校验通过 |
| 连续三次主控复位 | 每次启动自检均失败；每次最多 3 次初始化，身份均为 `0x00` |
| 硬件 SPI | 模式 0/3、100 kHz/1 MHz 未读到 `0x6D` |
| 软件 SPI | 正确线序、模式 0/3、半周期 5/20 µs，共 16 次均为 `0x00` |
| MISO 空闲读数 | CS 高时，弱上拉和弱下拉均读为 1；这不是电压测量，不能单独判断模块故障 |
| 采样专项 | `configured=0,samples=0,target=20,ok=0`，未进入采样 |
| 最小独立通信固件 | 不启动 GNSS、LoRa、中断或 CLKIN；模式 0/1/2/3、100 kHz/1 MHz；发送复位命令前及后均为 `0x00`，共 72 次身份读取；复位命令是否被器件接受无法确认 |
| GNSS 本轮状态 | `fix=0,pps=0,time=0,healthy=0` |
| 最终保留固件 | `heltec_v32_iim42352_tx`，节点 1002，460800 波特率 |

最小固件同样失败，说明本次零读数不依赖正式驱动的 FIFO 解析，也不依赖 GNSS/LoRa 任务运行。但这不能证明电源电压正确，也不能直接确定传感器损坏。下一步需在保持接线不变的条件下给开发板与传感器完整断电再上电，然后重试身份识别；如仍失败，再实测模块电源与 CS/SCK/MOSI/MISO 波形。

- 文件变化：`firmware/sensors/iim42352/Driver.cpp`、`Driver.h`、`tests/firmware_iim42352_driver.cpp`、`tests/firmware_support/Arduino.h`、本总文档和 `PROJECT_PLAN.md`。
- 接线变化：无。操作变化：本轮包含一次最小固件隔离测试，结束后恢复正式固件。
- Git 基线：`cc952c8`；本轮提交包含代码、测试与两份主文档，不包含数据或构建产物。
- 本机证据：`tmp/heltec42352_review_20261120/`，保留串口基线、三次启动、软件 SPI、采样、最小测试及最终烧录日志。临时资料未删除。
- 验收限制：50 Hz 连续采样、FIFO 时间戳实测、LoRa 上行和 ACK 均未在本轮通过。

### 12.5 Heltec V3.2 LF + IIM-42652 端到端实机测试（2026-11-20）

测试配置：

| 项目 | 值 |
|---|---|
| 开发板 | Heltec V3.2 LF |
| 传感器 | IIM-42652，`WHO_AM_I=0x6F` |
| 节点号 | 1004 |
| 串口 | COM6，460800 |
| 当前板内固件 | `heltec_v32_iim42652_tx` |
| 上行 | 470.9 MHz、SF6BW125、4/5、同步字 `0x12` |
| 控制下行 | 470.3 MHz |
| 网关 | E870-L470LG12-O，EUI `0000000000000923`，192.168.1.10 |
| 电脑 | 192.168.1.22，UDP 1700 |
| 采样 | 50 Hz、±4 g、加速度 only |
| 运行编号 | run 3042232764 |
| 计划时长 | 60 秒 |

编译、烧录和启动结果：

| 检查 | 结果 |
|---|---|
| `heltec_v32_iim42652_usb_test` | 编译和烧录成功 |
| `heltec_v32_iim42652_tx` | 编译和烧录成功，写入后哈希校验通过 |
| USB 身份识别 | `WHO_AM_I=0x6F` |
| USB 启动自检 | 首次通过，`fifo_samples=17`，`clock_edges=3871` |
| USB 采样专项 | 20/20 条有效样本，间隔 18911–21001 µs，平均 19890.1 µs |
| 正式固件连续三次复位 | 每次首次启动自检均通过，`READY,sensor=1,radio=1` |
| 传感器诊断 | `who=0x6F`，`accel=0x49`，`fifo_cfg=0x80`，`fifo1=0x6C`，`packet_bytes=16` |

端到端结果：

| 检查 | 结果 |
|---|---|
| 网关心跳 | 收到 `PULL_DATA`，peer `192.168.1.10:53660`，EUI 与配置一致 |
| 节点查询 | `DISCOVER` 的 `TX_ACK=NONE`，节点回复 `state=1`、`flags=3`、`queue=0` |
| 启动命令 | 3 次 START 均为 `TX_ACK=NONE`，节点回复 CONFIRM 并进入采集中 |
| 上行包数 | 121 |
| 样本数 | 3016 |
| 到达率 | 100% |
| 最终缺失率 | 0 |
| 重复包 | 0 |
| 重传率 | 0 |
| 10 秒内到达率 | 100% |
| 最大估计延迟 | 653.363 ms |
| RSSI / SNR | -19 至 -21 dBm / 11.2 至 12.2 dB |
| 节点结束状态 | Waiting，`queue=0`，`lost_samples=0`，`lost_packets=0` |
| 样本间隔 | 19982–20000 µs，平均 19999.9 µs（按节点时间戳网格） |

LoRa 和 ACK 结论：

- 上行频率 470.9 MHz 与网关接收配置一致。
- START、DISCOVER 和 ACK 下行均由网关接受，`TX_ACK` 全部为 `NONE`。
- 采集期间节点队列始终为 0，重传率为 0，说明 ACK 能到达节点并清除待确认包。
- 采集结束后节点回到 Waiting，队列为 0，本轮 LoRa 上行和 ACK 验收通过。

时钟和时间轴结果：

- 启动自检在 50 ms 内计到 1613 个 CLKIN 边沿，折算约 32.26 kHz，超出 ±100 ppm 要求。
- 固件按设计回退到传感器内部时钟。串口状态为 `clock_source=sensor_internal`、`clock_ok=1`、`clock_hz=0`。
- `clock_ok=1` 只表示内部时钟回退被接受，不表示速率已经通过精度验收。
- 121 个包的时间戳间隔为 500000 µs，电脑 UTC 实测到达间隔回归值为 497468.7 µs，折合节点时间轴约快 5090 ppm，约 305 ms/分钟。
- 本轮无法区分偏差来自内部时钟速率、CLKIN 实际频率还是时间戳换算。该偏差不影响 LoRa 和 ACK 结论，但会阻止跨节点同步精度验收。

证据和限制：

- 本机证据目录：`tmp/heltec42652_test_20261120/`，包含 USB 和正式固件烧录日志、三次启动日志、网关采集日志、CSV 和质量报告。该目录不进入 Git。
- 测试脚本：`tmp/heltec42652_test_20261120/gw_probe_1004.py`，仅调用项目上位机接口，不修改固件。
- 接线变化：无。所有测试使用 3.3 V 供电和共地。
- 烧录结果与实机结果分开记录；本轮没有修改固件或上位机代码。
- 下一步：先修正外部 32 kHz 频率或时间戳换算，使时间轴偏差进入验收范围；随后再执行 8 节点扩容和两小时稳定性测试。

## 13. 已知限制

1. 当前只完成单节点 1003 的端到端验收。
2. 8 节点共用 ACK 帧的信道容量和时隙余量尚未测试。
3. 尚未完成 1→2→4→8 节点扩容。
4. 尚未完成 8 节点 50 Hz 连续两小时稳定性测试。
5. 100 Hz/SF5 尚未启用，不能只通过修改界面参数绕过。
6. 400 ms ACK 发射补偿当前写死在 `host/gateway.py`。
7. 更换 E870 或网关固件后必须重新标定 ACK 发射延迟。
8. 外部 32 kHz 时钟当前不合格，正式采样使用传感器内部时钟。
9. 传感器内部时钟的长期温漂和跨节点同步精度尚未形成最终验收数据。
10. YBX-IIM42652 模块的供电和电平规格仍缺少厂家完整资料。
11. Heltec 与 IIM42652 组合已通过单次 60 秒 LoRa 和 ACK 采集（run 3042232764），但外部 32 kHz 载波约 32.26 kHz，不合格；内部时钟回退后时间轴约快 0.5%，跨节点同步验收仍未通过。
12. GNSS/PPS 健康状态会波动，每次 START 前仍要重新读取。
13. `clock_hz` 和 `ppm` 在内部时钟模式下没有判定意义。
14. 数据、日志、构建产物和网关本地配置不进入 Git。
15. 当前分支尚未推送到远程，合并 `main` 前必须获得明确授权。

## 14. 故障排查与恢复

### 14.1 节点启动失败

检查顺序：

1. 读取完整 BOOT 日志。
2. 发送 `STATUS`，检查 `startup_attempts`、`startup_failure` 和 `fault_reason`。
3. 发送 `DIAG`，检查 `WHO_AM_I`、`ACCEL_CONFIG0`、`PWR_MGMT0` 和 FIFO 配置。
4. 确认 `WHO_AM_I=0x6F`（IIM42652）；IIM-42352 的预期值为 `0x6D`。
5. 查看 `SENSOR_CHECK_ATTEMPT` 的 `drdy`、`fifo_samples`、`clock_edges`、`sample_valid` 和 `identity_ok`，先定位失败条件，再调整硬件或参数。
6. 确认节点处于 Waiting 且队列为 0。
7. 必要时发送 `SENSOR_RETRY`。
8. 如果仍失败，停止修改采样参数，执行六线最小测试。

`SENSOR_RETRY` 不允许在 Armed、Capturing 或 Draining 状态执行。

### 14.2 六线最小身份测试

六线测试只连接 VCC、GND、SCK、MOSI、MISO 和 CSB。不要连接 INT1、CLKIN、GNSS 或 LoRa 外设。

| 模块信号 | EoRa GPIO |
|---|---:|
| VCC | 3.3 V |
| GND | GND |
| SCK | 47 |
| MOSI | 48 |
| MISO | 39 |
| CSB | 40 |

构建和烧录：

```powershell
Set-Location .\minimal_six_wire_iim42652
pio run -e eora_s3_400_iim42652_six_wire
pio run -e eora_s3_400_iim42652_six_wire -t upload --upload-port COM8
pio device monitor -p COM8 -b 460800
```

只有 GPIO40 被独立证明异常时，才使用备用 CS GPIO35 环境 `eora_s3_400_iim42652_six_wire_alt_cs`。当前没有证据要求改用 GPIO35。

### 14.3 无数据或采样缺口

检查：

- `STATUS.state`
- `STATUS.queue`
- `STATUS.lost_samples`
- `STATUS.lost_packets`
- `FLOW.valid`
- `FLOW.invalid`
- `FLOW.fifo_errors`
- `FLOW.ts_anomaly`
- `FLOW.dt_mean_us`
- FIFO 包头和包长
- 传感器 INT1 状态
- 正式环境是否误用了 USB 或台架配置

### 14.4 GNSS/PPS 不健康

检查：

- GNSS 天线和室外视野。
- GNSS TX/RX 是否接反。
- GNSS 电源和共地。
- UART 是否为 115200。
- RMC 是否包含有效定位。
- PPS 是否接入 GPIO41。
- `GPS_DIAG` 的 `fix`、`pps`、`time`、`healthy` 和 `age_ms`。
- PPS 间隔是否约为 1 秒。

不要依赖 PPS 指示灯。正式 START 前必须重新读取 `GPS_DIAG`。

### 14.5 网关无心跳或不能下行

检查：

- 网关和电脑 IP 是否在同一网段。
- 网关 `192.168.1.10` 是否可达。
- 电脑 UDP 1700 是否被占用。
- Windows 防火墙是否阻止 UDP 1700。
- 网关是否持续发送 `PULL_DATA`。
- 当前 `gw` 服务是否运行。
- 当前软链接是否指向项目配置。
- 当前配置是否允许 470.3 MHz 下行。
- 网关是否能返回 `TX_ACK`。
- ACK 空口到达是否位于节点监听窗口。

`TX_ACK` 不能代替节点 CONFIRM。

### 14.6 队列持续增长

先停止扩容。检查：

- 节点上行频率是否正确。
- 网关是否包含对应信道。
- SF、带宽、编码率和同步字是否一致。
- ACK 是否到达节点。
- ACK 请求是否早约 400 ms 下发。
- 是否存在同频干扰或节点重复 ID。
- 网关是否持续把已保存序号写入 ACK。
- 是否有节点长期离线或反复重启。

### 14.7 固件回退

优先使用提交和标签回退，不要手工复制旧二进制。

```powershell
git show verified/2026-11-16-tx-50hz -- firmware platformio.ini
git switch --detach verified/2026-11-16-tx-50hz
```

验证标签 `verified/2026-11-16-tx-50hz` 不得移动、覆盖或删除。当前 2026-11-17 固件增加了软复位轮询和内部时钟回退；回退后需要重新确认传感器启动行为。

## 15. 工程目录和资料边界

| 路径 | 用途 |
|---|---|
| `docs/PROJECT_MASTER.md` | 当前唯一技术总文档 |
| `PROJECT_PLAN.md` | 当前步骤、状态和下一步 |
| `README.md` | 项目入口 |
| `platformio.ini` | 唯一 PlatformIO 固件构建配置 |
| `firmware/boards/` | 板级引脚和 LoRa 适配 |
| `firmware/sensors/` | IIM-42352、IIM-42652 和原厂驱动 |
| `firmware/common/` | 采样、时间、协议、状态机和队列 |
| `host/` | E870 UDP、GUI、配置、CSV 和质量报告 |
| `gateway/` | 网关配置模板和本地状态目录 |
| `tests/` | Python 协议和上位机测试 |
| `minimal_six_wire_iim42652/` | 六线最小身份测试工程 |
| `tmp/` | 本机测试数据、日志、手册和临时脚本，不进入 Git |

Git 不提交：

- 采集 CSV 和运行数据。
- 串口日志。
- `.pio` 构建产物。
- `.venv`。
- `gateway/state/` 本地配置和备份。
- `tmp/` 临时资料。

`tmp/` 虽然已在 `.gitignore` 中，但历史上有少量文件已被 Git 跟踪。2026-11-17 已把可恢复的历史资料移出工作区，并把已跟踪的旧 `tmp/pdfs` 文件从 Git 索引移除。

清理结果：

| 项目 | 清理前 | 清理后 |
|---|---:|---:|
| 文件数 | 1403 | 97 |
| 占用空间 | 296.87 MB | 12.78 MB |
| 释放空间 | — | 284.09 MB |

当前只保留 `e870_live_check_20261117`、`e870_lora_ack_20261117`、`e870_lora_rx_20261116`、`EoRa_PI_hardware`、`logs` 和 `pdfs` 六类目录。最新 LoRa 验收数据、关键日志和官方资料继续保留在本机，不进入 Git。

## 16. 当前下一步

1. 先完成当前 Heltec + IIM-42352 的完整断电重启及身份复测；恢复 `0x6D` 后验证启动自检、16 字节 FIFO 和 50 Hz 连续采样。
2. 补做单节点 5 分钟 LoRa 连续收发和补传稳定性测试。
3. 把 400 ms ACK 发射补偿改为可配置项。
4. 评估加宽节点 ACK 接收窗口，降低对网关固定延迟的依赖。
5. 按 1→2→4→8 节点逐级扩容。
6. 8 节点 50 Hz 连续两小时通过后，再设计 100 Hz/SF5 验收。
7. 需要合并 `main` 时，先取得用户明确授权。

## 17. 文档维护规则

从 2026-11-17 起，项目只维护本文件作为技术总文档。

- 新的功能、接线、协议和实测结果直接更新本文件。
- `PROJECT_PLAN.md` 只保存当前步骤、状态和下一步。
- 不再创建按日期堆积的 `docs/changes/` 报告。
- 历史版本通过 Git 提交查询，不把旧报告继续保留在工作区。
- 每轮先更新计划，再修改代码或文档。
- 编译结果和实机结果分别记录。
- 采集数据、日志和构建产物不进入提交。


