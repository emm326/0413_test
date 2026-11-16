# 已验证固件版本说明（2026-11-16）

## 用途

本文件标记 EoRa-S3-400TB + IIM42652 单节点正式固件的已验证版本。后续排障、交接和扩容都以本文件、Git 提交和 Git 标签为准。

## 版本标识

| 项目 | 内容 |
|---|---|
| 验证日期 | 2026-11-16（Asia/Hong_Kong） |
| 验证分支 | `codex/20261116-baseline` |
| 固件源码提交 | `cb99db0`（fix: 增加传感器启动自动恢复与时钟回退） |
| 说明所在提交 | 以 `git log -1 --oneline` 为准 |
| Git 标签 | `verified/2026-11-16-tx-50hz` |
| 固件版本字符串 | `3.0.1` |
| 构建环境 | `eora_s3_400_iim42652_tx` |
| 目标板 | EoRa-S3-400TB，ESP32-S3，8 MB Flash |
| 设备 MAC | `90:da:72:76:1f:30` |
| 节点号 | `1003` |
| 串口 | COM8，460800 波特率 |
| 固件文件 | `.pio/build/eora_s3_400_iim42652_tx/firmware.bin` |
| 固件文件大小 | 393936 字节 |
| 固件 SHA256 | `70871D6077F2771DFA93F78450A999E9109F681C8CF5BA7C3D8B0271DB2251AB` |
| 资源占用 | RAM 54676/327680 字节；Flash 393573/1310720 字节 |

## 代码一致性说明

固件源码与成功采样版本 `cb99db0` 完全一致。核对命令：

```powershell
git diff cb99db0 HEAD -- firmware platformio.ini
```

命令无输出，表示固件代码和构建配置没有变化。2026-11-16 之后到当前提交之间只增加文档。

## 已验证内容

- 室外 GNSS：`fix=1,pps=1,time=1,healthy=1`，PPS 间隔 999984 µs，`pps_bad=0`。
- 采样配置：50 Hz、±4 g、内部时钟、run 7010、持续 60 秒。
- 有效样本 2972 条；丢样 0 条；FIFO 错误 0 次；时间戳异常 0 条。
- 样本间隔：平均 19999 µs，最小 19985 µs，最大 20002 µs。
- 传感器诊断：`who=0x6F,accel=0x49,power=0x03,fifo_cfg=0x80`。
- 最终 FLOW：`valid=2972,invalid=0,timed=2972,fallback=0,ts_anomaly=0,fifo_errors=0,fifo_packet=16`。

## 复现步骤

1. 编译并烧录正式固件：

```powershell
pio run -e eora_s3_400_iim42652_tx -t upload --upload-port COM8
```

2. 打开串口，波特率 460800。
3. 启动后读取 `STATUS`。正常目标为 `state=1,startup_failure=none,fault_reason=none`。
4. 如果 `sensor=0` 或状态为 `state=5`，发送 `SENSOR_RETRY`，等待一次完整启动自检。
5. 发送 `GPS_DIAG`，确认 `healthy=1`。
6. 发送 `START_AT <未来UTC微秒> 50 4 <运行编号> 60`。
7. 用 `STATUS`、`FLOW` 和 `DIAG` 核对样本数、时间间隔和 FIFO 状态。

## 已知限制

- 冷启动时传感器自检不稳定。本次冷启动 3 次均报 `vendor_init`，手动 `SENSOR_RETRY` 1 次后恢复。该现象不能忽略，后续每次上电都应检查 `STATUS`。
- 外部 32 kHz CLKIN 不可用，采样使用 IIM42652 内部时钟，`clock_ok=1`、`clock_source=sensor_internal`。
- `clock_hz` 和 `ppm` 反映已停用的外部时钟测量，不能用于判断内部时钟精度。
- E870 网关未上线，本次 119 个测试包未获 ACK，节点保持 `state=4`。
- 正式空口验收、到达率和补传率仍未完成。

## 相关文档

- 本轮变更报告：`docs/changes/2026-11-16_10_成功固件提交与说明.md`
- 项目计划：`PROJECT_PLAN.md`
- 交接文档：`docs/HANDOVER.md`