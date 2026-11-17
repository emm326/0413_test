# E870 八节点加速度采集

本项目使用 ESP32-S3 节点采集 IIM-42352/IIM-42652 三轴加速度，通过 GNSS UTC/PPS 建立时间基准，再经 LoRa 将数据发送到 E870-L470LG12-O 网关。电脑端负责启停、ACK、CSV 保存和质量统计。

当前已完成节点 1003 的单节点 50 Hz 采样、LoRa 上行、ACK 和补传验收。8 节点扩容、两小时稳定性和 100 Hz 尚未完成。

## 项目入口

- [项目技术总文档](docs/PROJECT_MASTER.md)
- [当前计划和状态](PROJECT_PLAN.md)
- [启动上位机](启动E870上位机.cmd)

`docs/PROJECT_MASTER.md` 是唯一技术总文档，包含：

- 当前功能和器件说明
- 四套硬件组合与固定接线
- IIM42652 采样、FIFO 和时钟策略
- GNSS/PPS 时间基准
- LoRa 协议 v3 和五秒通信周期
- E870 网关配置和 ACK 时隙
- 编译、烧录、串口命令和质量验收方法
- 最新实测结果、已知限制和故障恢复步骤

## 快速开始

打开桌面软件：

```powershell
.\启动E870上位机.cmd
```

构建并烧录当前正式节点：

```powershell
$env:PLATFORMIO_DISABLE_TELEMETRY='1'
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_tx -t upload --upload-port COM8
```

运行软件测试：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

烧录前必须重新确认实际 COM 口，不能默认仍为 COM8。

## 工程目录

| 目录 | 用途 |
|---|---|
| `firmware/` | 节点固件、板级适配、传感器驱动和公共业务 |
| `host/` | E870 UDP、桌面软件、CSV 和质量报告 |
| `gateway/` | 网关配置模板和本地状态 |
| `tests/` | 协议、网关、桌面控制和固件仿真测试 |
| `minimal_six_wire_iim42652/` | 六线最小身份测试工程 |
| `tools/` | 节点号构建辅助 |
| `docs/` | 唯一项目技术总文档 |
| `tmp/` | 本机测试数据、日志、手册和临时文件，不进入 Git |

采集数据、日志、构建产物、虚拟环境和本地网关配置不进入 Git。
