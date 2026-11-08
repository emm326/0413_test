# E870 八节点三轴加速度采集

当前固件 v3：EoRa-S3-400TB / Heltec V3.2 LF × IIM-42352 / IIM-42652，ESP32 输出 32000 Hz CLKIN，结合 GNSS/PPS 授时，通过 E870-L470LG12-O 分信道收发并在电脑保存数据。

- [项目计划与进度](PROJECT_PLAN.md)
- [四种组合接线及烧录](docs/wiring_and_flash.md)
- [E870 网关和上位机](docs/e870_setup.md)
- [协议 v3](docs/protocol_v3.md)
- [设备能力与当前限制](docs/e870_capability_review.md)
- [实机验收记录](docs/hardware_acceptance.md)
- [本轮变更报告](docs/changes/)

## 工程目录

| 目录 | 用途 |
|---|---|
| `firmware/` | 当前节点固件；板级适配、公共业务和传感器驱动分别放置 |
| `host/` | E870 UDP 接入、桌面上位机、运行控制、CSV 和质量报告 |
| `gateway/` | 与设备原厂配置合并的项目模板；本地设备配置不提交 Git |
| `tools/` | 节点编号构建脚本及包初始化文件 |
| `tests/` | 当前 E870 v3 与八节点自动测试 |
| `docs/` | 当前方案的操作、协议、验收和变更说明 |

历史采集数据、开题报告、PPT、文献、制作记录和临时资料继续保留原位；它们不属于当前固件构建入口。旧工程已从工作目录移除，可通过 Git 标签 `archive/pre-cleanup-20261108` 查询清理前版本。

## 构建与运行

在项目根目录执行；四种正式环境和四种 USB 测试环境详见接线与烧录说明。

```powershell
pio run -e heltec_v32_iim42352_tx
$env:APP_NODE_ID="1001"
pio run -e heltec_v32_iim42352_tx
Remove-Item Env:APP_NODE_ID
python -m host --gateway-config gateway/project.local.json
python -m unittest discover -s tests -v
```

`platformio.ini` 是当前唯一固件构建配置。构建及软件测试通过与实机验收分别记录，当前硬件状态以 `PROJECT_PLAN.md` 为准。
