# E870 八节点采集项目交接文档

交接日期：2026-11-15（Asia/Hong_Kong）。仓库分支：`codex/sensor-recovery-20261114`；本次复测基线：`1c2cb94`。本文随计划、诊断文档和本轮Git提交更新，最终提交号以Git记录为准。

## 1. 接手先读与当前结论

先读[项目计划](../PROJECT_PLAN.md)，再按本文接手。用户已更换新的IIM42652模块，并按六线连接。本轮已在COM8执行只读身份复测并保存文档；没有重新烧录，操作硬件范围仅限串口扫描，也不自动推送远端。

项目目标是通过一台E870-L470LG12-O，将8个节点的UTC同步三轴加速度数据保存到电脑。当前软件框架、上位机和诊断入口已具备，**正式采集目标尚未完成**：最近1003实机身份为`0x00`，有效样本为0，节点处于Fault。

| 项目 | 状态 | 交接时可依赖的结论 |
|---|---|---|
| 目录、四种硬件组合、协议v3 | 已完成 | 固件唯一入口为根目录platformio.ini |
| E870双向接入、桌面上位机 | 已完成 | 已有历史网关及1003节点控制往返证据，见第4节 |
| 节点1003传感器识别、配置及采样 | 待实测 | 2026-11-15更换传感器后最新WHO仍为00、configured=0、samples=0 |
| GNSS/PPS与时钟同步验收 | 待实测 | 板端32kHz边沿恢复；clock_ok仍0 |
| 1→2→4→8节点50Hz扩容、两小时验收 | 待实测 | 不可用单节点控制成功代替数据验收 |
| 8节点100Hz/SF5 | 待开始 | 50Hz验收通过后才能显式启用 |

## 2. 固定配置与接线

固定使用EoRa-S3-400TB或低频版Heltec V3.2，搭配IIM-42352或IIM-42652。IIM42652只启用加速度，陀螺仪关闭。ESP32输出32000Hz、50%占空比到CLKIN；GNSS UART提供UTC、PPS校准时间映射。沿用现有供电和共地，DS3231断开。完整接线与烧录说明见[接线文档](wiring_and_flash.md)。

| 信号 | EoRa-S3-400TB | Heltec V3.2 LF |
|---|---|---|
| 传感器SCK / MOSI | GPIO47 / GPIO48 | GPIO47 / GPIO48 |
| 传感器MISO / CS | GPIO39 / GPIO40 | GPIO33 / GPIO34 |
| 传感器INT1 / CLKIN | GPIO38 / GPIO36 | GPIO26 / GPIO3 |
| GNSS TX→主控RX / GNSS RX←主控TX | GPIO16 / GPIO15 | GPIO5 / GPIO6 |
| GNSS PPS | GPIO41 | GPIO4 |
| 串口与波特率 | 原生USB CDC，460800 | CP210x/UART0，460800 |
| Flash配置 | 4MB，2MB Quad PSRAM配置 | 板型8MB |

YBX-IIM42652模块继续沿用现有3.3V供电，模块额定VCC和板级电路尚缺厂家资料。芯片手册不能替代模块供电说明；不得仅根据WHO失败推定接线错误、模块损坏或改用其他电压。用户已确认现有接线。

| 正式构建环境 | 默认节点 | 传感器预期WHO_AM_I |
|---|---:|---|
| eora_s3_400_iim42352_tx | 1001 | 0x6D |
| heltec_v32_iim42352_tx | 1002 | 0x6D |
| eora_s3_400_iim42652_tx | 1003 | 0x6F |
| heltec_v32_iim42652_tx | 1004 | 0x6F |

部署时通过`APP_NODE_ID`指定1001–1008，ID与板型/传感器解耦；每个节点必须唯一。USB测试环境把末尾`_tx`换为`_usb_test`，不初始化LoRa。

| 节点 | 1001 | 1002 | 1003 | 1004 | 1005 | 1006 | 1007 | 1008 |
|---|---|---|---|---|---|---|---|---|
| 上行MHz | 470.3 | 470.5 | 470.7 | 470.9 | 471.1 | 471.3 | 471.5 | 471.7 |

50Hz无线参数固定为SF6/BW125kHz/4/5、私有同步字0x12、前导码12；控制下行统一470.3MHz。正常数据包包含25组int16 XYZ、长度180字节、协议v3。五秒周期为0–4.00秒正常上传、4.00–4.25秒下行控制、4.25–4.45秒节点确认、4.45–5.00秒补传。详见[协议](protocol_v3.md)。

## 3. 最近硬件状态与故障边界

以下为现场记录。2026-11-15已再次识别COM8并通过串口读取新传感器结果；COM口会随枚举变化，接手时必须再次识别板型、设备和节点ID，不能只凭COM编号烧录。

- **2026-11-15 更换传感器后的六线复测**：用户更换新IIM42652并保持六线连接；COM8继续运行[六线最小身份程序](../minimal_six_wire_iim42652/README.md)。CS/SCK/MOSI低0高1均通过，旧CS持续低电平问题消失；MISO上拉和下拉均为1，空闲为1，四种SPI组合共32次WHO均00。当前应在模块端检查3.3V、GND、CSB、SCK、MOSI、MISO；不要改用GPIO35。
- **EoRa / IIM42652 / 节点1003**：最近为COM8，USB标识`90:DA:72:76:1F:30`；最后实测结束于2026-11-14 22:56:57 +08:00。`WHO_AM_I=0x00`，`state=5`（Fault）、`gps=1`、`clock_ok=0`、`queue=0`、丢样/丢包计数均0；这里的0计数不代表成功采集。
- **20点内部时钟诊断**：`configured=0,samples=0,target=20,ok=0`。由于先在身份确认失败，内部时钟配置和直接采样循环没有执行，不能称为“内部时钟采样通过”。
- **时钟输出恢复**：诊断后LEDC/PCNT恢复实测2ms内64边沿，后续约5秒160000边沿；这是板端输出证据，未完成模块端示波器验收、PPS连续测量或±100ppm验收。
- **Heltec / IIM42652 / 节点1004**：最近为COM6/CP210x，MAC `9c:13:9e:9d:e8:a8`；WHO为0，状态Fault，异常IRQ已关闭，未取得有效样本。此设备结果独立于EoRa。

1003最后烧录的正式主程序大小为372208字节，SHA256为`6a5088b5499ae442d00b8218634a0ea32b80ea2003606df99060b1cce9c932bb`。此哈希仅用于识别历史设备镜像，本轮构建产物不视为已经部署。

2026-11-11曾读到6F并通过启动自检；2026-11-14中途也曾出现6F。较早成功不能覆盖最后一次失败。当前没有完成原定300秒正式采集。最新证据见[更换传感器后的六线复测](changes/2026-11-15_03_更换传感器后的六线复测.md)，上一轮六线结果见[六线最小IIM42652身份测试](diagnostics/2026-11-15_02_六线最小IIM42652身份测试.md)。

## 4. 网关与电脑

网关`192.168.1.10`，电脑有线地址`192.168.1.22`，Semtech UDP v2端口1700，网关EUI `0000000000000923`。使用私有LoRa八信道配置和显式`e870-l470lg12-o`设备档；保留原厂校准，不直接改原厂配置数字绕过设备范围。

2026-11-14节点1003已有3次下发、3次`TX_ACK NONE`及3次匹配的节点CONFIRM，470.7MHz上行RSSI约-35dBm、SNR 10.2/10.5/10.2dB；通过的是单节点双向控制链路，没有启动采样。网关TX_ACK只证明网关执行状态，节点CONFIRM才证明节点收到相应命令。历史三次专用下发自检与节点CONFIRM测试是不同证据，均不能代替数据完整率验收。

现场配置`gateway/state/active.local.json`、本地连接配置和备份均不进Git；仅克隆仓库不能直接恢复现场配置。初始完整备份在`gateway/state/backups/20261112T055807747491Z_a9ee522e/`，另有应用前备份与设备侧副本。若交接到另一台电脑，需要另行复制这些现场文件，密码不写入文档。

原厂`470_CN470.json`历史SHA256为`7e036e4f60ccfb14697fbca825df01fa1ac1f7aa91771f89e596bc3e44e58f15`。2026-11-12曾发现网关设备日期为2026-08-01，日志须保留设备原时间并用电脑时间关联；本轮没有检查设备时间或校时。

## 5. 本轮提交保存的实现

| 文件/目录 | 当前实现与用途 |
|---|---|
| firmware/common/sensor/IimBase.*、Registers.h | 稳定身份检查、SPI重绑/回退、寄存器读回、FIFO计数/格式/样本校验、先停Accel/Gyro再配置、DRDY直接读取 |
| firmware/sensors/iim42652/Driver.* | IIM42652四线SPI/I3C相关设置、接口读回、FIFO配置，保持IDLE RC时钟 |
| firmware/common/sensor/StartupCheck.h | 自检与诊断状态门控，拒绝身份/样本/时钟不完整时宣布就绪 |
| firmware/common/app/NodeApp.* | 不发传感器软件复位的恢复入口、严格启动自检、SPI独立对照和有界20点直接采样 |
| firmware/common/clock/SampleClock.* | 诊断时隔离CLKIN输出，恢复LEDC及PCNT输入路由并重新累积PPS测量 |
| tests/test_firmware_sensor.py、firmware_sensor_transport.cpp、firmware_support/ | 使用模拟SPI执行真实驱动的31项专项；纳入Python测试入口 |
| docs/diagnostics/ | 前轮04–19号诊断记录，保留失败、回退和现场边界 |

板级代码继续放`firmware/boards/`，驱动放`firmware/sensors/`，共享业务放`firmware/common/`。上位机在`host/`，网关模板在`gateway/`，构建节点号注入在`tools/platformio_node_id.py`。

## 6. 启动与离线验证

在仓库根目录双击`启动E870上位机.cmd`；它使用现有`.venv`，启动时检查tkinter、pyserial、paramiko和psutil。没有该环境的新电脑需先准备依赖，现有电脑日常启动不需要联网安装。界面操作见[桌面操作说明](e870_desktop.md)，配置流程见[网关说明](e870_setup.md)。

```powershell
# 项目自带环境；系统Python可能缺少串口/SSH依赖
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# PlatformIO已在PATH时
pio run -e heltec_v32_iim42352_tx -e heltec_v32_iim42652_tx -e eora_s3_400_iim42352_tx -e eora_s3_400_iim42652_tx -e eora_s3_400_iim42652_usb_test

# 只打开上位机；不会因启动界面而自动采集
.\.venv\Scripts\python.exe -m host --gateway-config gateway/state/active.local.json
```

当前电脑PlatformIO路径是`C:\Users\26223\.platformio\penv\Scripts\pio.exe`。主机驱动专项需要PATH中有g++；当前使用`C:\mingw64\bin\g++.exe`。Python测试显示84项，其中1项测试编译并运行内部31项C++检查，不能把两者相加当成115项独立Python用例。

2026-11-15本轮验证结果：项目.venv下84/84项Python回归通过，内含31/31项真实驱动模拟SPI专项通过；上述四个正式环境与EoRa42652 USB测试环境共5/5构建通过。系统Python初次复核因临时目录权限及缺少paramiko/pyserial未通过，改用项目自带.venv和所需临时目录访问权限后通过；无需安装依赖或修改测试。构建和模拟测试通过只证明软件验证通过，硬件结论仍以第3节为准。

## 7. 接手顺序与成功判据

1. 确认当前分支和计划，读取19号与18号长期诊断；先保留现场设备状态、配置和历史证据。当前不靠反复烧录或修改引脚猜测原因。
2. 针对1003的SPI不稳定取得独立证据：新传感器的CS/SCK/MOSI板端输出已正常，下一步只在模块端核查3.3V、GND、CSB和SCK/MOSI/MISO，确认MISO/MOSI没有接反。用示波器或逻辑分析仪捕获一次0xF5命令和返回，或取得厂家已验证的最小例程与模块原理图。先恢复持续正确6F，再验证配置读回。
3. 需要再次操作设备时，确认现有设备身份与现场允许的操作范围。继续跳过IIM42652芯片软件复位；不擅自改变供电、接线或网关配置。诊断命令`SENSOR_RETRY`会配置寄存器并清FIFO，`SENSOR_LINK_TEST`会暂时切换SPI路由，`SENSOR_SAMPLE_TEST`会隔离CLKIN并临时配置内部时钟，均不是纯状态查询。
4. 仅在Waiting/Fault、无活动采样且queue=0时使用诊断入口。20点测试须实际输出20条有效XYZ、`configured=1,samples=20,ok=1`，结束确认外部32kHz恢复。内部测试使用local_us、无UTC同步，不能替代正式采集。原始XYZ换算g为`raw × 4 / 32768`（±4g测试）。
5. 正式自检须同时满足身份、有效XYZ、DRDY/FIFO数量及板端时钟边沿；不能强制移除Fault或用零值填样。默认约120ms检查要求DRDY/FIFO各4–30、clock_edges 3000–4700，并且计数、样本和身份均有效。
6. 检查GNSS UTC/PPS；连续60个有效PPS间隔完成时钟测量，要求`clock_ok=1`且误差在±100ppm内，补做示波器波形验收。`gps=1`本身不足以证明时钟健康。
7. 先对目标1003完成300秒50Hz采集，检查匹配运行号、节点CONFIRM、真实DATA、分节点CSV、时间戳及队列。START必须取得全部目标节点的成功CONFIRM；缺失则取消并按协议STOP，不能忽略拒绝原因。
8. 再按1→2→4→8扩容。8节点50Hz连续2小时，每节点约360000样本；补传后每节点缺失率≤0.1%，99%样本10秒内到达，队列无持续增长，无跨节点/运行污染。步骤6通过后才进入100Hz/SF5。

## 8. Git、报告与本地资料

本轮在已有分支更新计划、交接和复测记录，不新建分支。远端为`origin`，当前分支尚无跟踪分支；“提交”在本轮指本地Git commit，未执行push。可用`git log -1 --oneline`查看本次复测提交，用`git show 1c2cb94`查看本轮前基线。清理前工程保留于标签`archive/pre-cleanup-20261108`，不回退当前工作区。

当前报告为[更换传感器后的六线复测](changes/2026-11-15_03_更换传感器后的六线复测.md)。按项目规则，changes/只留本轮报告；已进入历史的旧报告通过Git查询，上一轮02号六线报告已转入diagnostics/长期保存。

采集数据、原始日志、镜像、构建产物、`.venv/`、`.pio/`、`tmp/`和`gateway/state/`不进入提交。本轮复核日志保存在本机`tmp/handover_20261115/`，31项结果保存在`tmp/firmware_sensor_tests/result.log`。诊断文档中的tmp链接依赖本机留存资料，克隆仓库后可能不可用；关键结果已写入长期文档。其他长期Markdown、历史资料与数据原位保留。
