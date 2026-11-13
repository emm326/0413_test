# 四种组合接线与烧录

## 接线约定

使用3.3V逻辑、引出CLKIN的IIM模块。表中的脚号是芯片脚号，模块排针以信号名为准；先核对模块电源和电平转换电路。VDDIO必须与3.3V信号兼容。
所有模块共地。断开DS3231；PPS只接主控PPS脚。板载LoRa无需外部杜邦线。

## A：Heltec V3.2 + IIM-42352

环境：`heltec_v32_iim42352_tx`，默认节点`1002`。

| 模块信号 | 主控连接 |
|---|---|
| IIM VDD(8)/VDDIO(5) | 3.3V（按模块原理图接供电入口） |
| IIM GND | GND |
| IIM SCK/AP_SCLK(13) | GPIO47 |
| IIM MOSI/AP_SDI(14) | GPIO48 |
| IIM MISO/AP_SDO(1) | GPIO33 |
| IIM CS/AP_CS(12) | GPIO34 |
| IIM INT1(4) | GPIO26 |
| IIM CLKIN/INT2/FSYNC(9) | GPIO3，输出32000Hz/50% |
| GNSS TX → 主控RX | GPIO5 |
| GNSS RX ← 主控TX | GPIO6 |
| GNSS PPS → 主控输入 | GPIO4 |
| GNSS GND | GND |
| GNSS 电源 | 沿用现有模块标注的供电入口；信号需3.3V兼容 |

固件识别号为`0x6D`。

## B：Heltec V3.2 + IIM-42652

环境：`heltec_v32_iim42652_tx`，默认节点`1004`。

| 模块信号 | 主控连接 |
|---|---|
| IIM VDD(8)/VDDIO(5) | 3.3V（按模块原理图接供电入口） |
| IIM GND | GND |
| IIM SCK/AP_SCLK(13) | GPIO47 |
| IIM MOSI/AP_SDI(14) | GPIO48 |
| IIM MISO/AP_SDO(1) | GPIO33 |
| IIM CS/AP_CS(12) | GPIO34 |
| IIM INT1(4) | GPIO26 |
| IIM CLKIN/INT2/FSYNC(9) | GPIO3，输出32000Hz/50% |
| GNSS TX → 主控RX | GPIO5 |
| GNSS RX ← 主控TX | GPIO6 |
| GNSS PPS → 主控输入 | GPIO4 |
| GNSS GND | GND |
| GNSS 电源 | 沿用现有模块标注的供电入口；信号需3.3V兼容 |

固件识别号为`0x6F`；只启用加速度，陀螺仪关闭。

## C：EoRa-S3-400TB + IIM-42352

环境：`eora_s3_400_iim42352_tx`，默认节点`1001`。

| 模块信号 | 主控连接 |
|---|---|
| IIM VDD(8)/VDDIO(5) | 3.3V（按模块原理图接供电入口） |
| IIM GND | GND |
| IIM SCK/AP_SCLK(13) | GPIO47 |
| IIM MOSI/AP_SDI(14) | GPIO48 |
| IIM MISO/AP_SDO(1) | GPIO39 |
| IIM CS/AP_CS(12) | GPIO40 |
| IIM INT1(4) | GPIO38 |
| IIM CLKIN/INT2/FSYNC(9) | GPIO36，输出32000Hz/50% |
| GNSS TX → 主控RX | GPIO16 |
| GNSS RX ← 主控TX | GPIO15 |
| GNSS PPS → 主控输入 | GPIO41 |
| GNSS GND | GND |
| GNSS 电源 | 沿用现有模块标注的供电入口；信号需3.3V兼容 |

固件识别号为`0x6D`。

## D：EoRa-S3-400TB + IIM-42652

环境：`eora_s3_400_iim42652_tx`，默认节点`1003`。

用户于2026-11-13提供的模块正面图片丝印为`YBX-IIM42652`。图中正面朝向不变时，模块排针从左到右与现有程序的对应如下。下表是固定配置与丝印的映射，不代表已通过图片核验现场连线或模块额定电压。

| 排针位置（左起） | 图中丝印 | 当前计划连接 |
|---:|---|---|
| 1 | VCC | 沿用现有3.3V供电；YBX模块额定输入仍待厂家资料确认 |
| 2 | GND | 主控GND，共地 |
| 3 | SCK | GPIO47 |
| 4 | MOSI | GPIO48 |
| 5 | MISO | GPIO39 |
| 6 | CSB | GPIO40，SPI片选 |
| 7 | INT1 | GPIO38，中断输入 |
| 8 | INT2 | GPIO36，按现有方案用作CLKIN输入，主控输出32000Hz |

模块排针序号与下方括号中的芯片封装脚号是两套编号。TDK手册确认芯片INT2/FSYNC/CLKIN共用封装脚9；驱动通过bank1 `INTF_CONFIG5.PIN9_FUNCTION=10`及`RTC_MODE`选择时钟功能，模块丝印只写INT2并不意味着芯片不支持CLKIN。芯片默认四线SPI，不能仅凭识别失败就要求增加接口切换跳线。

本次未取得该YBX模块可靠原理图或供电说明。正面图不能确认内部VDD/VDDIO电压、稳压器压差、电平转换拓扑或背面焊桥；不据图修改供电电压、旁路元件或调整接线。待购买链接/商家说明确认板级电路，必要时实测模块端供电和信号。芯片手册不等同于模块供电规格。

芯片依据：[TDK IIM-42652 DS-000440 v1.1](https://product.tdk.com/system/files/dam/doc/product/sensor/mortion-inertial/imu/data_sheet/ds-000440-iim-42652-typ-v1.1.pdf)，引脚表与INTF_CONFIG4/5寄存器；项目本地DS-000440 v1.3文本也有相同功能说明。本次图片核对报告见[本轮报告](diagnostics/2026-11-13_05_YBX模块照片核对.md)。

| 模块信号 | 主控连接 |
|---|---|
| IIM VDD(8)/VDDIO(5) | 3.3V（按模块原理图接供电入口） |
| IIM GND | GND |
| IIM SCK/AP_SCLK(13) | GPIO47 |
| IIM MOSI/AP_SDI(14) | GPIO48 |
| IIM MISO/AP_SDO(1) | GPIO39 |
| IIM CS/AP_CS(12) | GPIO40 |
| IIM INT1(4) | GPIO38 |
| IIM CLKIN/INT2/FSYNC(9) | GPIO36，输出32000Hz/50% |
| GNSS TX → 主控RX | GPIO16 |
| GNSS RX ← 主控TX | GPIO15 |
| GNSS PPS → 主控输入 | GPIO41 |
| GNSS GND | GND |
| GNSS 电源 | 沿用现有模块标注的供电入口；信号需3.3V兼容 |

固件识别号为`0x6F`；只启用加速度，陀螺仪关闭。

## 板载资源

- Heltec板载LoRa：SCK9、MOSI10、MISO11、CS8、RST12、BUSY13、DIO1=14；GPIO36为Vext控制，CLKOUT使用GPIO3。
- EoRa板载LoRa：SCK5、MOSI6、MISO3、CS7、RST8、BUSY34、DIO1=33；GPIO21控制E22电源；CLKOUT使用GPIO36。
- EoRa OLED使用GPIO17/18；新接线不占用这些引脚。固件当前用串口输出状态，OLED界面未实现。
- 本轮固定EoRa PPS=GPIO41。GPIO33/34不再接传感器。
- 依据：EoRa原理图的Peripheral and I/O mapping table、Heltec板型文件，以及IIM-42352第18页/IIM-42652第20页芯片引脚表。

## 编译与烧录

在项目根目录执行，以下以方案A为例：

```powershell
pio run -e heltec_v32_iim42352_tx
pio run -e heltec_v32_iim42352_tx -t upload --upload-port COM5
pio device monitor -p COM5 -b 460800
```

按上表更换环境名；COM5换成实际设备端口。先核对BOOT日志中的板型、传感器、节点和版本。
同一硬件组合可部署为任一节点 `1001–1008`。不要为每个节点复制 PlatformIO 环境；烧录前通过 `APP_NODE_ID` 指定唯一节点，未设置时才使用环境内的默认值：

```powershell
$env:APP_NODE_ID="1005"
pio run -e heltec_v32_iim42352_tx -t upload --upload-port COM5
Remove-Item Env:APP_NODE_ID
```

`APP_NODE_ID` 非整数或超出 `1001–1008` 时构建会停止。烧录前检查没有重复节点，烧录后核对 BOOT 日志中的 `id` 和 `uplink`。节点与频率固定映射为 1001/470.3、1002/470.5、1003/470.7、1004/470.9、1005/471.1、1006/471.3、1007/471.5、1008/471.7 MHz。
EoRa使用4MB Flash和2MB Quad PSRAM配置；Heltec沿用其8MB板型。两者都使用原生USB CDC。

## USB时钟/采样验证

环境对应替换为`heltec_v32_iim42352_usb_test`、`heltec_v32_iim42652_usb_test`、`eora_s3_400_iim42352_usb_test`、`eora_s3_400_iim42652_usb_test`。
USB测试不初始化LoRa。上电自检后等待GNSS/PPS，连续60个有效PPS间隔完成时钟测量；状态需显示clock_ok=1。
串口命令：

```text
STATUS
START_AT <未来UTC微秒> 100 4 123 600
STOP
```

START_AT依次为UTC启动微秒、50或100Hz、量程g、非零运行编号、时长秒（0=手动停止）。先用GNSS已锁定的电脑UTC计算未来约10秒的时间。
输出`SAMPLE,node,run,utc_us,x,y,z`，XYZ为原始int16。
采集期间不拔插时钟线；用示波器验证CLKIN=32.000kHz，时钟测量必须在±100ppm内。

## 当前工程与历史版本

本文环境均由根目录 `platformio.ini` 构建，使用当前 `firmware/` 固件；四种正式组合和四种 USB 测试环境继续保留。
旧版固件及专属工具已从工作目录移除，历史代码通过 Git 标签 `archive/pre-cleanup-20261108` 查询。现行节点需配套当前 `host/` 上位机和协议 v3。
