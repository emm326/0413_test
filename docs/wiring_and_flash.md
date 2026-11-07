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
新增同组合节点时，在该环境的build_flags中设置唯一`APP_DEVICE_ID_VALUE`（1001..1010），烧录记录保存环境、端口、节点和提交号。
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

## 旧工程回退入口

原src/include/lib保留，旧配置保存为`platformio_legacy.ini`：

```powershell
pio run -c platformio_legacy.ini -e heltec_wifi_lora_32_V3_iim_gps_tx_1001
```

旧固件使用v2轮询协议，新固件使用v3，与上位机配套选择。
