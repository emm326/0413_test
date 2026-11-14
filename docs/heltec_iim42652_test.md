# Heltec V3.2 + IIM-42652连线与分阶段测试

适用项目固定的Heltec V3.2（正式无线为低频版）与IIM-42652模块。默认节点1004，现有USB自检环境heltec_v32_iim42652_usb_test，正式环境heltec_v32_iim42652_tx。使用GPIO丝印编号，不是排针孔位序号。

## 1. 先接传感器8根线

断开主控和模块供电，将同一IIM模块从EoRa的所有连接中移出，只接本次Heltec。按模块丝印连接：

| IIM模块信号 | Heltec V3.2连接 | 作用 |
|---|---|---|
| VCC | 3.3V，沿用已确认的模块供电入口 | 供电 |
| GND | GND | 共地 |
| SCK | GPIO47 | SPI时钟 |
| MOSI | GPIO48 | 主控发往传感器 |
| MISO | GPIO33 | 传感器发回主控 |
| CSB/CS | GPIO34 | SPI片选 |
| INT1 | GPIO26 | 数据就绪输入 |
| INT2/CLKIN | GPIO3 | 主控输出32000Hz，模块端配置为CLKIN |

Heltec的GPIO36为Vext控制，时钟输出是GPIO3；EoRa曾使用的MISO39/CS40/INT38/CLK36不适用于这套接线。第一阶段不需要GNSS或E870，也不调用LoRa初始化/发射。请先用USB给Heltec供电，确认实际串口。

## 2. 烧录USB启动自检版

本项目已将Heltec的Serial配置为UART0，经板载CP2102连接USB；EoRa保持原生USB。这里的USB自检是测试模式名称，不要求把Heltec改焊成原生USB。

在项目目录PowerShell执行，COMx替换为Heltec实际端口，不能直接沿用上次EoRa端口：

```powershell
$env:APP_NODE_ID = '1004'
$pio = Join-Path $env:USERPROFILE '.platformio\penv\Scripts\pio.exe'
& $pio device list
& $pio run -e heltec_v32_iim42652_usb_test -t upload --upload-port COMx
& $pio device monitor --port COMx --baud 460800 --eol LF --echo
```

打开监视器后按一次RST，等待约5秒查看BOOT。需要退出串口监视器时按Ctrl+C；再次烧录前先退出，避免串口占用。无法进入下载模式时按住BOOT，点按RST，再松BOOT后重试；先确认选中的确实是Heltec。

## 3. 第一阶段看什么

下面是成功判据示例，不是本次实机记录，数字不必逐字一致：

```text
BOOT,...board=Heltec V3.2 LF,sensor=IIM-42652,id=1004,...
SENSOR_ID,...expected=0x6F,actual=0x6F,ok=1
SENSOR_RESET,identity=0x6F,ok=1
SENSOR_SAMPLE,valid=1,...
SENSOR_CHECK,init=1,drdy=12,fifo_samples=12,clock_edges=3840,...ok=1
READY,sensor=1,...clock_output=1,...
STATUS,id=1004,state=1,...
```

启动自检窗口约120ms，使用100Hz、±4g，正常DRDY/FIFO约12个，板端时钟约3840边沿；固件允许合理数量波动，不能把32kHz中断当成功。自检后停在Waiting，默认不会连续采集。

在监视器输入DIAG再回车，检查WHO仍0x6F及配置读回；输入STATUS查看状态。命令要求LF换行，上述监视器命令已指定。USB测试环境里READY的radio1是跳过无线后的占位就绪，不能当作无线初始化或收发通过。

没有GNSS时gps0、clock_ok0可以出现在第一阶段，独立于传感器自检。板端clock_edges只是输出存在检查，不是±100ppm精度验收。若actual0/FF或SENSOR_CHECK失败，保存从BOOT开始的完整日志，停在本阶段。

## 4. 第二阶段接GNSS/PPS

第一阶段通过后再接，所有模块共地；GNSS电源沿用其模块标注的入口，信号须3.3V兼容：

| GNSS信号 | Heltec连接 |
|---|---|
| TX | GPIO5（主控RX） |
| RX | GPIO6（主控TX） |
| PPS | GPIO4 |
| GND | GND |
| 电源 | 沿用GNSS模块已确认供电 |

固定GNSS UART 115200。等待有效UTC/PPS及连续至少60个有效PPS间隔，STATUS应达到gps1、clock_ok1且ppm满足±100。室内无法锁定时保持待实测，不降低正式采集门槛。USB模式的连续采集同样有这些门槛，不是无GNSS强行采样环境。

## 5. 第三阶段50Hz与无线

需要完整E870无线测试时，确认Heltec为适配本项目470MHz的低频硬件并接好天线，烧录heltec_v32_iim42652_tx。目标1004上行470.9MHz，控制470.3MHz，50Hz/SF6；本阶段先只启用1004，100Hz继续不启用。

在E870上位机采集页把目标节点设为1004，重新启动接收服务并查询；确认传感器、GNSS/PPS、时钟和网关下行就绪后预约50Hz采集。正常数据按25组XYZ打包，先观察短时采集、队列和缺失，再停止并等节点Waiting/queue0。只收到网关TX_ACK不能当节点已执行STOP。USB测试版不能替代无线验收，详见[上位机操作](e870_desktop.md)。

## 6. 当前验证边界

2026-11-14已完成板型核对、UART0串口修正和三个环境构建。用户随后授权在COM6烧录1004 USB自检版，Flash校验及日志读取通过，但WHO0、sensor0、state5，传感器自检未通过；未进行连续采集或无线验收。详见[实际烧录报告](diagnostics/2026-11-14_03_Heltec1004烧录与启动自检.md)与[软件准备记录](diagnostics/2026-11-14_02_Heltec与IIM42652接线和自检准备.md)。

官方依据：Heltec WiFi LoRa 32 V3.2数据手册及原理图，项目匹配Arduino 2.0.17/PlatformIO板型与Serial条件宏。原生USB GPIO19/20是额外硬件连接方式，不是板载CP2102串口。
