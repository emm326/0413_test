# IIM42652 分阶段诊断与时钟修复

- 日期：2026-11-11（Asia/Hong_Kong）。
- 对应步骤：硬件验收第 1 节“单节点硬件”；本次故障定位、修复和启动自检已完成，连续采样与同步精度待实测。
- Git 基线：`75e0f1c`（`codex/e870-8-node`），本轮开始时工作区无改动。
- 提交约定：用户要求检查成功后再提交；本轮代码、文档和报告先保留工作区，未执行 git add/commit。原始日志和构建产物不提交。
- 测试前提：用户已确认接线无误，室内 GNSS 无信号；沿用原 EoRa + IIM42652、节点 1003、原接线、32 kHz 输出和协议配置。

## 故障定位

原启动日志只有 sensor=0。增加分阶段诊断后实测：

```text
SENSOR_ID,name=IIM-42652,expected=0x6F,actual=0x6F,ok=1
SENSOR_RESET,identity=0x6F,ok=1
SENSOR_CONFIG,accel=0x48,accel_ok=1,clock=0x95,clock_select_ok=1,fifo=0x01,int_source=0x08,int_config=0x03
SENSOR_CHECK,init=1,drdy=0,fifo_samples=0,clock_edges=0,int1_level=0,ok=0
READY,sensor=0,radio=1,clock_output=1,clock_measurement=wait_60_pps
```

SPI 识别、复位和关键配置读回正常；采样启动后没有 DRDY、FIFO 或板端时钟脉冲。对照与 Arduino 内核匹配的 ESP-IDF 4.4.7 GPIO 实现，gpio_set_direction 的输出使能会将输出矩阵选择改为 SIG_GPIO_OUT_IDX。原 SampleClock::begin 在 LEDC 绑定后配置 PCNT，再调用 gpio_set_direction(INPUT_OUTPUT)，因此覆盖 LEDC 输出路由。初始化 API 返回成功不等于仍有实际时钟输出。

原厂 IIM42652 DS-000440 Rev 1.3 第 79、98 页已提取并渲染核对：RTC_MODE=1 要求输入时钟，PIN9_FUNCTION=10 选择 CLKIN，现有对应寄存器配置保持不变。数据手册及下载页面脚本只保存在忽略的 tmp/pdfs 中。

参考原始资料：

- [ESP-IDF 4.4.7 GPIO 实现](https://github.com/espressif/esp-idf/blob/v4.4.7/components/driver/gpio.c)：gpio_output_enable、gpio_set_direction。
- [ESP-IDF 4.4.7 PCNT 实现](https://github.com/espressif/esp-idf/blob/v4.4.7/components/driver/pcnt.c)：_pcnt_set_pin。
- [TDK IIM42652 原厂数据手册下载入口](https://www.invensense.tdk.com/en-us/download-resource/ds-000440-iim-42652-datasheet)。

## 修改内容

1. 将时钟初始化顺序改为 PCNT 配置后再连接 LEDC，随后通过 GPIO 输入使能 HAL 保留同一引脚的计数输入，不再用会覆盖输出矩阵的 gpio_set_direction。
2. 增加传感器身份、复位后身份、配置读回及启动检查日志，区分识别失败和 DRDY/FIFO 无数据。
3. 启动自检期间记录板端原始时钟脉冲数，并在原自检完成、FIFO 清空之前读取一组有效性和 XYZ；不改变正式采集的 GNSS/PPS 门槛。
4. 适配固定 Arduino/ESP-IDF 版本可用的 GPIO HAL；中间编译兼容性错误已修正，最终构建通过。

## 实机验证

修复后同一接线三次连续 USB 复位，均成功：

| 次数 | WHO_AM_I | DRDY | FIFO 样本 | 约 120 ms 时钟脉冲 | XYZ 有效 | READY sensor/radio/clock_output | state |
|---|---|---:|---:|---:|---|---|---:|
| 1 | 0x6F | 12 | 12 | 3832 | 是 | 1/1/1 | 1 |
| 2 | 0x6F | 12 | 12 | 3832 | 是 | 1/1/1 | 1 |
| 3 | 0x6F | 12 | 12 | 3833 | 是 | 1/1/1 | 1 |

第一次修复后样例：

```text
SENSOR_SAMPLE,valid=1,x=5220,y=-2464,z=4108
SENSOR_CHECK,init=1,drdy=12,fifo_samples=12,clock_edges=3832,int1_level=0,ok=1
READY,sensor=1,radio=1,clock_output=1,clock_measurement=wait_60_pps
STATUS,id=1003,state=1,gps=0,clock_hz=0.0000,ppm=0.00,clock_ok=0,queue=0,lost_samples=0,lost_packets=0
```

板端原始计数只用于证明输出恢复，使用本地约 120 ms 窗口，不能替代 GNSS/PPS 校准或示波器测量。XYZ 为启动自检读取的一组原始值，不是静态精度、方向或连续 50 Hz 采样验收。室内 gps=0、clock_ok=0 仍属未完成同步验证，LoRa 仅完成初始化验证。

## 软件验证

- eora_s3_400_iim42652_tx：构建、COM8 烧录及 Flash 哈希验证通过。
- heltec_v32_iim42352_tx：代表另一板型/传感器的构建通过，未进行该组合实机验证。
- python -m unittest discover -s tests -v：32/32 通过。
- 原始日志验证：修复后三个启动片段均包含 sensor=1、有效 XYZ、非零 DRDY/FIFO/时钟计数和 state=1。
- 本轮未重新构建其余六个环境；历史八环境全量通过记录保留，独立于本轮结果。

## 文件变化

- firmware/common/clock/SampleClock.cpp：修复 PCNT 与 LEDC 引脚初始化顺序。
- firmware/common/clock/SampleClock.h：提供原始脉冲计数访问，用于启动诊断。
- firmware/common/sensor/IimBase.cpp：增加身份、复位和配置诊断。
- firmware/common/app/NodeApp.cpp：增加启动 DRDY/FIFO/时钟计数和一组 XYZ 诊断。
- PROJECT_PLAN.md、docs/hardware_acceptance.md：记录根因、修复、实测范围与下一步。
- docs/changes/2026-11-11_03_IIM42652分阶段诊断.md：本轮报告。
- docs/changes/2026-11-11_02_室内非GNSS硬件自检.md：已在基线历史保存，按规则从工作区移除。

## 接线、烧录与操作

- 接线和环境配置保持原样。诊断和修复均烧录至 COM8、同一块 EoRa、节点 1003，最后运行的是修复后的 tx 固件。
- 未发送 START_AT，没有启动正式采集，也没有绕过 GNSS/PPS 与时钟健康门槛。
- 检查结束释放串口。原始日志为 .pio/iim42652_diagnostic_boot.log，构建产物在 .pio，均被 Git 忽略；临时手册和串口辅助脚本在忽略的 tmp 中。
- 本轮暂不提交 Git；修复和报告可在工作区直接审阅。

## 下一步

在有 GNSS/PPS 的条件下完成连续 60 个有效 PPS 间隔测量和 ±100 ppm 验收，用示波器验证传感器 CLKIN 的实际频率与占空比；随后做单节点 50 Hz 连续采样、方向及 E870 无线联调。八节点 100 Hz 继续为待开始。

## 2026-11-13 ESP32时钟专项对照（进行中）

本轮保持接线，实际SampleClock.cpp原样编入诊断。GPIO36每本地1秒约32000上升沿、约50%高电平，PCNT约32001；暂停PCNT后GPIO38仍跟随，未复现历史LEDC路由丢失。XTAL/APB32k与XTAL40k均按设置输出；真高阻输出关闭时两脚无边沿，WHO仍0。输出38使能始终0，未由软件重复路由LEDC。

这不是外部精度或模块端波形验收，也未排除早启时钟的上电时序。矩阵结束曾恢复保护版并校验，随后追加CLOCK_COLD_READY诊断；**当前设备运行上电始终关闭时钟的临时程序，等待用户真正断电约10秒再上电后只读身份，完成后仍须恢复保护版。**

详见[本轮专项报告](diagnostics/2026-11-13_06_ESP32时钟专项检查.md)，原始数据在忽略目录tmp/node1003_clock_20261113。

## 2026-11-13 无时钟冷启动已完成

用户已在上电始终关闭时钟输出的CLOCK_COLD_READY下重新上电；首次REPORT输出使能为0、LEDC未配置，硬件及软件WHO仍0，一秒GPIO36/38均无边沿。本次识别失败不依赖应用先输出时钟；未证实时钟软件为根因，模块端模拟波形/电压仍待测。

**冷启动对照与恢复义务均已完成，当前已重新烧回并校验原保护版；sensor0/state5/queue0，异常IRQ关闭，临时诊断已退出。** 前节等待断电的描述为历史阶段。后续转模块端供电与SPI电平核查。

详见[本轮结果](diagnostics/2026-11-13_07_无时钟冷启动复查.md)，证据位于tmp/node1003_cold_result_20261113。
