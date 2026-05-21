# 项目当前功能与使用流程

本文档记录当前工程已经实现的功能，以及从设备烧录、网关配置到电脑端接收 CSV 数据的完整使用流程。当前日期：2026-05-18。

## 1. 项目定位

本项目是一个基于 `HTIT-WB32LA / Heltec WiFi LoRa 32 V3` 的无线振动采集系统。

硬件组成：

- 主控与无线：ESP32-S3 + SX1262
- 传感器：ADXL345 三轴加速度传感器
- 传感器接口：I2C
- SDA：GPIO41
- SCL：GPIO42
- 可选中断 INT1：GPIO3
- ADXL345 默认 I2C 地址：`0x53`

当前工程同时保留两条通信路线：

- 原始 P2P 路线：`TX -> RX -> 电脑串口 -> CSV`
- 新增 LoRaWAN 网关路线：`TX -> E870-L470LG11 网关 -> LoRaWAN 服务器 -> 电脑脚本 -> CSV`

后续如果使用亿佰特 `E870-L470LG11`，应该使用 LoRaWAN 路线。

## 2. 当前已经实现的功能

### 2.1 TX 发送端功能

TX 端已经实现：

- ADXL345 I2C 初始化
- 读取 ADXL345 设备 ID，正常应为 `0xE5`
- ADXL345 FIFO 采样
- 三轴原始数据读取：`ax_raw, ay_raw, az_raw`
- OLED 状态显示
- 板载按键切换采样率
- 支持采样率：`25 Hz / 50 Hz / 100 Hz / 200 Hz / 400 Hz`
- 默认采样率：`100 Hz`
- 按包打包多条样本
- 保留原始 P2P LoRa 发送
- 新增 LoRaWAN Class A ABP 上行发送

### 2.2 原始 P2P RX 接收端功能

P2P RX 端已经实现：

- 接收 SX1262 P2P LoRa 数据包
- 校验内部 CRC16
- 解析二进制振动数据包
- 根据 `t0_ms + i * sample_period` 重建样本时间
- 串口输出：

```text
dev_id,timestamp_ms,ax_raw,ay_raw,az_raw
```

示例：

```text
1001,250651,116,-109,213
```

### 2.3 LoRaWAN 网关路线功能

LoRaWAN 路线已经实现：

- 使用 RadioLib `LoRaWANNode`
- 区域：`CN470`
- 激活方式：`ABP`
- 上行类型：`unconfirmed uplink`
- 默认 LoRaWAN FPort：`10`
- 默认 DataRate：`DR5`
- 默认 Sub-band：`1`
- 每个节点独立配置：
  - `dev_id`
  - `DevAddr`
  - `FNwkSIntKey`
  - `SNwkSIntKey`
  - `NwkSEncKey`
  - `AppSKey`
- payload 继续使用二进制振动包，不发送 CSV 字符串

当前已经固化 4 个 LoRaWAN TX 环境：

```text
heltec_wifi_lora_32_V3_lorawan_tx_1001
heltec_wifi_lora_32_V3_lorawan_tx_1002
heltec_wifi_lora_32_V3_lorawan_tx_1003
heltec_wifi_lora_32_V3_lorawan_tx_1004
```

同时原始 P2P 环境仍然保留：

```text
heltec_wifi_lora_32_V3_tx
heltec_wifi_lora_32_V3_rx
```

### 2.4 电脑端 CSV 工具

当前有三类电脑端工具。

P2P 串口转 CSV：

```text
tools/serial_rx_to_file.py
```

P2P 串口采集并上传 HTTP：

```text
tools/serial_gateway_csv_http.py
```

LoRaWAN 网关数据转 CSV：

```text
tools/lorawan_gateway_to_csv.py
```

LoRaWAN 脚本支持：

- stdin 输入
- TCP 输入
- HTTP POST 输入
- MQTT 输入
- 自动解析 LoRaWAN 服务器输出的 base64/hex payload
- 按 `dev_id` 分文件夹保存
- 自动将 raw 值换算成 g，默认系数为 `0.0039`
- 默认 30 分钟切分 CSV
- 输出质量日志 `quality.csv`

CSV 输出格式：

```text
datetime,ax,ay,az
2026/4/18 19:52:21.5126,0.631800,0.058500,0.838500
```

质量日志包含：

```text
received_at,dev_id,seq,count,fs_hz,rssi,snr,freq,fcnt,lost_packets,payload_len
```

## 3. 当前代码文件说明

主要配置文件：

```text
platformio.ini
```

核心固件：

```text
src/main.cpp
src/protocol.cpp
src/lora_transport.cpp
src/lorawan_transport.cpp
```

核心头文件：

```text
include/config.h
include/protocol.h
include/lora_transport.h
include/lorawan_transport.h
include/board_pins.h
```

电脑端脚本：

```text
tools/serial_rx_to_file.py
tools/serial_gateway_csv_http.py
tools/lorawan_gateway_to_csv.py
```

网关说明文档：

```text
docs/ebyte_lorawan_gateway_setup.md
```

本文档：

```text
docs/project_current_features_and_usage.md
```

## 4. 编译验证状态

当前工程已经验证过以下环境均可编译通过：

```text
heltec_wifi_lora_32_V3_rx
heltec_wifi_lora_32_V3_tx
heltec_wifi_lora_32_V3_lorawan_tx_1001
heltec_wifi_lora_32_V3_lorawan_tx_1002
heltec_wifi_lora_32_V3_lorawan_tx_1003
heltec_wifi_lora_32_V3_lorawan_tx_1004
```

验证命令：

```powershell
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1001 -e heltec_wifi_lora_32_V3_lorawan_tx_1002 -e heltec_wifi_lora_32_V3_lorawan_tx_1003 -e heltec_wifi_lora_32_V3_lorawan_tx_1004 -e heltec_wifi_lora_32_V3_tx -e heltec_wifi_lora_32_V3_rx
```

Python 脚本语法检查命令：

```powershell
python -m py_compile tools\lorawan_gateway_to_csv.py
```

## 5. 使用路线选择

### 5.1 如果继续使用原来的 P2P 接收方式

使用：

```text
TX 板子：heltec_wifi_lora_32_V3_tx
RX 板子：heltec_wifi_lora_32_V3_rx
电脑脚本：tools/serial_rx_to_file.py
```

这种方式适合当前点对点调试。

### 5.2 如果使用亿佰特 E870-L470LG11 网关

使用：

```text
TX 板子：heltec_wifi_lora_32_V3_lorawan_tx_1001 到 1004
网关：E870-L470LG11
电脑脚本：tools/lorawan_gateway_to_csv.py
```

注意：E870-L470LG11 是 LoRaWAN 网关，不能直接接收旧的 P2P 包。使用这个网关时，TX 必须烧录 LoRaWAN 环境。

## 6. LoRaWAN 路线完整使用流程

### 6.0 一页式快速流程

现场第一次使用时，推荐只按下面顺序做，不要一开始同时打开多个节点：

1. 只准备 `TX1001`、E870-L470LG11 网关和电脑。
2. 在 `platformio.ini` 里把 `TX1001` 的 `DevAddr / NwkSKey / AppSKey` 改成 LoRaWAN 服务器里的真实参数。
3. 烧录 `TX1001`：

```powershell
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1001 -t upload --upload-port COM7
```

4. 配置 E870-L470LG11：

```text
Region: CN470
Sub-band: 先使用 1
Activation: ABP
FPort: 10
Uplink: unconfirmed
```

5. 在 LoRaWAN 服务器里注册 `TX1001`：

```text
DevAddr: 26011001
NwkSKey: 与 platformio.ini 一致
AppSKey: 与 platformio.ini 一致
```

6. 先确认网关/服务器后台能看到 `DevAddr 26011001` 上行。
7. 再启动电脑端脚本，例如 MQTT：

```powershell
python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-topic "application/+/device/+/event/up" --output data
```

8. 确认生成：

```text
data/sensor1001/...
data/quality.csv
```

9. `TX1001` 稳定后，再逐个加入 `TX1002`、`TX1003`、`TX1004`。

### 6.0.1 ABP Key 填写模板

如果 LoRaWAN 服务器是传统 LoRaWAN 1.0 ABP，只给出 `DevAddr / NwkSKey / AppSKey`，在当前固件里按下面方式填写：

```text
APP_LORAWAN_DEV_ADDR_VALUE      = DevAddr
APP_LORAWAN_FNWKSINT_KEY_VALUE  = NwkSKey
APP_LORAWAN_SNWKSINT_KEY_VALUE  = NwkSKey
APP_LORAWAN_NWKSENC_KEY_VALUE   = NwkSKey
APP_LORAWAN_APPS_KEY_VALUE      = AppSKey
```

示例格式：

```text
-D APP_LORAWAN_DEV_ADDR_VALUE=0x26011001UL
-D APP_LORAWAN_FNWKSINT_KEY_VALUE=0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,0x10
-D APP_LORAWAN_SNWKSINT_KEY_VALUE=0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,0x10
-D APP_LORAWAN_NWKSENC_KEY_VALUE=0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,0x10
-D APP_LORAWAN_APPS_KEY_VALUE=0x10,0x0F,0x0E,0x0D,0x0C,0x0B,0x0A,0x09,0x08,0x07,0x06,0x05,0x04,0x03,0x02,0x01
```

注意：上面只是格式示例，不是正式密钥。

### 第一步：准备硬件

准备：

- E870-L470LG11 网关
- LoRa 天线
- 8V 到 28V 电源
- 网线或可用的网络连接
- 至少 1 块 HTIT-WB32LA 作为 TX
- ADXL345 传感器
- 电脑一台

先只测试 `TX1001`，不要一开始同时打开 4 个节点。

### 第二步：配置 TX1001 的 ABP 参数

打开：

```text
platformio.ini
```

找到：

```text
[env:heltec_wifi_lora_32_V3_lorawan_tx_1001]
```

重点参数：

```text
APP_DEVICE_ID_VALUE=1001
APP_LORAWAN_DEV_ADDR_VALUE=0x26011001UL
APP_LORAWAN_FNWKSINT_KEY_VALUE=...
APP_LORAWAN_SNWKSINT_KEY_VALUE=...
APP_LORAWAN_NWKSENC_KEY_VALUE=...
APP_LORAWAN_APPS_KEY_VALUE=...
```

当前 key 是占位值，现场必须改成 LoRaWAN 服务器里注册的真实 key。

如果你的服务器只提供传统 LoRaWAN 1.0 ABP 参数：

```text
DevAddr
NwkSKey
AppSKey
```

可以先这样对应：

```text
APP_LORAWAN_DEV_ADDR_VALUE      = DevAddr
APP_LORAWAN_FNWKSINT_KEY_VALUE  = NwkSKey
APP_LORAWAN_SNWKSINT_KEY_VALUE  = NwkSKey
APP_LORAWAN_NWKSENC_KEY_VALUE   = NwkSKey
APP_LORAWAN_APPS_KEY_VALUE      = AppSKey
```

key 必须是 16 字节，例如：

```text
0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,0x10
```

### 第三步：烧录 TX1001

连接 TX1001 到电脑，查看实际 COM 口。

假设 TX1001 是 `COM7`，烧录：

```powershell
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1001 -t upload --upload-port COM7
```

打开串口监视：

```powershell
pio device monitor -p COM7 -b 115200
```

正常时应看到类似：

```text
ROLE=TX_LORAWAN
LoRaWAN init ok
TX LoRaWAN uplink ...
```

### 第四步：配置 E870-L470LG11 网关

网关侧需要完成：

```text
区域/频段：CN470
网关类型：LoRaWAN Gateway
信道组/Sub-band：尽量先匹配代码里的 APP_LORAWAN_SUB_BAND_VALUE=1
服务器：填写 LoRaWAN 网络服务器地址
NTP/时间同步：建议开启
```

如果网关有内置 LoRaWAN Server，可以直接在网关后台注册设备。

如果网关只是 Packet Forwarder，则需要配合 LoRaWAN 网络服务器，例如 ChirpStack。此时网关负责收包，ChirpStack 负责解密和转发 payload。

### 第五步：在 LoRaWAN 服务器注册设备

先只注册 `TX1001`：

```text
Device name: TX1001
Activation: ABP
Region: CN470
DevAddr: 26011001
FPort: 10
Uplink type: unconfirmed
NwkSKey: 与 platformio.ini 一致
AppSKey: 与 platformio.ini 一致
```

如果服务器要求 `DevEUI`，可以自己设置一个唯一值，例如：

```text
TX1001 DevEUI: 70B3D57ED0001001
TX1002 DevEUI: 70B3D57ED0001002
TX1003 DevEUI: 70B3D57ED0001003
TX1004 DevEUI: 70B3D57ED0001004
```

ABP 调试阶段建议注意帧计数器问题。如果节点重启后服务器提示 `FCnt too low` 或直接丢包，可以在服务器里关闭 frame counter check，或重置该设备的 frame counter。

### 第六步：确认服务器能看到上行

打开网关或 LoRaWAN 服务器后台，确认能看到：

```text
DevAddr: 26011001
FPort: 10
Uplink frame counter 增加
RSSI/SNR 有数值
```

如果服务器能看到上行，说明 TX 到网关、网关到服务器这段基本通了。

### 第七步：启动电脑端 CSV 脚本

LoRaWAN 数据转 CSV 脚本：

```text
tools/lorawan_gateway_to_csv.py
```

查看常用命令模板：

```powershell
python tools\lorawan_gateway_to_csv.py --example
```

#### MQTT 模式

如果 LoRaWAN 服务器通过 MQTT 发布上行数据，先安装 MQTT 依赖：

```powershell
python -m pip install paho-mqtt
```

启动脚本：

```powershell
python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-topic "application/+/device/+/event/up" --output data
```

把 `192.168.1.10` 改成你的 MQTT Broker 或 LoRaWAN 服务器 IP。

如果 MQTT 有用户名密码：

```powershell
python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-username your_user --mqtt-password your_password --mqtt-topic "application/+/device/+/event/up" --output data
```

#### HTTP 模式

如果 LoRaWAN 服务器可以 HTTP POST 到电脑：

```powershell
python tools\lorawan_gateway_to_csv.py --http --http-port 8080 --http-path /uplink --output data
```

服务器 HTTP 集成地址填写：

```text
http://电脑IP:8080/uplink
```

#### TCP 模式

如果网关或服务器可以把 JSON 按行发到 TCP：

```powershell
python tools\lorawan_gateway_to_csv.py --tcp-port 9000 --output data
```

#### stdin 测试模式

用于手动粘贴 base64/hex payload 测试解析：

```powershell
python tools\lorawan_gateway_to_csv.py --stdin --output data
```

### 第八步：查看输出文件

默认输出目录：

```text
data
```

设备数据文件：

```text
data/sensor1001/YYYYMMDD/HHMMSS_to_HHMMSS.csv
data/sensor1002/YYYYMMDD/HHMMSS_to_HHMMSS.csv
```

CSV 内容：

```text
datetime,ax,ay,az
2026/4/18 19:52:21.5126,0.631800,0.058500,0.838500
```

质量日志：

```text
data/quality.csv
```

质量日志用于检查：

- `seq` 是否连续
- `lost_packets` 是否增加
- `rssi` 是否过低
- `snr` 是否过低
- `payload_len` 是否正常
- `fs_hz` 是否符合当前采样率

### 第九步：加入 TX1002 到 TX1004

TX1001 跑通后，再逐个加入其他节点。

烧录：

```powershell
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1002 -t upload --upload-port COM8
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1003 -t upload --upload-port COM9
pio run -e heltec_wifi_lora_32_V3_lorawan_tx_1004 -t upload --upload-port COM10
```

同时在服务器注册：

```text
TX1002 DevAddr: 26011002
TX1003 DevAddr: 26011003
TX1004 DevAddr: 26011004
```

最终应看到：

```text
data/sensor1001/...
data/sensor1002/...
data/sensor1003/...
data/sensor1004/...
```

## 7. 原始 P2P 路线使用流程

如果不使用亿佰特网关，而是继续使用一块 HTIT-WB32LA 作为 RX：

### 第一步：烧录 P2P RX

```powershell
pio run -e heltec_wifi_lora_32_V3_rx -t upload --upload-port COM6
```

### 第二步：烧录 P2P TX

```powershell
pio run -e heltec_wifi_lora_32_V3_tx -t upload --upload-port COM7
```

### 第三步：运行串口转 CSV

安装串口依赖：

```powershell
python -m pip install pyserial
```

命令行模式：

```powershell
python tools\serial_rx_to_file.py --port COM6 --output data --rotate-minutes 30 --header
```

GUI 模式：

```powershell
python tools\serial_rx_to_file.py --gui
```

P2P 路线输出也会按设备号分文件。

## 8. 推荐测试顺序

推荐按这个顺序测试：

1. 只测试 `TX1001`
2. 网关后台确认收到上行
3. 电脑脚本确认生成 `sensor1001` CSV
4. 测试 `25 Hz`
5. 测试 `50 Hz`
6. 测试 `100 Hz`
7. 加入 `TX1002`
8. 加入 `TX1003`
9. 加入 `TX1004`
10. 最后测试 `200 Hz`

`200 Hz` 不建议一开始就作为稳定生产档位。4 个节点同时 200 Hz 三轴原始数据对 LoRaWAN 空口压力很大，必须根据 `quality.csv` 的 `lost_packets` 和 `seq` 连续性判断是否可用。

## 9. 常见问题排查

### 9.1 TX 串口显示 LoRaWAN init failed

检查：

- 板子型号是否是 Heltec WiFi LoRa 32 V3 / HTIT-WB32LA
- SX1262 引脚是否和 `include/board_pins.h` 一致
- RadioLib 版本是否为 `7.6.0`
- 是否烧录了 LoRaWAN 环境，而不是 P2P 环境

### 9.2 网关看不到任何上行

检查：

- 天线是否接好
- 网关是否是 `E870-L470LG11`
- 网关区域是否为 `CN470`
- 网关信道组是否匹配 `APP_LORAWAN_SUB_BAND_VALUE=1`
- TX 是否已经正常启动并显示 uplink

快速判断：

```text
TX 串口没有 TX LoRaWAN uplink -> 先查 TX 固件、ADXL345、LoRaWAN 初始化
TX 串口有 uplink，网关没有记录 -> 查 CN470、Sub-band、天线、距离
网关有 RF 记录，服务器没有设备上行 -> 查网关到服务器的转发配置
```

### 9.3 网关能看到包，但服务器解不开

检查：

- `DevAddr` 是否一致
- `NwkSKey` 是否一致
- `AppSKey` 是否一致
- ABP frame counter 是否被服务器拒绝
- FPort 是否为 `10`

快速判断：

```text
DevAddr 不一致 -> 服务器无法匹配设备
NwkSKey/AppSKey 不一致 -> 服务器能看到上行但解密失败
FCnt too low -> ABP 节点重启后帧计数器被服务器拒绝
```

### 9.4 服务器能看到数据，但电脑没有 CSV

检查：

- MQTT topic 是否匹配
- HTTP 地址是否写成电脑真实 IP
- Windows 防火墙是否放行端口
- `lorawan_gateway_to_csv.py` 是否正在运行
- 服务器输出里是否包含解密后的应用 payload，例如 `frm_payload`

快速判断：

```text
脚本启动后没有任何 packet 日志 -> 查 MQTT topic / HTTP URL / TCP 地址
脚本提示 no payload field found -> 服务器转发 JSON 里没有应用 payload 字段
脚本提示 crc mismatch -> payload 被服务器二次解析或转码，需要使用原始 frm_payload
```

### 9.5 CSV 有数据但丢包

检查：

- `data/quality.csv` 里的 `lost_packets`
- `RSSI`
- `SNR`
- 多节点是否同时发送
- 采样率是否过高

建议先降到 `25 Hz` 或 `50 Hz` 验证稳定性。

快速判断：

```text
lost_packets 持续增加 -> 空口或服务器链路有丢包
RSSI 很低 -> 距离、天线、安装位置需要调整
SNR 长期为负且波动大 -> 链路裕量不足，先降低采样率
只有 200 Hz 丢包 -> 属于预期风险，先使用 25/50/100 Hz
```

## 10. 后续可继续优化的方向

详细进度判断和优化路线已经整理到：

```text
docs/project_optimization_suggestions.md
```

当前阶段可以理解为：LoRaWAN 软件改造已经完成并编译通过，下一步进入真实 E870-L470LG11 网关现场验证。后续优先优化：

- 采样与 LoRaWAN 发送解耦，避免上行阻塞导致 ADXL345 FIFO 溢出
- ABP frame counter 持久化，或切换 OTAA 入网模式
- 将真实 LoRaWAN 密钥从 `platformio.ini` 分离到本地配置文件
- 确认 `APP_MAX_SAMPLES_PER_PACKET_VALUE=37` 在现场 DR5 / CN470 配置下有足够余量
- 根据 E870-L470LG11 实际信道计划工程化配置 `APP_LORAWAN_SUB_BAND_VALUE`
- 给 `tools/lorawan_gateway_to_csv.py` 增加设备在线状态、丢包率告警和 MQTT 重连日志
- 在 CSV 中可选同时写入 raw 和 g 数据
- 增加更严格的多节点时间同步
- 增加 ADXL345 标定文件
- 增加自动化实测脚本，用于统计不同采样率下的丢包率
