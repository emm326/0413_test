# 项目进度判断与后续优化建议

本文档根据 `PLAN.md`、当前源码、配置、文档和本项目目录中的数据文件整理。检查日期：2026-05-20。

## 1. 当前实现到了计划的哪一步

结论：当前项目已经完成了计划中的“设备选型、固件改造、电脑端脚本”这三块软件实现，并且已经进入“验证顺序”的现场验证阶段。更准确地说，现在是“验证顺序第 1 步准备就绪”，还需要用真实 E870-L470LG11 网关、真实 LoRaWAN 服务器参数和真实 TX1001 做现场联调。

已完成的软件能力：

- 已选定并记录 `E870-L470LG11`，项目方向已切到 CN470 LoRaWAN 网关路线。
- 保留原始 P2P 路线：`heltec_wifi_lora_32_V3_tx` / `heltec_wifi_lora_32_V3_rx`。
- 新增 LoRaWAN 路线：`heltec_wifi_lora_32_V3_lorawan_tx_1001` 到 `heltec_wifi_lora_32_V3_lorawan_tx_1004`。
- TX 端保留 ADXL345 采样、FIFO、OLED、按键切换采样率和二进制 payload。
- LoRaWAN TX 使用 RadioLib `LoRaWANNode`、`CN470`、`ABP`、`unconfirmed uplink`、FPort `10`。
- 电脑端已新增 `tools/lorawan_gateway_to_csv.py`，支持 stdin、TCP、HTTP POST、MQTT 输入。
- LoRaWAN 脚本可以解析 base64/hex payload，按 `dev_id` 分文件保存 CSV，并输出 `quality.csv`。
- 2026-05-20 已重新编译验证 6 个环境全部通过：
  - `heltec_wifi_lora_32_V3_rx`
  - `heltec_wifi_lora_32_V3_tx`
  - `heltec_wifi_lora_32_V3_lorawan_tx_1001`
  - `heltec_wifi_lora_32_V3_lorawan_tx_1002`
  - `heltec_wifi_lora_32_V3_lorawan_tx_1003`
  - `heltec_wifi_lora_32_V3_lorawan_tx_1004`

当前还没有从项目文件中看到足够证据证明真实 E870-L470LG11 网关已经完成 LoRaWAN 现场闭环。`data` 目录里有 `sensor1001` CSV，也有 LoRaWAN smoke test 输出，但没有看到根目录 `data/quality.csv` 或 4 个 LoRaWAN 节点的现场质量日志。因此不要把当前状态理解成“4 个节点已经稳定跑通”，而应该理解成“软件已准备好，下一步该做现场验证”。

## 2. 对照原计划的完成度

### 2.1 设备选择

状态：已完成。

项目文档已明确推荐 `E870-L470LG11`，并排除了 868 MHz 和 915 MHz 型号。

### 2.2 重要前提

状态：已完成。

代码和文档已经区分了两条路线：

- E870-L470LG11 使用 LoRaWAN TX 固件。
- 原来的 RadioLib P2P RX 只作为旧路线保留。

### 2.3 固件改造

状态：软件实现已完成，真实密钥和现场烧录待做。

已完成：

- `src/lorawan_transport.cpp` 已实现 CN470 ABP LoRaWAN 初始化与上行发送。
- `platformio.ini` 已有 TX1001 到 TX1004 的独立环境。
- payload 仍使用二进制振动帧，没有改成 CSV 字符串。
- ADXL345、FIFO、OLED、按键采样率切换逻辑仍保留。

待做：

- 把 `platformio.ini` 里的占位 DevAddr / Key 替换为 LoRaWAN 服务器中的真实参数。
- 烧录 TX1001 并用真实网关确认上行。
- 确认 E870-L470LG11 的 CN470 子频段与 `APP_LORAWAN_SUB_BAND_VALUE=1` 匹配。

### 2.4 电脑端脚本

状态：已完成软件实现，真实网关数据待验证。

已完成：

- `tools/lorawan_gateway_to_csv.py` 已存在。
- 支持 MQTT、HTTP、TCP、stdin。
- 支持 payload 解析、CRC 校验、按设备分文件、raw 转 g。
- 支持 `quality.csv`，记录 `seq`、`rssi`、`snr`、`fcnt`、`lost_packets` 等字段。

待做：

- 用 E870-L470LG11 或 LoRaWAN 服务器真实 JSON 验证字段路径。
- 确认 MQTT topic、HTTP 地址或 TCP 输出格式。

### 2.5 验证顺序

状态：第 1 步准备就绪，但现场验证还没完成。

下一步建议严格按原计划做：

1. 只接 `TX1001`，确认 E870-L470LG11 能收到 LoRaWAN 上行。
2. 启动 `tools/lorawan_gateway_to_csv.py`，确认生成 `data/sensor1001/...csv` 和 `data/quality.csv`。
3. 逐个加入 `TX1002`、`TX1003`、`TX1004`。
4. 先测 `25 Hz / 50 Hz / 100 Hz`。
5. 最后测 `200 Hz`，并记录 `seq` 连续性、RSSI、SNR 和丢包率。

## 3. 第一优先级优化

### 3.1 采样与 LoRaWAN 发送解耦

当前 TX 在凑满一包后直接调用 LoRaWAN `sendUplink()`。LoRaWAN Class A 上行后可能等待接收窗口，发送过程会阻塞主循环。采样率越高，ADXL345 FIFO 越容易在发送期间堆积甚至溢出。

建议：

- 增加 RAM 环形缓冲区。
- 采样侧只负责快速读取 ADXL345 FIFO 并写入缓冲区。
- 发送侧从缓冲区批量取样、编码、LoRaWAN 上行。
- 在串口日志和质量日志里记录 `fifo_overload`、`buffer_overflow`、`dropped_samples`。

这项最影响 100 Hz / 200 Hz 时的数据连续性，建议优先做。

### 3.2 ABP 帧计数持久化或切换 OTAA

当前使用 ABP 方便调试，但节点重启后 frame counter 可能回到初始值。如果服务器启用了 frame counter check，可能出现网关能看到 RF 包、服务器却拒收的情况。

建议：

- 调试阶段可以继续 ABP。
- 正式测试优先评估 OTAA。
- 如果继续 ABP，把 frame counter 存入 ESP32 NVS。
- 不建议长期依赖关闭服务器 frame counter check。

### 3.3 LoRaWAN 真实密钥从 `platformio.ini` 分离

当前 `platformio.ini` 中是占位 key。后续换成真实 key 后，不建议直接写进仓库。

建议：

- 新增 `lorawan_keys.example.ini` 作为模板。
- 新增本地文件 `lorawan_keys.local.ini` 保存真实参数。
- 把 `lorawan_keys.local.ini` 加入 `.gitignore`。
- 文档说明如何复制模板并填写 DevAddr / NwkSKey / AppSKey。

### 3.4 确认 LoRaWAN payload 长度安全余量

当前 LoRaWAN 环境设置 `APP_MAX_SAMPLES_PER_PACKET_VALUE=37`。这会提高吞吐效率，但真实最大可用应用 payload 受区域、DR、服务器和网关配置影响。

建议：

- 现场先用当前 DR5 和 37 样本包验证。
- 如果出现 `packet too long`、服务器丢包或网关不转发，先降到 20 或 24 样本每包。
- 在文档里记录每个采样率下的 payload 长度、发送间隔和丢包率。

### 3.5 LoRaWAN 子频段配置工程化

当前固定 `APP_LORAWAN_SUB_BAND_VALUE=1`。如果 E870-L470LG11 网关实际监听的 CN470 信道组不同，TX 会显示发送成功，但网关可能完全收不到。

建议新增测试环境，例如：

```text
heltec_wifi_lora_32_V3_lorawan_tx_1001_sb1
heltec_wifi_lora_32_V3_lorawan_tx_1001_sb2
heltec_wifi_lora_32_V3_lorawan_tx_1001_sb3
```

这样现场排查时不用反复手改配置。

## 4. 第二优先级优化

### 4.1 网关脚本增加在线状态和告警

建议给 `tools/lorawan_gateway_to_csv.py` 增加：

- `--require-devices 1001,1002,1003,1004`
- `--offline-timeout 10`
- 每分钟输出一次各设备最近包时间、丢包数、RSSI、SNR。
- 某设备超时未收到数据时报警。
- 丢包率超过阈值时报警。
- MQTT 断线重连时输出明确日志。

### 4.2 同时保存 raw 和 g 数据

当前主 CSV 保存转换后的 g 值：

```text
datetime,ax,ay,az
```

建议增加可选格式：

```text
datetime,ax_g,ay_g,az_g,ax_raw,ay_raw,az_raw
```

这样后续标定、复算和排查更方便。

### 4.3 增强质量日志

当前质量日志已经有关键字段。建议继续增加：

- `gateway_id`
- `channel`
- `spreading_factor`
- `bandwidth`
- `time_on_air`
- `fifo_overload`
- `buffer_overflow`
- `dropped_samples`

这些字段可以帮助判断问题来自传感器读取、TX 缓冲、LoRa 空口、网关还是服务器。

### 4.4 多节点时间同步

当前电脑端按每个设备首包时间建立时间锚点，适合普通采集和趋势分析。如果后续要做严格相位分析或振型分析，时间同步需要加强。

建议：

- 普通监测：保留当前方案。
- 需要相位一致性：增加硬件同步触发线。
- 更高要求：使用 GPS PPS、统一 RTC 或专门的同步信号。
- 在质量日志中增加时间漂移估计。

## 5. 第三优先级优化

### 5.1 ADXL345 标定文件

当前 raw 到 g 的换算默认使用 `0.0039`。不同 ADXL345 模块可能有零偏、比例误差和安装方向差异。

建议新增：

```text
calibration/sensor1001.json
calibration/sensor1002.json
calibration/sensor1003.json
calibration/sensor1004.json
```

每个文件记录零偏、比例系数、坐标方向、标定日期和备注。

### 5.2 自动化测试

建议给 `tools/lorawan_gateway_to_csv.py` 增加测试样例，覆盖：

- 原始 base64 payload。
- ChirpStack uplink JSON。
- E870-L470LG11 或其服务器导出的真实 JSON。
- CRC 错误 payload。
- seq 丢包统计。
- 多设备同时写入。
- 跨天和跨 30 分钟分文件。

### 5.3 现场测试记录模板

建议新增 `docs/field_test_record_template.md`，每次测试记录：

- 日期、地点、网关型号、天线、供电方式。
- TX 编号、DevAddr、采样率、安装位置。
- CN470 子频段、DR、FPort、服务器类型。
- 每个采样率下运行时长、总包数、丢包数、RSSI、SNR。
- 是否生成完整 CSV 和 `quality.csv`。

## 6. 推荐下一步行动

建议下一次实际操作按这个顺序推进：

1. 把 TX1001 的真实 LoRaWAN ABP 参数填入本地配置。
2. 烧录 `heltec_wifi_lora_32_V3_lorawan_tx_1001`。
3. 配置 E870-L470LG11 为 CN470，并确认子频段。
4. 在服务器注册 TX1001，确认后台能看到 `DevAddr 26011001` 上行。
5. 启动 `tools/lorawan_gateway_to_csv.py`，确认生成 `data/sensor1001` 和 `data/quality.csv`。
6. 先做 25 Hz、50 Hz、100 Hz，每档至少连续跑 10 到 30 分钟。
7. `quality.csv` 丢包可接受后，再逐个加入 TX1002 到 TX1004。
8. 最后才测试 200 Hz，并把它当作实验档位。

