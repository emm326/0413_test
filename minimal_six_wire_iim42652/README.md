# IIM-42652 六线最小身份测试

本工程只验证 IIM-42652 的 SPI 身份读取。程序不初始化 LoRa、GNSS、INT1 或 CLKIN，不写传感器寄存器，也不发送传感器软件复位。

## 六线接线

| YBX-IIM42652 模块 | EoRa-S3-400TB 排针 |
|---|---|
| VCC | 第 3 脚 `VDD`，3.3V |
| GND | 第 4 脚 GND |
| SCK | 第 20 脚 GPIO47 |
| MOSI | 第 19 脚 GPIO48 |
| MISO | 第 10 脚 GPIO39 |
| CSB | 第 9 脚 GPIO40 |

INT1、INT2/CLKIN、GNSS 全部断开。不要将 5V 接到传感器模块 VCC。

### 备用 CS 引脚

如果断电并拆下 CSB 线后，GPIO40 仍不能输出高电平，可确认 GPIO40 的板级问题。断电后把 CSB 改接 EoRa 第 23 脚 GPIO35，并使用：

```powershell
pio run -e eora_s3_400_iim42652_six_wire_alt_cs -t upload --upload-port COM8
```

## 构建、烧录和测试

在本目录执行：

```powershell
$env:PLATFORMIO_DISABLE_TELEMETRY="1"
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_six_wire
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" run -e eora_s3_400_iim42652_six_wire -t upload --upload-port COM8
& "$env:USERPROFILE\.platformio\penv\Scripts\pio.exe" device monitor -p COM8 -b 460800
```

启动后自动执行一次扫描。扫描前会分别把 CS、SCK、MOSI 拉低和拉高，并读回引脚电平；随后对 MISO 做上拉和下拉，判断线路是否处于浮空状态。串口输入 `SCAN` 可重新扫描，输入 `READ` 可执行一次 Mode0/100kHz 读取。

扫描顺序：

1. Mode0 / 100kHz
2. Mode0 / 1MHz
3. Mode3 / 100kHz
4. Mode3 / 1MHz

每个组合读取 8 次，只比较 `WHO_AM_I=0x6F`。

## 2026-11-15 更换传感器后的COM8复测

用户更换新的IIM42652模块并保持六线连接后，CS/SCK/MOSI低/高回读均正常，旧CS持续低电平问题已消失。MISO上拉和下拉均为1，空闲为1；四种SPI组合共32次WHO读取仍全部为0x00。

这表示ESP32侧输出链路正常，故障应在模块端供电、CSB/MISO/MOSI线路或传感器响应。先用万用表核查模块端3.3V和GND，再确认MISO/MOSI没有接反；不要改用GPIO35备用CS。完整结果见[本轮报告](../docs/changes/2026-11-15_03_更换传感器后的六线复测.md)，原始日志保留在本机`tmp/com8_new_sensor_six_wire_20261115.log`。

## 结果判断

六线正常时应出现任一组合：

```text
RESULT,mode=0,hz=100000,matches=8,reads=8,ok=1,values=6F 6F 6F 6F 6F 6F 6F 6F
SIX_WIRE_OK,who=0x6F,next=reconnect_INT1_and_CLKIN
```

身份链路仍未恢复时会出现：

```text
SIX_WIRE_FAIL,who!=0x6F,next=measure_3V3_CSB_MISO_at_module
```

此时在模块端测量 3.3V、GND、CSB、SCK、MOSI 和 MISO，不要继续修改采样配置。
