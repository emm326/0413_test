# 09-20260709 版本的传感器同步原理

本文说明当前 `backup/09-20260709` 分支中的 IIM-42352 + GPS/PPS + DS3231 同步方式。

结论先说：你画的这种情况是符合当前代码逻辑的：

```text
第2包：
样本0    样本1    样本2    样本3    样本4
0.9375s  0.9570s  0.9766s  0.9961s  1.0156s
                                ▲
                              PPS 1.0000s

S_k                  GPS 1PPS                 S_k+1
●-----------------------▲------------------------●
t_k                  T_pps                  t_k+1
```

当前版本并不是让 IIM 的每一个采样沿都落在 PPS 上，也不是每秒停采重新对齐。当前做法是：

1. IIM-42352 用外部 32.768 kHz 时钟产生稳定采样。
2. GPS PPS 给 ESP32 一个绝对 UTC 秒边界。
3. ESP32 记录 PPS 到来的本地时间。
4. IIM INT1 到来时记录采样沿的本地 `micros()` 时间。
5. 程序把每个样本的本地时间换算成 UTC epoch 微秒。
6. 两个节点后续用 UTC 时间戳比较和对齐。

所以 PPS 可以位于两个样本之间。只要每个样本的时间戳可信，就可以知道这个样本是在 PPS 前还是 PPS 后采到的。

## 硬件时间源

当前接线和代码角色如下：

- GPS LC76G PPS -> ESP32 GPIO4。
- GPS NMEA 串口 -> ESP32 UART。
- DS3231 32K -> IIM-42352 CLKIN。
- IIM-42352 INT1 -> ESP32 GPIO26。
- IIM-42352 FIFO -> ESP32 通过 SPI 读取。

这里有两个时间概念：

- 绝对时间：GPS RMC + PPS，给出 UTC 秒。
- 采样节拍：DS3231 32K 驱动 IIM-42352，让 IIM 自己连续采样。

## 代码路径

### 1. GPS/PPS 建立 UTC 时间基准

`lib/GpsTimebase/src/GpsTimebase.cpp`

- PPS ISR 使用 `esp_timer_get_time()` 捕获 PPS 本地时间。
- NMEA RMC 提供 UTC 日期和时分秒。
- 当前 09 版本把 RMC 秒映射为下一次 PPS 秒：

```cpp
const uint64_t nmeaSec = lastNmeaEpochUs_ / 1000000ULL;
const uint64_t candidatePpsEpochUs = (nmeaSec + 1ULL) * 1000000ULL;
```

得到映射关系：

```text
本地时间 lastMappedPpsLocalUs64_  <->  UTC 时间 lastPpsEpochUs_
```

之后任意本地采样时间都可以换算：

```text
sample_epoch_us = lastPpsEpochUs + (sample_local_us - lastMappedPpsLocalUs64_)
```

### 2. IIM-42352 连续采样

`lib/IIM42352Driver/src/IIM42352Driver.cpp`

初始化时会配置：

- RTC/CLKIN 模式；
- INT1 data-ready；
- FIFO；
- 加速度量程；
- ODR。

关键点：IIM 是连续采样，不是 PPS 来一次就重启一次。

### 3. INT1 和 FIFO 样本建立本地时间

`src/main.cpp`

INT1 中断记录本地 `micros()`：

```cpp
void IRAM_ATTR onIimInt1() {
  const uint32_t nowUs = micros();
  ...
  iimInt1Timestamps[tail] = nowUs;
}
```

读取 FIFO 时，程序用最新 INT1 边沿作为锚点，然后按实际采样间隔反推 FIFO 里每个样本的时间。

对于配置 `50 Hz` 档，IIM 实际采样率是 `51.2 Hz`：

```text
period = 1 / 51.2 Hz = 19.53125 ms
```

所以你看到的采样时间大概是：

```text
... 0.9375, 0.9570, 0.9766, 0.9961, 1.0156 ...
```

这正是 51.2 Hz 采样网格。

### 4. START_AT 只负责启动协调

USB GPS sync 测试模式下，PC 发：

```text
START_AT epoch_us sample_rate_hz delay_ms range_g
```

固件接收后用 GPS 时间基准把 `epoch_us` 转成本地启动时刻：

```cpp
gpsTimebase.localMicrosForEpoch(startEpochUs, startLocalUs)
```

到了本地启动时刻后开始采集。这个动作的作用是让两个节点尽量在同一个 GPS 时间附近开始，不代表每个节点的第一个 IIM 采样沿一定完全相同。

### 5. 输出数据

USB 测试模式输出格式是：

```text
TXH,dev_id,seq,t0_offset_us,fs_x10,count
dev_id,epoch_us,ax_raw,ay_raw,az_raw
TXG,dev_id,pps_epoch_us,pps_local_us,pps_interval_us,age_ms
TIMEBASE,dev_id,healthy,pps_epoch_us,pps_local_us64,pps_interval_us,age_ms,pps_bad_count,local_timer_jump_count
```

加速度行里的 `epoch_us` 已经是换算后的 UTC 微秒时间。

## 时间图

SVG 图见：

```text
docs/sensor-sync-09-timing.svg
```

图里表达的是：

- PPS 是 UTC 秒边界；
- IIM 样本沿由 32K 派生的 51.2 Hz 采样网格决定；
- PPS 不一定落在样本沿上；
- 代码用 PPS 映射把每个样本沿标成 UTC 时间；
- 双节点同步依赖比较这些 UTC 时间戳，而不是强制无线包同时到达。

## 当前同步误差如何理解

当前工程的后处理默认把 GPS PPS 当作准确 UTC 基准，不单独估计 PPS 自身误差。需要分析的是两个 CSV 采样时间网格之间的相对误差，主要包括：

- `micros()`/`esp_timer_get_time()` 与 32 位时间重建误差；
- INT1 中断捕获延迟；
- FIFO 样本时间反推误差；
- 两个节点各自 IIM 采样网格的初始相位不同；
- 两个 DS3231/IIM 采样节拍之间的相对频率漂移；
- CSV 时间字符串的 `0.1 ms` 量化精度；
- 丢样、重复时间戳或明显的大时间间隔。

因此当前版本更准确地说是“GPS/PPS 时间戳同步”，不是“采样沿闭环相位同步到 ±50 us”。

你图里的两个样本夹住 PPS 时，同步判断应该看：

```text
样本时间戳 t_k 与 PPS 时间 T_pps 的差值
样本时间戳 t_k+1 与 PPS 时间 T_pps 的差值
两个节点同一事件或同一 UTC 窗口内样本时间戳的差值
```

而不是要求某个样本必须刚好出现在 `1.000000s`。

## 一句话版本

当前代码的同步方式是：

```text
GPS PPS 定义绝对秒 -> IIM 用 DS3231 32K 连续采样 -> INT1/FIFO 给样本本地时间 -> 本地时间换算成 UTC -> 两个节点按 UTC 时间戳对齐
```

所以 PPS 在两个样本之间是正常的；真正重要的是每个样本能被标上正确的 UTC 时间。

## 文献依据与当前方案定位

当前方案采用“独立时间戳 + PC 端重采样”的路线，而不是要求所有传感器的物理采样沿完全重合。

Koo、Hester 和 Kim 提出的低成本 GPS 同步方法，首先结合 GPS PPS、NMEA 和节点本地计数器给测量值建立准确时间戳，然后把不同起始相位、不同采样间隔的数据线性重采样到共同的规则时间点。论文特别面向无线加速度传感器和模态分析，并用自然频率、振型以及互谱相位验证同步结果。这与当前工程的处理路线直接对应：

```text
PPS 作为准确 UTC 基准
-> IIM 持续采样
-> 样本获得 UTC 时间戳
-> PC 端估计节点间偏置和漂移
-> 线性插值到公共 UTC 网格
```

Salazar-Lopez 等人的结构振动实验使用 RTC+PPS 统一多个无线节点的采集启动时刻，说明同步对振动传播时间判断非常重要；论文同时指出，100 Hz 采样只能提供约 `10 ms` 的传播时间分辨率。因此 PPS 本身很准确，并不表示最终结构信号的时间分辨率可以超过采样周期。

Bocca 等人的同步无线传感器网络工作表明，同步加速度数据可用于实验模态分析。对齐后的数据应进一步通过自然频率、振型、互谱和相干性验证，而不能只看 CSV 时间列是否一致。

Johansson 与 Löwenborg讨论了非均匀采样信号的分数延迟重建。当前项目的目标是误差小于一个采样周期，并优先保持无线采集和后处理简单，因此先使用相邻两个样本的线性插值；只有以后需要接近奈奎斯特频率的高精度相位分析时，才需要升级为带限或分数延迟滤波。

## 后处理方法

USB 与后续 P2P 无线采集都继续保存简单的四列 CSV：

```text
datetime,ax,ay,az
```

后处理不保存额外的 `TXG`、`TIMEBASE`、PPS sidecar 或 metadata 文件，也不依赖这些诊断行。处理步骤固定为：

1. 检查两个 CSV 的时间戳是否单调，统计重复时间戳、丢样和大间隔。
2. 只分析两个文件重叠的公共时间区间。
3. 按时间顺序进行一对一最近邻匹配，避免一个样本被重复配对。
4. 计算有符号偏置、平均绝对误差、中位数、RMS、P95、最大值、起止偏置和线性漂移 ppm。
5. 以 UTC 整秒为原点建立 `51.2 Hz` 公共时间网格。
6. 每个轴使用相邻两个原始样本做线性插值。
7. 如果相邻原始样本间隔大于 `1.5` 个采样周期，则不跨越这个缺口插值。

运行示例：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\sensor_time_error_analysis.py `
  --sensor-a data1\sensor1001\20260711\tx_usb_capacity.csv `
  --sensor-b data1\sensor1002\20260711\tx_usb_capacity.csv `
  --actual-rate-hz 51.2 `
  --write-aligned
```

工具在控制台打印误差，并生成一个简洁的 `aligned_common_grid.csv`：

```text
datetime,
sensor1001_ax,sensor1001_ay,sensor1001_az,
sensor1002_ax,sensor1002_ay,sensor1002_az
```

## 当前 data1 实测结果

对以下两个文件进行公共区间一对一匹配：

```text
data1/sensor1001/20260711/tx_usb_capacity.csv
data1/sensor1002/20260711/tx_usb_capacity.csv
```

结果为：

| 指标 | 结果 |
| --- | ---: |
| 有效匹配样本数 | 5999 |
| 平均绝对误差 | 1.517 ms |
| 中位绝对误差 | 1.500 ms |
| RMS | 1.565 ms |
| P95 | 2.100 ms |
| 最大有效误差 | 2.200 ms |
| 相对漂移 | 约 11.28 ppm |

`50 Hz` 配置对应实际约 `51.2 Hz`，一个采样周期为：

```text
T = 1 / 51.2 Hz = 19.53125 ms
```

当前最大有效误差 `2.200 ms < 19.53125 ms`，所以按“小于一个采样周期”的标准通过。节点 1001 最开头有一个样本不在公共区间内，它只计入未匹配边界样本，不作为同步失败。

这些数值表示当前 CSV 时间戳网格之间的误差。把两个信号插值到同一个公共网格后，CSV 时间列会完全一致，但这不能反向证明两个 IIM 的物理 INT1 采样沿同步到相同精度。

如果两个传感器安装在结构的不同位置，冲击峰值、互相关峰值或相位差还可能包含真实的振动传播时间。只有把两个传感器同位置、同方向安装并施加同一激励，或者使用示波器/公共电触发，才能把信号延迟主要解释为测量同步误差。

## PPS 每秒软件拉回（无线 `pps-aligned`）

无线软件版本不修改 DS3231、IIM 或 GPS 的物理接线。每个 TX 仍以本地 GPS PPS 建立 UTC 时间，并由 DS3231 32.768 kHz 连续驱动 IIM。PC 下发的 P2P `START_AT` 会由 RX 向上对齐到下一个 UTC 整秒并广播；各 TX 在该目标秒或之后的首个本地 PPS 开始本轮采集。RX 不接收物理 PPS 电平，也不使用无线包到达时刻作为时间基准。

RX 串口采集工具新增 `--csv-timebase pps-aligned`。它根据无线包重建的 UTC 时间将两个节点按整秒缓存；每秒取两节点 PPS 后首条样本中较晚的 UTC 时间为共同锚点 `G`，然后把第 `k` 对样本写为：

```text
datetime(k) = G + k * T
```

其中 `T` 是实际 IIM 采样周期。到下一个 UTC 秒时重新计算 `G`，所以软件时间网格每秒都会拉回。若任一节点该秒缺样、乱序过晚或两节点样本数不同，则仅输出可一对一配对的部分，并在控制台输出 `PPS_ALIGN` 统计；P2P 乱序缓存最多保留 5 个 UTC 秒，超过该窗口的迟到样本会丢弃。不插值补造样本，也不保存 PPS sidecar 或额外 metadata 文件。

运行无线接收保存时使用：

```powershell
& "$env:USERPROFILE\.platformio\penv\Scripts\python.exe" tools\serial_rx_to_file.py `
  --port COMx --output data_pps_aligned --rotate-minutes 30 --with-header `
  --csv-timebase pps-aligned --pps-aligned-device-ids 1001,1002 `
  --sample-rate-hz 50 --start-delay-seconds 10
```

`pps-aligned` 中两份 CSV 的逻辑 `datetime` 会一致，但这只证明软件公共网格一致；真实 INT1 相位差仍须用 `raw` 模式和示波器的 `PPS1001/PPS1002/INT1-1001/INT1-1002` 四通道测量验证。

## 参考文献

1. K. Y. Koo, D. Hester, and S. Kim, “Time Synchronization for Wireless Sensors Using Low-Cost GPS Module and Arduino,” *Frontiers in Built Environment*, vol. 4, art. 82, 2018. https://doi.org/10.3389/fbuil.2018.00082
2. J. R. Salazar-Lopez, J. R. Millan-Almaraz, J. R. Gaxiola-Camacho, G. E. Vazquez-Becerra, and J. M. Leal-Graciano, “GPS-Based Network Synchronization of Wireless Sensors for Extracting Propagation of Disturbance on Structural Systems,” *Sensors*, vol. 24, art. 199, 2024. https://doi.org/10.3390/s24010199
3. M. Bocca, L. M. Eriksson, A. Mahmood, R. Jäntti, and J. Kullaa, “A Synchronized Wireless Sensor Network for Experimental Modal Analysis in Structural Health Monitoring,” *Computer-Aided Civil and Infrastructure Engineering*, vol. 26, no. 7, pp. 483–499, 2011. https://doi.org/10.1111/j.1467-8667.2011.00718.x
4. H. Johansson and P. Löwenborg, “Reconstruction of Nonuniformly Sampled Bandlimited Signals by Means of Digital Fractional Delay Filters,” *IEEE Transactions on Signal Processing*, vol. 50, no. 11, pp. 2757–2767, 2002. https://doi.org/10.1109/TSP.2002.804089

## 文档维护要求

以后只要修改以下任一内容，都要同步更新本文：

- GPS/PPS 与本地时间的映射方式；
- IIM 的采样时钟、实际采样率或 FIFO 时间重建；
- USB/P2P 无线 CSV 的时间戳含义；
- 同步误差指标、通过阈值或公共网格插值方法；
- 无线现场测试流程和最终模态分析方法。
