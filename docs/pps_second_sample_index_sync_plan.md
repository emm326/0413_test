# PPS 秒内样本序号同步模式方案

- 方案确认时间：2026-08-14 09:54:13 +08:00（Asia/Hong_Kong）
- 模式名称：PPS 秒内样本序号同步模式
- 当前状态：电脑端第一版已实现，真实硬件验收待完成
- 适用链路：普通 `usb_gps_sync_test` 固件的 USB 多节点采集

## 1. 目标

新增一种独立的多节点同步模式。多个节点在同一个未来 UTC 整秒开始保存数据，并在每个 GPS PPS 秒内按照样本出现顺序重新编号。电脑端使用“PPS 绝对秒 + 秒内样本序号”配对不同节点的数据，再为共同样本生成完全一致的逻辑时间。

该模式解决的是多节点数据文件的逻辑序号和时间轴对齐，不会让各节点 IIM-42352 的物理 INT1 采样沿同时发生。

## 2. 硬件和时间基准

每个节点继续使用现有接线：

```text
DS3231 32K -> IIM-42352 CLKIN
LC76G PPS   -> ESP32-S3 GPIO4
```

- DS3231 的 32.768 kHz 信号驱动 IIM 连续采样。
- GPS PPS 提供 UTC 整秒边界。
- PPS 不停止、复位或重新启动 IIM。
- 不恢复 GPIO3 CLKIN、`GPS_DISCIPLINED_CLKIN` 或 DPLL 实验方案。
- 使用名称中不含 `usb_gps_clk_sync_test` 的普通 USB GPS 固件。

## 3. 统一启动

采集程序必须先等待所有配置节点满足以下条件：

```text
SENSOR_READY
GPS/PPS healthy
```

随后根据各节点最新 GPS PPS，选择同一个未来 UTC 整秒并向所有节点发送相同的 `START_AT`。各节点到达目标 PPS 后开始保存，目标 PPS 之前的样本不进入本次采集。

这里的“同时开始”仅表示从同一个 UTC 秒开始保存。IIM 在启动前后保持连续运行，各节点 PPS 后首个物理样本的相位可以不同。

## 4. 实际采样率和每秒容量

计算必须使用 DS3231 32.768 kHz 驱动下的 IIM 实际 ODR，而不是界面显示的配置值：

| 配置采样率 | 实际 ODR | 采样周期 `T` | 每秒最大保留数 `N` |
|---:|---:|---:|---:|
| 50 Hz | 51.2 Hz | 19.53125 ms | 51 |
| 100 Hz | 102.4 Hz | 9.765625 ms | 102 |
| 200 Hz | 204.8 Hz | 4.8828125 ms | 204 |
| 400 Hz | 409.6 Hz | 2.44140625 ms | 409 |

通用计算规则为：

```text
actual_rate_hz = configured_rate_hz * 1.024
T = 1 / actual_rate_hz
N = floor(actual_rate_hz)
```

采样率在一次采集运行中保持不变。改变配置采样率时，实际 ODR、`T` 和 `N` 必须一起重新计算。

## 5. 每个 PPS 秒的编号规则

设某个 GPS PPS 的绝对时间为 `S`，下一个 PPS 为 `S + 1秒`。每个节点独立执行以下规则：

1. 只接收严格位于当前 PPS 之后、下一 PPS 之前的样本。
2. 将样本按真实 GPS 校准 UTC 时间升序排列。
3. PPS 后首个样本的秒内序号为 `1`，后续依次为 `2、3……N`。
4. 每秒最多保留前 `N` 个样本。
5. 第 `N` 个之后、下一 PPS 之前出现的样本直接忽略，不把它们移入下一秒。
6. 下一 PPS 到来后重新从序号 `1` 开始。

51.2 Hz 的一个完整秒示例：

```text
PPS S
  第1个样本  -> index=1
  第2个样本  -> index=2
  ...
  第51个样本 -> index=51
  可能出现的第52个样本 -> 忽略
PPS S+1
  PPS后的首个样本 -> 新一秒的 index=1
```

## 6. 逻辑时间生成

节点数据上传到电脑后，由电脑端使用统一公式生成同步时间，避免不同设备独立浮点计算产生差异：

```text
sync_epoch_us = pps_epoch_us
              + round(second_sample_index * 1,000,000 / actual_rate_hz)
```

其中 `second_sample_index` 从 `1` 开始。51.2 Hz 时：

```text
index=1  -> S + 19.53125 ms
index=2  -> S + 39.06250 ms
...
index=51 -> S + 996.09375 ms
```

同一 PPS 秒、相同秒内序号在所有节点中必须得到完全相同的 `sync_epoch_us`。`ax/ay/az` 始终使用对应原始样本值，不插值、不补零、不平均。

## 7. 多节点样本序号交集

同步配对键定义为：

```text
(pps_epoch_us, second_sample_index)
```

每个 UTC 秒分别收集所有配置节点的序号集合，然后计算全部节点的交集：

```text
common_indices = indices_node_1
               ∩ indices_node_2
               ∩ ...
               ∩ indices_node_n
```

只将 `common_indices` 中的样本写入同步 CSV。例如：

```text
1001: 1,2,3,4,5
1002: 1,2,3,4
1003: 1,2,4,5

共同序号: 1,2,4
```

三个节点最终都只写序号 `1、2、4`，因此同步 CSV 的数据行数和时间列逐行一致。某节点不足 `N` 条时，不使用其他节点的额外数据补齐，也不从下一秒借用数据。

## 8. 乱序、掉线和 PPS 异常

- 沿用 5 秒乱序/掉线窗口；窗口结束后才最终确定旧秒的交集。
- 已经完成的 UTC 秒不得被迟到数据修改。
- 迟到样本不得跨秒配对。
- 任意节点缺少整个 UTC 秒时，该秒交集为空，不生成同步数据行，并在报告中记录缺失节点。
- 节点 PPS 缺失、重复或健康状态异常时，对应秒不参加同步。
- PPS 恢复健康后，从新的 PPS 后首个样本重新编号为 `1`。
- 采集只输出从目标 `START_AT` PPS开始的完整秒；停止时未完成的秒不进入同步 CSV。

## 9. 输出设计

新模式已经作为独立选项加入，不改变现有 `raw+pps-aligned` 最近槽位模式：

```text
--csv-timebase raw+pps-index-aligned
```

双节点 600 秒命令行示例：

```powershell
python tools\tx_usb_capacity_test.py --ports COM5,COM6 --device-ids 1001,1002 --gps-sync --csv-timebase raw+pps-index-aligned --output data_usb_pps_index_sync --duration-seconds 600 --sample-rate-hz 50 --range-g 4
```

三个及以上节点时，`--ports` 与 `--device-ids` 按相同顺序增加，并保持数量一致。GUI 的 `CSV output mode` 下拉框中也可以直接选择 `raw+pps-index-aligned`。

建议输出目录：

```text
data_usb_pps_index_sync/
  raw/
    sensor<id>/<date>/tx_usb_capacity.csv
  pps_index_aligned/
    sensor<id>/<date>/tx_usb_capacity.csv
  capture_report.txt
```

`raw` 保留现有 GPS 校准真实 UTC 时间；`pps_index_aligned` 保存本方案生成的共同逻辑时间。同步 CSV 继续使用四列格式：

```text
datetime,ax,ay,az
```

每个完整 UTC 秒输出一条质量日志：

```text
PPS_INDEX_ALIGN,pps_epoch_us=...,actual_rate_hz=...,capacity=...,
common=...,count_1001=...,ignored_1001=...,missing_1001=...,
count_1002=...,ignored_1002=...,missing_1002=...
```

节点字段根据本次配置的设备列表动态生成：

- `capacity`：该采样率每秒最大保留数 `N`。
- `count_<id>`：窗口结束时该节点在本秒可用于编号的样本数，上限为 `N`。
- `ignored_<id>`：该节点本秒超过 `N` 后被忽略的样本数。
- `missing_<id>`：相对 `N` 缺少的数量。
- `common`：所有节点序号交集的大小，也是每个同步 CSV 在该秒新增的行数。

## 10. 与现有 PPS 最近槽位模式的区别

| 项目 | 现有 `pps_aligned` | 新 `pps_index_aligned` |
|---|---|---|
| 每秒锚点 | 两节点首样本中较晚者 | GPS PPS 绝对秒 |
| 配对方式 | 映射到最近公共槽位 | PPS 后样本序号取交集 |
| 首个逻辑时间 | 共同锚点 `G` | `PPS + T` |
| 每秒数量 | 由共同锚点到下一 PPS 的槽位决定 | 固定上限 `floor(实际ODR)` |
| 超额样本 | 由槽位映射结果决定 | 达到上限后直接忽略 |
| 物理采样沿同步 | 否 | 否 |

两种模式必须并存，用户可以根据分析目的选择，不能用新模式替换或改变旧模式的输出语义。

## 11. 已知限制

第一版暂不处理“某个中间样本读取或传输失败后，是否保留原始序号缺口”的问题。暂定按照电脑端在每个 PPS 秒内实际收到的样本顺序连续编号。

因此，如果实际丢失的是秒内中间样本，丢失点之后的样本可能向前移动一个逻辑序号。该问题必须在报告中标记为已知限制，后续可通过固件上报 INT1 级秒内序号解决，但不属于当前第一版范围。

另外，本模式每秒重新设置逻辑序号，会隐藏不同节点之间的真实采样相位差。相同逻辑序号样本的物理采样时刻仍可能相差接近一个采样周期。需要研究真实偏移和漂移时，应使用 `raw` 数据。

## 12. 实施状态和后续步骤

电脑端第一版已经完成：

1. `tools/tx_usb_capacity_test.py` 已新增多节点 PPS 秒缓存和序号交集处理器。
2. 已复用实际 ODR 换算和 5 秒乱序窗口，并增加每秒容量限制。
3. 命令行和 GUI 已加入 `raw+pps-index-aligned` 选项。
4. 已增加独立输出管理器、`pps_index_aligned` 目录和 `PPS_INDEX_ALIGN` 报告。
5. 现有 `raw`、`aligned` 和 `raw+pps-aligned` 行为保持不变。
6. 固件协议没有修改，继续使用普通 `usb_gps_sync_test` 固件。

两个普通 USB GPS 固件已经编译通过。剩余步骤是使用 COM5/1001 和 COM6/1002 进行 600 秒真实硬件验收。

## 13. 测试和验收标准

自动化测试至少覆盖：

- 所有支持采样率的实际 ODR、周期和每秒容量计算。
- 51.2 Hz 下每秒收到 51、52 和少于 51 个样本。
- 第 52 个样本被忽略，并且不会进入下一 UTC 秒。
- 两节点以及三个以上节点的序号交集。
- 某节点整秒缺失、短时掉线、5 秒乱序和窗口结束后的迟到数据。
- PPS 异常后停止同步，并在下一健康 PPS 重新从 1 编号。
- 所有节点同步 CSV 的数据行数和时间列完全一致。
- 加速度值来自原始样本，不插值、不补零、不平均。
- 不跨 UTC 秒或大缺口配对。
- 现有同步模式回归测试全部通过。

真实硬件 600 秒验收应满足：

- 所有节点在同一目标 UTC PPS 开始保存。
- 每个健康完整秒产生一条 `PPS_INDEX_ALIGN`。
- 50 Hz 配置下每节点每秒最多使用 51 个样本。
- 超过第 51 个的样本不进入本秒或下一秒同步输出。
- 多份 `pps_index_aligned` CSV 的时间列和数据行数逐行一致。
- 不生成空 CSV，不伪造数据，不跨秒配对。

截至本次实现，Python 全量自动化测试为 116 项通过、1 项跳过；1001 和 1002 的普通 `usb_gps_sync_test` 固件均编译成功，`git diff --check` 通过。
