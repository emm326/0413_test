# LoRa 采集协议 v3

固定小端；CRC16-CCITT-FALSE，poly=`0x1021`、init=`0xffff`，CRC 低字节在前。魔数 `0xA55A`，版本 3，最大 255 字节。

## 包头和载荷

| 偏移 | 字段 | 类型 |
|---:|---|---|
| 0 | magic | uint16 |
| 2 | version | uint8 |
| 3 | type | uint8 |
| 4 | node，广播 `0xffff` | uint16 |
| 6 | flags | uint16 |
| 8 | run | uint32 |
| 12 | sequence | uint32 |
| 16 | epoch_us | uint64 |
| 24 | rate_hz | uint16 |
| 26 | sample_count | uint8 |
| 27 | range_g | uint8 |

类型：DATA=1、START=2、STOP=3、CONFIRM=4、STATUS=5、DISCOVER=6、ACK=7。flags：bit0 GNSS 健康、bit1 时钟健康、bit2 采样缺口、bit3 队列溢出、bit4 尾包。

- DATA：最多 25 组 XYZ int16；正常包 180 字节。首样本 UTC 加 `i*1000000/rate` 还原时间。
- START：`float32 uplinkMHz`、`uint32 durationSeconds`、`uint8 sf`、保留字节、`uint16 nodeMask`。bit0..7 对应节点 1001..1008。上行频率由节点 ID 固定，字段为 0 或与映射一致。
- CONFIRM/STATUS：时钟、ppm、丢样、丢包、队列、状态和结果，共 20 字节。
- ACK：若干 10 字节条目 `node, firstSequence, bitmap`。单个批量包最多 8 条，每节点一个 32 位位图；重复数据也再次确认。
- STOP 和 DISCOVER 载荷为空。ACK、CONFIRM、STOP 必须匹配当前 `run`，过期运行不得污染当前采集。

## 节点和无线参数

节点 1001–1008 分别固定为 470.3–471.7 MHz，间隔 0.2 MHz。待机和控制窗口统一监听 470.3 MHz。

- 50 Hz：SF6BW125、4/5、前导码 12、同步字 `0x12`。
- 100 Hz：SF5BW125，只有 50 Hz 实机验收后才允许显式启用。
- Semtech UDP v2 的 PUSH_ACK/TX_ACK 不代替节点 CONFIRM。

## 五秒周期

| 相位 | 节点行为 |
|---|---|
| 0–4.00 秒 | 自身上行信道发送正常数据 |
| 4.00–4.25 秒 | 470.3 MHz 接收 ACK/START/STOP/状态请求 |
| 4.25–4.45 秒 | 自身上行信道并行回复 CONFIRM/STATUS |
| 4.45–5.00 秒 | 补传未确认数据 |

START 可重复发送，但节点只对同一运行做幂等确认。主机必须等待全部目标节点成功 CONFIRM；缺失确认时取消启动。未列入目标集合的节点只告警，不写 CSV。
