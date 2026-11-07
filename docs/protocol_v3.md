# LoRa采集协议v3

固定小端；CRC16-CCITT-FALSE，poly=0x1021、init=0xffff，CRC低字节在前。魔数0xA55A，版本3，最大255字节。

## 28字节包头

| 偏移 | 字段 | 类型 |
|---:|---|---|
| 0 | magic | uint16 |
| 2 | version | uint8 |
| 3 | type | uint8 |
| 4 | node，广播0xffff | uint16 |
| 6 | flags | uint16 |
| 8 | run | uint32 |
| 12 | sequence | uint32 |
| 16 | epoch_us | uint64 |
| 24 | rate_hz | uint16 |
| 26 | sample_count | uint8 |
| 27 | range_g | uint8 |

类型：DATA1、START2、STOP3、CONFIRM4、STATUS5、DISCOVER6、ACK7。
flags：bit0 GNSS健康、bit1时钟健康、bit2采样缺口、bit3队列溢出、bit4尾包。

DATA：25组XYZ int16，150字节；包合计180字节。尾包按count缩短。首样本UTC + i*1000000/rate还原本包时间。
START载荷12字节：float32 uplinkMHz（0保留节点频点）、uint32 durationSeconds（0手动停止）、uint8 sf、uint8 reserved、uint16 nodeMask。掩码bit0..9对应1001..1010。包头携带启动UTC、采样率、量程、运行编号。
CONFIRM/STATUS载荷20字节：float32 clockHz、float32 clockPpm、uint32 lostSamples、uint32 lostPackets、uint16 queued、uint8 state、uint8 result。
state：0启动/1待机/2已武装/3采集中/4尾包补传/5硬件异常。result：0接受/1未就绪/2参数错误/3忙/4启动过近。
ACK载荷为若干10字节条目：uint16 node、uint32 firstSequence、uint32 bitmap；bit i确认firstSequence+i。重复包也再次确认。ACK必须匹配run。
STOP载荷空，run=当前运行编号；DISCOVER载荷空。

普通状态/控制下行使用SF6BW125、4/5、pre12、sync0x12、正常IQ、物理CRC开启。
主机PUSH_DATA/PULL_DATA/下发协议参照Semtech sx1302_hal packet_forwarder/PROTOCOL.md。
