# P2P ACK-Grant Burst Update

当前双节点 100Hz 推荐配置：

```text
nodes=2
burst=6
packet_samples=37
tx_queue=128
```

ACK-grant burst 不再依赖 RX 对同一节点重复发送 `POLL`。RX 第一次 `POLL` 点名后，TX 发送队列头数据包；RX 收到 DATA 后返回 ACK，并通过 `ACK.param` 授权是否继续：

```text
POLL.param = 本轮该节点最多允许连续发送的包数
ACK.param  = 1 表示 TX 可以立刻继续发送下一个队列包
ACK.param  = 0 表示本节点本轮结束，等待下一次 POLL
```

TX 只有收到匹配 ACK 后才删除队列头。若 ACK 丢失，TX 保留未确认包，等待下一次 POLL 重发。若 ACK 带 grant 但 TX 队列已空，TX 立即回 `NO_DATA`，避免 RX 等到 poll timeout。

RX/TX1001/TX1002 必须烧录同一版固件。新旧固件混用时，ACK grant 语义不一致，不能保证吞吐改善。
