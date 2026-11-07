# E870 八节点网关与上位机

## 1. 连接和备份

1. 给 E870-L470LG12-O 接好 LoRa 天线和 8–28 V 电源，WAN 与电脑处于同一局域网。
2. USB2 调试口使用 115200；账号密码以设备交付设置为准。
3. 在网关执行 `gw status`、`gw --help`，查出实际 `global_conf.json` 软链接目标。
4. 下载原配置和启动脚本，保存为不提交 Git 的 `gateway/factory.local.json`。配置前执行 `gw stop`。

本项目只使用一台 E870 接收 8 个上行信道并下发控制，不增加 ESP32 接收网关。

## 2. 验证并生成配置

```powershell
python -m host.configure_gateway `
  --input gateway/factory.local.json `
  --output gateway/project.local.json `
  --server 192.168.1.100
```

生成器保留原射频类型、校准、功率表和 TX 范围。只有原配置明确允许 `radio_0` 在 470.3 MHz 发射时才写出项目配置；若原范围不包含 470.3 MHz，命令会失败且不生成文件。不得只修改 JSON 的上下限冒充硬件支持，必须向厂家确认型号并完成射频实测。

| 项目 | 值 |
|---|---|
| 模式 | 原始 LoRa，`lorawan_public=false` |
| 上行信道 | 470.3/470.5/470.7/470.9/471.1/471.3/471.5/471.7 MHz |
| 50 Hz | SF6、125 kHz、4/5、同步字 `0x12`、前导码 12 |
| 100 Hz | SF5，仅在 50 Hz 验收后显式启用 |
| 控制下行 | 470.3 MHz、SF6BW125、4/5、正常 IQ |
| 电脑链路 | Semtech UDP v2，上下行端口 1700 |

将生成文件上传到原配置位置后执行 `gw start`、`gw status`，并查看网关日志。

## 3. 启动上位机

```powershell
python -m host --gateway-config gateway/project.local.json --output data/e870
```

- 默认目标为 `1001–1008`，默认 50 Hz/SF6。`--nodes` 可改为 1、2、4 个节点做分级测试。
- `--gateway-config` 必须指向包含原 TX 能力的配置。未验证时程序仍可纯接收，但查询、开始和停止下行会被阻止。
- 纯接收：`python -m host --headless --gateway-config gateway/project.local.json`。
- 100 Hz 必须在界面显式选择 100 Hz 和 SF5；程序不会从 50 Hz 自动切换。

开始前确认 8 个目标节点均在线、待机、GNSS/PPS 和时钟健康。START 重复广播三次，只有全部目标节点返回成功 CONFIRM 才会进入正式运行；启动前两秒仍缺节点时，本次运行取消并发送 STOP。

## 4. 五秒周期

- `0–4.00 s`：8 个节点在各自信道上传。
- `4.00–4.25 s`：节点监听 470.3 MHz；E870 串行发送批量 ACK、START、STOP 或状态请求。
- `4.25–4.45 s`：节点切回自身信道并行发送 CONFIRM/STATUS。
- `4.45–5.00 s`：节点补传未确认数据。

PUSH_ACK 和 TX_ACK 只表示 UDP/网关发送链路状态，节点 CONFIRM 才表示控制命令实际到达。未知节点只显示告警，不写入当前运行 CSV。

## 5. 输出和排查

每个节点写入独立目录：

```text
data/e870/run_<运行编号>/sensor1001/acceleration.csv
data/e870/run_<运行编号>/sensor1001/packets.csv
data/e870/capture_report.json
```

质量报告提供每节点及整体到达率、补传率、最终缺失率、最大延迟和 10 秒内到达比例。

| 现象 | 检查 |
|---|---|
| 无网关连接 | 电脑 IP、防火墙、UDP 1700、PULL_DATA 和网关日志 |
| 不能下行 | `--gateway-config` 路径、原 TX 范围、TX_ACK 和 470.3 MHz 实测 |
| 节点无状态 | 节点 ID/频率、SF6、同步字、GNSS/PPS 和硬件状态 |
| START 被取消 | 找出未 CONFIRM 节点，不要绕过全部确认检查 |
| 队列持续增长 | 停止扩容，检查信道遗漏、ACK 窗口、干扰和空口余量 |
| 跨节点数据 | 检查重复节点 ID；未知节点不得进入当前运行目录 |

恢复时先 `gw stop`，还原原配置和软链接，再执行 `gw start`。
