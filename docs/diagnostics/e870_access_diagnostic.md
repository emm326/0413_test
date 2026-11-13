# E870 上位机与网关接入检查

- 日期：2026-11-12（Asia/Hong_Kong）。
- 对应步骤：步骤 2 上位机软件复核；步骤 4 E870 实机接入与 TX 能力前置检查。
- 当前状态：上位机软件检查已完成；USB2 只读诊断和原配置备份已完成；网关网络接入和项目射频联调待处理。
- Git 基线：`75e0f1c`，分支 `codex/e870-8-node`。
- 本轮开始时已有上一轮未提交的固件修复、计划和验收文档改动；本轮没有覆盖固件源码，没有执行 git add/commit。
- 本轮没有发送 START/STOP、没有启动正式采集、没有烧录节点或网关固件。

## 1. 手册核对

来源为用户提供的本地《E870-LxxxLG12-O 用户手册》CN v1.1，共 29 个 PDF 页面。已提取文本，并渲染查看网络设置、串口和网关配置目录相关页面。

- 印刷第 4 页：USB2 是调试接口，USB1 是烧录接口；WAN 为 10/100/1000 Mbps。
- 印刷第 5–8 页：支持串口或 SSH 管理；串口 115200，手册列出的出厂账号和密码均为 root，实际交付若已修改则使用实际凭据。
- 印刷第 11、13–14 页：网关可通过 `ifconfig eth0` 查看实际地址；网口由 systemd-networkd 管理，配置位置为 `/etc/systemd/network/01-eth0.network`，支持 DHCP 和静态 IP。
- 印刷第 12、15 页：`gw status` 和 `journalctl -u gw` 用于服务状态和日志检查。
- 印刷第 17 页：`global_conf.json` 是指向 `.gwsrc/config/` 地区配置的软链接。
- 印刷第 20–23 页的 ChirpStack、Class C、Node-RED 是 LoRaWAN 示例，不能替代本项目的原始 LoRa 私有协议和节点确认流程。手册中的 `192.168.1.10` 只是示例地址，不能当作本机固定地址。

## 2. 上位机程序检查

检查范围：`host/__main__.py`、`gui.py`、`gateway.py`、`wire.py`、`recorder.py`、`configure_gateway.py`、`gateway/project.overlay.json` 及两份现有测试文件。

| 检查项 | 结果 | 说明 |
|---|---|---|
| Python 语法编译 | 通过 | `host`、`gateway` compileall 通过 |
| 现有自动测试 | **32/32 通过** | 协议、UDP 回环、配置校验、节点确认、补传 ACK、CSV 与统计 |
| 图形界面初始化 | 通过 | 隐藏窗口构造并销毁成功；1003、50 Hz、SF6 设置正常 |
| 命令行入口 | 通过 | `python -m host --help` 正常 |
| UDP 1700 本地绑定 | 通过 | 两次 15 秒只接收检查均能绑定，结束后已释放 |
| 真实网关上下行 | 待实测 | 尚无项目节点 CONFIRM 或项目数据包 |

上位机配置生成器会保留原厂射频校准、功率表和 TX 限制；如果原配置不允许 470.3 MHz 下发，会拒绝写出项目配置。使用本次从网关导出的真实原配置进行验证时，生成器退出码为 2，且没有写出项目配置，行为符合项目安全规则。

运行环境中没有 pytest，本轮使用项目已有的 unittest；在正常系统权限下重跑结果为 32/32 通过。

## 3. USB2 管理登录与网关状态

用户接入 USB2 后，电脑识别到 `COM11`。使用 115200、8N1、关闭 DTR/RTS，成功登录 `T113-lorawanopen`，登录后执行只读命令并正常退出。

### 3.1 网关网络

- 电脑有线网卡：Realtek Gaming 2.5GbE Family Controller，Up，1 Gbps。
- 电脑有线 IPv4：`192.168.1.22/24`，无有线默认网关。
- 网关 `eth0`：物理链路 `LOWER_UP`，但没有 IPv4，仅有 `fe80::24:48ff:fe19:d801/64`。
- 网关 `/etc/systemd/network/01-eth0.network`：DHCP 模式，包含 `DHCP=ipv4`。
- 网关 `networkctl`：`State: degraded (configuring)`。
- 网关 IPv4 路由：没有 eth0 的 IPv4 路由。

这解释了当前电脑无法通过 IPv4 找到网关的直接原因：网线物理链路存在，但网关处于 DHCP 模式且没有取得 IPv4 地址。另外报文目的地址仍为设备本机，不能发到电脑。

### 3.2 网关程序

- `gw.service`：`active (running)`。
- 主进程：`lora_pkt_fwd`，PID 2118，`NRestarts=0`。
- Packet Forwarder 版本：2.1.0。
- SX1302 HAL 版本：2.1.0。
- 启动日志：集中器成功启动；日志中有非本项目 LoRa 报文接收记录，以及发往本机服务器的 PUSH_DATA 和本机 PULL_ACK 记录。
- `gw --help`：脚本功能正常，提供 start/stop/restart/status/enable/disable/switch。
- 设备时间：`Sat, 01 Aug 2026 10:43:33 +0800`。当前日期为 2026-11-12，设备时间相差约三个月；本轮没有校时。

网关程序本身能运行，但这只证明原厂网关服务和射频接收进程已启动，不代表项目私有 LoRa 协议已经联通。

## 4. 原厂配置核验

- 当前软链接：

  `/root/lorawanopen/gateway/.gwsrc/global_conf.json` → `/root/lorawanopen/gateway/.gwsrc/config/470_CN470.json`

- 当前服务器地址：`127.0.0.1`。
- 上行和下行端口：均为 UDP `1700`。
- `lorawan_public`：`true`。
- radio_0：SX1250，中心频率 470.6 MHz，接收信道覆盖项目所需的 470.3–470.9 MHz。
- radio_1：SX1250，中心频率 471.4 MHz，接收信道覆盖项目所需的 471.1–471.7 MHz。
- 8 个多 SF 接收信道实际组合为：470.3、470.5、470.7、470.9、471.1、471.3、471.5、471.7 MHz。
- 配置启用 SF5–SF12，说明当前接收配置包含 SF6 的软件参数。
- radio_0 TX：`tx_enable=true`，`tx_freq_min=500000000`，`tx_freq_max=510000000`。
- 项目控制下行要求：470.3 MHz、SF6BW125、私有同步字 `0x12`。

因此当前原配置存在三项项目适配差异：

1. 网关没有 IPv4 地址。
2. 上行数据转发给网关自身 `127.0.0.1:1700`，不是电脑 `192.168.1.22:1700`。
3. 原配置的 TX 范围 500–510 MHz 不包含项目控制下行 470.3 MHz；原配置还使用 `lorawan_public=true`，与项目私有 LoRa 配置不同。

项目生成器已用真实备份验证：由于 470.3 MHz 不在真实 TX 范围，拒绝生成 `project.local.json`。没有修改 TX 上下限，也没有伪造硬件能力。

## 5. 原配置和证据备份

已从 USB2 串口导出并校验：

| 文件 | 位置 | 校验 |
|---|---|---|
| 原厂配置 | [gateway/factory.local.json](../../gateway/factory.local.json) | SHA256 `7e036e4f60ccfb14697fbca825df01fa1ac1f7aa91771f89e596bc3e44e58f15` |
| 网卡配置 | `tmp/e870_serial_20261112/01-eth0.network` | 与设备一致 |
| 网关脚本 | `tmp/e870_serial_20261112/gw.sh` | 与设备一致 |
| systemd 服务 | `tmp/e870_serial_20261112/gw.service` | 与设备一致 |
| 串口只读日志 | `tmp/e870_serial_20261112/management_readonly_full.log` | 被 Git 忽略 |

备份清单位于 `tmp/e870_serial_20261112/backup_manifest.json`。`gateway/factory.local.json` 和 `tmp/` 均被 Git 忽略，不进入提交。

已准备一份仅供审阅的静态地址方案，但尚未写入设备：

```ini
[Match]
Name=eth0

[Network]
DHCP=no
Address=192.168.1.10/24
```

## 6. 文件变化、接线和操作

- `PROJECT_PLAN.md`：更新 USB2 实测结果、网络阻塞点、服务状态、原配置 TX 限制和下一步。
- `docs/e870_setup.md`：补充 USB2/串口、DHCP 直连限制和实际配置结果。
- `docs/hardware_acceptance.md`：记录 COM11、网关服务、原配置和当前待实测边界。
- 本报告：补充 USB2 登录、网关状态、原配置和备份结果。
- `gateway/factory.local.json`：只读导出，内容未修改，文件被 Git 忽略。
- 节点接线、节点固件和电脑网络均未修改。
- 未停止、重启或重配置网关服务；未写入静态 IP；未修改原厂射频配置；未校时。
- 未启动上位机常驻接收、未发无线命令、未执行正式采集。

## 7. 结论与下一步

当前可以确认：上位机程序自身通过测试，网关原厂 `gw.service` 正常运行，接收链路和 8 个 470 MHz 上行信道配置存在。

当前不能确认：项目私有 LoRa 收发、电脑到网关的 UDP 双向链路、470.3 MHz 下行、节点 CONFIRM 和数据采集。

下一步有两个独立动作：

1. 先把网关 eth0 配置为与电脑同网段的地址（候选 `192.168.1.10/24`），然后把网关服务器地址改为电脑 `192.168.1.22`，验证 UDP 1700 的 PUSH_DATA/PULL_ACK。网络写入前需要确认使用这个地址，避免覆盖现场已有网络规划。
2. 在任何下行测试前，向厂家确认 E870-L470LG12-O 这台设备的发射通路、滤波器、射频校准和官方配置是否支持 470.3 MHz。不能仅修改 JSON 的 TX 上下限；在确认前，上位机应保持“仅接收/禁止开始和停止”的安全状态。

即使网络配置完成，在厂家确认 470.3 MHz 下行前，也只能继续做接收侧检查，不能把 TX_ACK 或网关服务运行当成节点收到控制命令的证据。
