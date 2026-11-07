# E870-L470LG12-O 网关与上位机设置

## 1. 连接与登录

1. LoRa天线接LoRa SMA接口，使用8–28V配套电源。
2. WAN口与电脑接同一局域网。
3. USB2为调试口，MobaXterm Serial=115200。默认账号/密码root/root，以交付设置为准。
4. 执行`ifconfig eth0`查询地址，后续可SSH登录。电脑IP固定或在路由器做DHCP保留。

## 2. 备份实际配置

在网关终端执行：

```sh
gw status
gw --help
readlink -f /root/lorawanopen/gateway/.gwsrc/global_conf.json
```

记录当前软链接目标，下载该配置和gw.sh到本机备份。手册中的目录为`/root/lorawanopen/gateway/.gwsrc/config/`，CN470通常为`470_CN470.json`。
配置前运行`gw stop`。切换频段按`gw --help`的参数格式执行，并重新查询生效文件。

## 3. 生成项目配置

把原配置导出为`gateway/factory.local.json`，在项目根目录执行：

```powershell
python -m host.configure_gateway --input gateway/factory.local.json --output gateway/project.local.json --server 192.168.1.100
```

192.168.1.100替换为电脑实际IP。生成器保留射频类型、校准、功率表和原发射范围，允许输入带注释的JSON。
输出检查项“配置允许470.3MHz下发”必须为True，并完成实机确认。若原TX范围是500–510MHz，先向厂家确认470.3MHz射频能力；不通过改范围数字冒充硬件支持。

项目参数：

| 配置 | 值 |
|---|---|
| 无线方式 | 原始LoRa，lorawan_public=false |
| 同步字 | 0x12（SX1302私有模式） |
| 上行信道 | 470.3/470.5/470.7/470.9/471.1/471.3/471.5/471.7MHz |
| 带宽 | 125kHz |
| 初期上行 | SF6，4/5，前导码12，物理CRC开启 |
| 控制下行 | 470.3MHz、SF6BW125、4/5、前导码12、正常IQ(ipol=false) |
| 服务器 | 电脑IP，UDP上/下行均1700 |

将生成配置传回原配置目标位置，保留备份。然后执行：

```sh
gw start
gw status
journalctl -u gw -f
```

## 4. 上位机

电脑安装Python 3.10+并带Tk，项目上位机仅用标准库。根目录执行：

```powershell
python -m host --nodes 1001,1002 --output data/e870
```

1. 设置监听0.0.0.0:1700、目标节点、目录，点击连接网关。
2. 放行本机可信局域网UDP1700；确认界面显示网关下行通道。
3. 点击查询节点。等待GNSS锁定及60秒时钟验证，两个健康标志均正常。
4. 选择50/100Hz、量程和时长，点击开始。
5. 上位机重复广播带目标掩码的START_AT，节点错时回复；检查每个节点确认。
6. 到未来UTC时刻正式采集。点击停止后继续保持接收，让尾包和确认完成。
7. 关闭上位机时输出采集报告。

纯接收诊断：`python -m host --headless --nodes 1001,1002`。正式采集由桌面界面发起。

文件：

```text
data/e870/
  run_<运行编号>/sensor1001/acceleration.csv
  run_<运行编号>/sensor1001/packets.csv
  run_<运行编号>/sensor1002/acceleration.csv
  capture_report.json
```

acceleration.csv为UTC时间与g单位XYZ；packets.csv保留序号、名义采样率、量程、标志、接收质量、接收UTC和估算延迟。乱序包先暂存，补齐后按序写入。结束时输出未补齐缺口；缓存超限后的迟到数据另存late_acceleration.csv。
界面的“样本数”是已收到数据的统计；存在缺口时主CSV会等候补传。

## 5. 控制和确认

- 网关PUSH_ACK仅确认UDP报文；TX_ACK只报告网关发送结果。节点CONFIRM才代表节点接受开始命令。
- 5秒周期：0–4秒正常上行，4–4.5秒控制/批量ACK，4.5–5秒补传。
- 主机根据节点状态时间估计UTC偏移；当前控制发送为即时下发，需在实际网关验证网络延迟和窗口余量。
- 初版以双节点为联调目标；8–10节点需要按实际链路分配信道和验证SF5/时隙，不能只增加节点列表。
- 每个IIM用实际DRDY作UTC锚点。GNSS暂失时数据标记时间降级；新一轮启动需重新满足健康条件。

## 6. 排查与恢复

| 现象 | 检查 |
|---|---|
| 没有网关连接 | 上位机IP、UDP上/下行1700、防火墙、PULL_DATA和gw日志 |
| 没有节点状态 | 节点FAULT、频率、SF6/125k、私有同步字、IQ、物理CRC |
| 网关报TX_FREQ错误 | 原射频TX范围是否包含470.3MHz；完成厂家确认与硬件实测 |
| TX_ACK成功但节点没确认 | 节点是否待机监听控制频点，SF/IQ一致，确认时隙是否碰撞 |
| 开始被拒绝 | GNSS/PPS健康、60秒实测clock_ok、目标时间至少在未来1秒 |
| 收到但无法解析 | 必须固件v3和本上位机配套；旧v2数据不能混用 |
| 丢样/队列增长 | 先用USB测试区分传感器读取与无线容量，再查ACK窗口和干扰 |

恢复网关：`gw stop`，还原备份配置及原软链接，再`gw start`。代码回退：在新基线保存后，对指定阶段提交执行git revert，按匹配环境重新烧录。
