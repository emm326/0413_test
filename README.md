# 双开发板三轴加速度无线采集

当前固件v3：EoRa-S3-400TB / Heltec V3.2 LF × IIM-42352 / IIM-42652，ESP32 32000Hz CLKIN、GNSS/PPS、50/100Hz、25组数据包。

- [项目计划与进度](PROJECT_PLAN.md)
- [四种组合接线及烧录](docs/wiring_and_flash.md)
- [E870网关和上位机](docs/e870_setup.md)
- [v3协议](docs/protocol_v3.md)
- [每轮变更报告](docs/changes/)

```powershell
pio run -e heltec_v32_iim42352_tx
python -m host --nodes 1001,1002
python -m unittest discover -s tests -p test_e870_v3.py -v
```

当前测试与硬件验收状态见PROJECT_PLAN.md。旧版入口为platformio_legacy.ini。
