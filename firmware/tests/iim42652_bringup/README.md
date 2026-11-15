# IIM42652 六线启动测试

本目录是独立测试入口，不引用旧 `IimBase`。它使用原厂 Ixm42xxx 1.1.1 驱动，验证 IIM42652 能否通过 SPI 完成初始化并开始加速度采样。

## 接线

| IIM42652 模块 | EoRa-S3-400TB |
|---|---|
| VCC | 3.3V |
| GND | GND |
| SCK | GPIO47 |
| MOSI | GPIO48 |
| MISO | GPIO39 |
| CSB | GPIO40 |

本测试不接 INT1、CLKIN、GNSS 和 LoRa。

## 构建与烧录

```powershell
pio run -e eora_s3_400_iim42652_bringup
pio run -e eora_s3_400_iim42652_bringup -t upload --upload-port COM8
pio device monitor -p COM8 -b 460800
```

烧录端口按实际电脑端口替换。固件启动后先打印 5 秒倒计时，便于烧录后接入串口监视器。测试阶段不要提交 Git。

## 串口判断

1. `PIN_IDLE` 打印 MISO 空闲电平，只作现场记录，不作为通过条件。
2. `PREFLIGHT` 依次尝试 HSPI/FSPI、Mode0/Mode3、100kHz/2MHz 和 GPIO 软件 SPI。
3. 某个组合连续 3 次读到 `0x6F` 时打印 `PREFLIGHT_SELECT`。
4. 预检通过后调用原厂 `inv_ixm42xxx_init()`，再关闭 FIFO、读取 WHO_AM_I。
5. 配置加速度为 ±4g、50Hz、低噪声模式，等待 20ms，轮询寄存器 5 秒。
6. `BRINGUP_RESULT,result=PASS` 表示采样率达到 40–60Hz、样本不少于 180、SPI 无错误且加速度已关闭。

## 输出示例

```text
PREFLIGHT,config=hspi_mode0_2m,who1=0x6F,who2=0x6F,who3=0x6F,stable=1
PREFLIGHT_SELECT,config=hspi_mode0_2m
DRIVER_STEP,vendor_init,rc=0,OK
WHO_AM_I,value=0x6F,expected=0x6F,ok=1
SAMPLE,index=1,dt_us=20013,x=12,y=-34,z=2044,temp_raw=...,magnitude_g=1.0002
SAMPLING,samples=250,elapsed_us=5000000,rate_hz=50.000,spi_errors=0,...
BRINGUP_RESULT,result=PASS,samples=250,rate_hz=50.000,mean_g=1.0000
```

`mean_g` 是 XYZ 合向量均值。静止时应接近 1g。DS-000440 规定 ±4g 的灵敏度为 8192 LSB/g；该项用于判断数值是否合理，不单独决定测试通过。