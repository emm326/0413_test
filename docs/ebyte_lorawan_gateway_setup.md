# Ebyte E870-L470LG11 LoRaWAN Gateway Setup

This project has two radio paths now:

- Legacy P2P: `heltec_wifi_lora_32_V3_tx` and `heltec_wifi_lora_32_V3_rx`
- LoRaWAN gateway path: `heltec_wifi_lora_32_V3_lorawan_tx_1001` to `heltec_wifi_lora_32_V3_lorawan_tx_1004`

Use the LoRaWAN environments with the Ebyte E870-L470LG11 gateway. The gateway is an SX1302 CN470 LoRaWAN gateway, so it cannot directly receive the old RadioLib P2P packets.

## Hardware Choice

Use `E870-L470LG11`.

Do not use `E870-L868LG12` or `E870-L915LG12` for this project, because those are 868 MHz and 915 MHz variants. This project is configured for the CN470 / 470 MHz direction.

## Firmware Build Environments

The default build environment is:

```text
heltec_wifi_lora_32_V3_lorawan_tx_1001
```

Available LoRaWAN node environments:

```text
heltec_wifi_lora_32_V3_lorawan_tx_1001
heltec_wifi_lora_32_V3_lorawan_tx_1002
heltec_wifi_lora_32_V3_lorawan_tx_1003
heltec_wifi_lora_32_V3_lorawan_tx_1004
```

Each environment sets:

```text
APP_DEVICE_ID_VALUE
APP_LORAWAN_DEV_ADDR_VALUE
APP_LORAWAN_FNWKSINT_KEY_VALUE
APP_LORAWAN_SNWKSINT_KEY_VALUE
APP_LORAWAN_NWKSENC_KEY_VALUE
APP_LORAWAN_APPS_KEY_VALUE
```

The keys in `platformio.ini` are placeholders. Replace them with the same ABP keys configured on the LoRaWAN network server or gateway platform before field testing.

## Current LoRaWAN Defaults

```text
Region: CN470
Activation: ABP
Uplink type: unconfirmed
Sub-band: 1
Data rate: DR5
FPort: 10
Max samples per packet: 37
Payload protocol: existing binary vibration frame
```

The application payload is still:

```text
dev_id, seq, t0_ms, fs_x10, count, ax_raw, ay_raw, az_raw...
```

The PC script validates the inner CRC16 and then reconstructs every sample time from `t0_ms` and `fs_x10`.

## Gateway / Server Configuration

Configure the E870-L470LG11 as a CN470 LoRaWAN gateway and connect it to a network server or gateway forwarding mode that can output uplink JSON by MQTT, HTTP, or TCP.

For each TX node, register an ABP device:

```text
TX1001:
  DevAddr: 26011001
  FPort: 10
  Keys: replace the placeholders in platformio.ini and use the same values here

TX1002:
  DevAddr: 26011002

TX1003:
  DevAddr: 26011003

TX1004:
  DevAddr: 26011004
```

If the gateway receives nothing, first check that the gateway CN470 channel plan matches `APP_LORAWAN_SUB_BAND_VALUE`. Try changing the sub-band in `platformio.ini` if the gateway uses a different CN470 channel group.

## PC CSV Script

Use:

```text
tools/lorawan_gateway_to_csv.py
```

MQTT example:

```powershell
python tools\lorawan_gateway_to_csv.py --mqtt-host 192.168.1.10 --mqtt-topic "application/+/device/+/event/up" --output data
```

HTTP example:

```powershell
python tools\lorawan_gateway_to_csv.py --http --http-port 8080 --http-path /uplink --output data
```

TCP example:

```powershell
python tools\lorawan_gateway_to_csv.py --tcp-port 9000 --output data
```

The default CSV output is:

```text
data/sensor1001/YYYYMMDD/HHMMSS_to_HHMMSS.csv
data/sensor1002/YYYYMMDD/HHMMSS_to_HHMMSS.csv
```

CSV rows are:

```text
datetime,ax,ay,az
2026/4/18 19:52:21.5126,0.631800,0.058500,0.838500
```

A quality log is also written by default:

```text
data/quality.csv
```

It records `dev_id`, `seq`, `count`, `fs_hz`, `rssi`, `snr`, `freq`, `fcnt`, `lost_packets`, and payload length.

## Validation Order

1. Register only `TX1001` on the gateway/server.
2. Flash `heltec_wifi_lora_32_V3_lorawan_tx_1001`.
3. Confirm the gateway/server shows uplinks from `DevAddr 26011001`.
4. Start `tools/lorawan_gateway_to_csv.py` and confirm `data/sensor1001` is created.
5. Add `TX1002` to `TX1004` one by one.
6. Test `25 Hz`, `50 Hz`, and `100 Hz` first.
7. Test `200 Hz` last and inspect `data/quality.csv` for packet loss.

## Throughput Warning

Four nodes at 200 Hz with raw three-axis samples is heavy for LoRaWAN. The SX1302 gateway can receive multiple channels, but the air interface is still the bottleneck. Treat 200 Hz as an experimental mode until field tests show acceptable `seq` continuity and packet loss.
