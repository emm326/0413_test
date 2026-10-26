#include <iostream>
#include <stdexcept>
#include <string>
#include <RadioLib.h>
#include "lora_transport.h"
#include "GpsTimebase.h"
void check(bool ok, const char* message) {
  if (!ok) throw std::runtime_error(message);
}
int main(int argc, char** argv) {
  try {
    check(argc == 2, "test case required");
    const std::string name = argv[1];
    if (name == "gps_budget") {
      HardwareSerial serial;
      serial.input = std::string(500, 'x') + "\n$GPRMC,120000.00,A,2230.0,N,11400.0,E,0,0,261026,,,A*00\r\n";
      GpsTimebase gps(serial);
      gps.update();
      check(serial.available() > 0, "GPS update drained its entire backlog instead of yielding to radio");
      check(!gps.fixValid(), "split RMC sentence should not be parsed yet");
      gps.update();
      check(gps.fixValid(), "RMC parsing must survive the per-loop budget boundary");
      return 0;
    }
    fakeRadio = {};
    testDio = LOW;
    check(LoRaTransport::begin() && LoRaTransport::startReceive(), "radio init");
    testMillis += 10;
    uint8_t buffer[32] = {};
    size_t length = 0;
    LoRaTransport::ReceiveMeta meta = {};
    if (name == "dio_fallback") {
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_RX_DONE;
      check(LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "RX_DONE was ignored when DIO1 was low");
      check(length == 4 && buffer[0] == 0x42 && meta.rssi == -70, "packet and metadata must be preserved");
    } else if (name == "crc_error") {
      testDio = HIGH;
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_RX_DONE | RADIOLIB_SX126X_IRQ_CRC_ERR;
      fakeRadio.readState = RADIOLIB_ERR_CRC_MISMATCH;
      check(!LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "corrupt packet accepted");
      check(LoRaTransport::lastState() == RADIOLIB_ERR_CRC_MISMATCH, "receive restart erased CRC error");
      check(fakeRadio.arms == 2 && length == 0, "CRC error must rearm RX without delivering data");
      fakeRadio.readState = 0;
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_RX_DONE;
      check(LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "next valid packet was lost after recovery");
    } else if (name == "timeout_recovery") {
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_TIMEOUT;
      check(!LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "timeout treated as packet");
      check(fakeRadio.arms == 2 && fakeRadio.reads == 0, "timeout must rearm without reading stale payload");
      check(LoRaTransport::lastState() == RADIOLIB_ERR_RX_TIMEOUT, "timeout error erased");
    } else if (name == "header_recovery") {
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_HEADER_ERR;
      check(!LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "bad header accepted");
      check(fakeRadio.arms == 2 && fakeRadio.reads == 0, "header error must rearm RX");
      check(LoRaTransport::lastState() == RADIOLIB_ERR_LORA_HEADER_DAMAGED, "header error erased");
    } else if (name == "rearm_failure") {
      testDio = HIGH;
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_RX_DONE;
      fakeRadio.readState = RADIOLIB_ERR_CRC_MISMATCH;
      fakeRadio.armState = RADIOLIB_ERR_SPI_CMD_FAILED;
      check(!LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "corrupt packet accepted");
      check(LoRaTransport::lastState() == RADIOLIB_ERR_SPI_CMD_FAILED, "RX restart failure must be reported");
      fakeRadio.armState = 0;
      testDio = LOW;
      LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta);
      check(fakeRadio.arms == 3, "failed RX restart must retry next iteration");
    } else if (name == "oversized_packet") {
      testDio = HIGH;
      fakeRadio.irq = RADIOLIB_SX126X_IRQ_RX_DONE;
      fakeRadio.length = sizeof(buffer) + 1;
      check(!LoRaTransport::receivePacket(buffer, sizeof(buffer), length, meta), "oversized packet accepted");
      check(LoRaTransport::lastState() == RADIOLIB_ERR_PACKET_TOO_LONG && fakeRadio.reads == 0, "oversized packet error erased or buffer overwritten");
    } else {
      throw std::runtime_error("unknown test case");
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
