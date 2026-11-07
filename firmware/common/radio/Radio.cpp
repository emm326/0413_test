#include "Radio.h"

#include <RadioLib.h>
#include <SPI.h>

#include "Config.h"
#include "SelectedBoard.h"
namespace Radio {
namespace {
#if defined(APP_BOARD_EORA)
SX1268 chip = new Module(Board::radioCs, Board::radioDio1, Board::radioReset, Board::radioBusy);
#else
SX1262 chip = new Module(Board::radioCs, Board::radioDio1, Board::radioReset, Board::radioBusy);
#endif
bool tx = false;
uint32_t deadline = 0;
float currentFrequency = 0;
uint8_t currentSf = 0;
int16_t error = 0, recovery = 0;
bool arm() {
  recovery = chip.startReceive();
  return recovery == RADIOLIB_ERR_NONE;
}
}  // namespace
bool begin() {
  SPI.begin(Board::radioSck, Board::radioMiso, Board::radioMosi, Board::radioCs);
  error = chip.begin(Config::controlMHz, Config::bandwidthKHz, Config::spreadingFactor,
                     Config::codingRate, Config::syncWord, Config::powerDbm, Config::preamble,
                     Board::tcxoVoltage);
  if (error != RADIOLIB_ERR_NONE)
    return false;
  if ((error = chip.setCRC(true)) != RADIOLIB_ERR_NONE)
    return false;
  currentFrequency = Config::controlMHz;
  currentSf = Config::spreadingFactor;
  return arm();
}
bool channel(float frequency, uint8_t sf) {
  if (tx)
    return false;
  if (currentFrequency == frequency && currentSf == sf)
    return true;
  if ((error = chip.standby()) != RADIOLIB_ERR_NONE)
    return false;
  if ((error = chip.setFrequency(frequency)) != RADIOLIB_ERR_NONE)
    return false;
  if ((error = chip.setSpreadingFactor(sf)) != RADIOLIB_ERR_NONE)
    return false;
  currentFrequency = frequency;
  currentSf = sf;
  return arm();
}
bool send(const uint8_t* bytes, size_t length) {
  if (tx || !bytes || !length || length > 255)
    return false;
  error = chip.startTransmit(bytes, length);
  if (error != RADIOLIB_ERR_NONE) {
    arm();
    return false;
  }
  tx = true;
  deadline = millis() + 100 + chip.getTimeOnAir(length) / 1000;
  return true;
}
void update() {
  if (!tx)
    return;
  if (digitalRead(Board::radioDio1) == LOW && int32_t(millis() - deadline) < 0)
    return;
  const bool timedOut = digitalRead(Board::radioDio1) == LOW;
  error = chip.finishTransmit();
  if (timedOut)
    error = RADIOLIB_ERR_TX_TIMEOUT;
  tx = false;
  arm();
}
bool receive(uint8_t* bytes, size_t& length, int16_t& rssi, float& snr) {
  length = 0;
  if (tx)
    return false;
  if (recovery != RADIOLIB_ERR_NONE) {
    arm();
    return false;
  }
  if (digitalRead(Board::radioDio1) == LOW)
    return false;
  length = chip.getPacketLength();
  if (!length || length > 255) {
    error = RADIOLIB_ERR_PACKET_TOO_LONG;
    length = 0;
    arm();
    return false;
  }
  error = chip.readData(bytes, length);
  if (error != RADIOLIB_ERR_NONE) {
    length = 0;
    arm();
    return false;
  }
  rssi = int16_t(chip.getRSSI());
  snr = chip.getSNR();
  arm();
  return true;
}
bool transmitting() {
  return tx;
}
int16_t lastError() {
  return error;
}
int16_t recoveryError() {
  return recovery;
}
}  // namespace Radio
