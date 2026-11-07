#pragma once
namespace Board {
constexpr const char* name = "Heltec V3.2 LF";
constexpr int radioSck = 9, radioMiso = 11, radioMosi = 10, radioCs = 8;
constexpr int radioReset = 12, radioBusy = 13, radioDio1 = 14;
constexpr float tcxoVoltage = 1.6f;
constexpr int sensorSck = 47, sensorMiso = 33, sensorMosi = 48, sensorCs = 34;
constexpr int sensorInt = 26, clockOut = 3;
constexpr int gpsRx = 5, gpsTx = 6, gpsPps = 4;
void begin();
}  // namespace Board
