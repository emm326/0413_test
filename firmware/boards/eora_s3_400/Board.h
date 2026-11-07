#pragma once
namespace Board {
constexpr const char* name = "EoRa-S3-400TB";
constexpr int radioSck = 5, radioMiso = 3, radioMosi = 6, radioCs = 7;
constexpr int radioReset = 8, radioBusy = 34, radioDio1 = 33;
constexpr float tcxoVoltage = 0.0f;  // E22 passive crystal.
constexpr int sensorSck = 47, sensorMiso = 39, sensorMosi = 48, sensorCs = 40;
constexpr int sensorInt = 38, clockOut = 36;
constexpr int gpsRx = 16, gpsTx = 15, gpsPps = 41;
void begin();
}  // namespace Board
