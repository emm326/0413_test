#include "Board.h"

#include <Arduino.h>
namespace Board {
void begin() {
  pinMode(21, OUTPUT);
  digitalWrite(21, HIGH);  // E22_EN drives the board's LoRa power switch.
  delay(10);
}
}  // namespace Board
