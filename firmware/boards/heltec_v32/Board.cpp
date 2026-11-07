#include "Board.h"

#include <Arduino.h>
namespace Board {
void begin() {
  pinMode(36, OUTPUT);
  digitalWrite(36, LOW);  // Heltec Vext active low.
}
}  // namespace Board
