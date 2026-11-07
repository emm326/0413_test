#include "common/app/NodeApp.h"
NodeApp application;
void setup() {
  application.begin();
}
void loop() {
  application.tick();
}
