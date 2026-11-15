#include <Arduino.h>
#include <SPI.h>

// EoRa-S3-400TB external header. Sensor module VCC must use the 3.3V VDD pin.
constexpr int PIN_SCK = 47;
constexpr int PIN_MOSI = 48;
constexpr int PIN_MISO = 39;
#ifndef SIX_WIRE_CS_PIN
#define SIX_WIRE_CS_PIN 40
#endif
constexpr int PIN_CS = SIX_WIRE_CS_PIN;
constexpr uint8_t IIM42652_WHO_AM_I = 0x75;
constexpr uint8_t IIM42652_ID = 0x6F;

SPIClass sensorSpi(HSPI);

bool scanBusy = false;

uint8_t readWho(uint32_t hz, uint8_t mode) {
  sensorSpi.beginTransaction(SPISettings(hz, MSBFIRST, mode));
  digitalWrite(PIN_CS, LOW);
  delayMicroseconds(5);
  sensorSpi.transfer(IIM42652_WHO_AM_I | 0x80);  // 0xF5 for a read.
  const uint8_t value = sensorSpi.transfer(0x00);
  delayMicroseconds(2);
  digitalWrite(PIN_CS, HIGH);
  sensorSpi.endTransaction();
  delayMicroseconds(50);
  return value;
}

void printResult(uint32_t hz, uint8_t mode, const uint8_t* values, size_t count) {
  uint8_t matches = 0;
  for (size_t i = 0; i < count; ++i)
    matches += values[i] == IIM42652_ID;

  char sequence[80];
  size_t offset = 0;
  sequence[0] = 0;
  for (size_t i = 0; i < count && offset + 5 < sizeof(sequence); ++i)
    offset += snprintf(sequence + offset, sizeof(sequence) - offset, "%s%02X",
                       i ? " " : "", values[i]);

  Serial.printf("RESULT,mode=%u,hz=%lu,matches=%u,reads=%u,ok=%u,values=%s\n",
                mode, static_cast<unsigned long>(hz), matches,
                static_cast<unsigned>(count), matches == count ? 1 : 0, sequence);
}

void scan() {
  if (scanBusy)
    return;
  scanBusy = true;
  digitalWrite(PIN_CS, HIGH);
  sensorSpi.end();
  pinMode(PIN_CS, OUTPUT);
  digitalWrite(PIN_CS, HIGH);
  pinMode(PIN_SCK, OUTPUT);
  digitalWrite(PIN_SCK, LOW);
  pinMode(PIN_MOSI, OUTPUT);
  digitalWrite(PIN_MOSI, LOW);
  pinMode(PIN_MISO, INPUT);
  delay(10);

  auto outputTest = [](int pin, const char* name) {
    pinMode(pin, OUTPUT);
    digitalWrite(pin, LOW);
    delay(2);
    const int lowRead = digitalRead(pin);
    digitalWrite(pin, HIGH);
    delay(2);
    const int highRead = digitalRead(pin);
    Serial.printf("PIN_TEST,name=%s,low_read=%u,high_read=%u,ok=%u\n",
                  name, lowRead, highRead,
                  lowRead == LOW && highRead == HIGH ? 1 : 0);
  };
  outputTest(PIN_CS, "CS");
  outputTest(PIN_SCK, "SCK");
  outputTest(PIN_MOSI, "MOSI");
  digitalWrite(PIN_CS, HIGH);
  digitalWrite(PIN_SCK, LOW);
  digitalWrite(PIN_MOSI, LOW);

  pinMode(PIN_MISO, INPUT_PULLUP);
  delay(2);
  const int misoPullUp = digitalRead(PIN_MISO);
  pinMode(PIN_MISO, INPUT_PULLDOWN);
  delay(2);
  const int misoPullDown = digitalRead(PIN_MISO);
  pinMode(PIN_MISO, INPUT);
  delay(2);
  Serial.printf("MISO_TEST,pull_up=%u,pull_down=%u,floating=%u\n",
                misoPullUp, misoPullDown,
                misoPullUp == HIGH && misoPullDown == LOW ? 1 : 0);
  Serial.printf("LEVEL,miso_idle=%u,cs_high=%u\n", digitalRead(PIN_MISO),
                digitalRead(PIN_CS));

  sensorSpi.begin(PIN_SCK, PIN_MISO, PIN_MOSI, PIN_CS);
  pinMode(PIN_CS, OUTPUT);
  digitalWrite(PIN_CS, HIGH);
  pinMode(PIN_SCK, OUTPUT);
  digitalWrite(PIN_SCK, LOW);
  pinMode(PIN_MOSI, OUTPUT);
  digitalWrite(PIN_MOSI, LOW);
  pinMode(PIN_MISO, INPUT);
  delay(5);
  Serial.printf("LEVEL_AFTER_SPI,miso_idle=%u,cs_high=%u\n",
                digitalRead(PIN_MISO), digitalRead(PIN_CS));
  Serial.println("SCAN_BEGIN,register=0x75,read_command=0xF5,expected=0x6F");

  struct Candidate { uint32_t hz; uint8_t mode; };
  const Candidate candidates[] = {
      {100000UL, SPI_MODE0},
      {1000000UL, SPI_MODE0},
      {100000UL, SPI_MODE3},
      {1000000UL, SPI_MODE3},
  };

  uint8_t anyMatches = 0;
  uint8_t anyStable = 0;
  for (const Candidate& candidate : candidates) {
    uint8_t values[8] = {};
    uint8_t matches = 0;
    for (uint8_t i = 0; i < 8; ++i) {
      values[i] = readWho(candidate.hz, candidate.mode);
      matches += values[i] == IIM42652_ID;
    }
    printResult(candidate.hz, candidate.mode, values, 8);
    anyMatches += matches;
    if (matches == 8)
      anyStable = 1;
  }

  Serial.printf("SCAN_END,matched=%u,stable_combination=%u,ok=%u\n", anyMatches,
                anyStable, anyStable ? 1 : 0);
  if (anyStable)
    Serial.println("SIX_WIRE_OK,who=0x6F,next=reconnect_INT1_and_CLKIN");
  else
    Serial.println("SIX_WIRE_FAIL,who!=0x6F,next=measure_3V3_CSB_MISO_at_module");
  scanBusy = false;
}

void printHelp() {
  Serial.println("COMMANDS: SCAN; READ; HELP");
}

void setup() {
  Serial.begin(460800);
  const uint32_t started = millis();
  while (!Serial && millis() - started < 3000)
    delay(10);
  delay(500);

  pinMode(PIN_CS, OUTPUT);
  digitalWrite(PIN_CS, HIGH);
  pinMode(PIN_SCK, OUTPUT);
  digitalWrite(PIN_SCK, LOW);
  pinMode(PIN_MOSI, OUTPUT);
  digitalWrite(PIN_MOSI, LOW);
  pinMode(PIN_MISO, INPUT);
  sensorSpi.begin(PIN_SCK, PIN_MISO, PIN_MOSI, PIN_CS);

  Serial.println();
  Serial.println("BOOT,minimal_iim42652_six_wire=1,board=EoRa-S3-400TB");
  Serial.printf("WIRING,vcc=3V3,gnd=GND,sck=%d,mosi=%d,miso=%d,cs=%d\n",
                PIN_SCK, PIN_MOSI, PIN_MISO, PIN_CS);
  Serial.println("SAFETY,int1=not_connected,clkin=not_connected,gnss=not_connected");
  Serial.println("SAFETY,only_WHO_AM_I_read,no_register_write,no_sensor_reset");
  printHelp();
  scan();
}

void loop() {
  if (!Serial.available())
    return;
  String command = Serial.readStringUntil('\n');
  command.trim();
  command.toUpperCase();
  if (command == "SCAN") {
    scan();
  } else if (command == "READ") {
    const uint8_t who = readWho(100000UL, SPI_MODE0);
    Serial.printf("READ,mode=0,hz=100000,who=0x%02X,ok=%u\n", who,
                  who == IIM42652_ID ? 1 : 0);
  } else if (command == "HELP") {
    printHelp();
  } else if (command.length()) {
    Serial.println("ERROR,unknown_command");
  }
}
