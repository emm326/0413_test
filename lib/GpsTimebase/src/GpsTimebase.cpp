#include "GpsTimebase.h"

#include <stdlib.h>
#include <string.h>

GpsTimebase* GpsTimebase::activeInstance_ = nullptr;

GpsTimebase::GpsTimebase(HardwareSerial& serial)
    : serial_(serial),
      ppsPin_(-1),
      line_{},
      lineLen_(0),
      fixValid_(false),
      timeValid_(false),
      lastNmeaEpochUs_(0),
      lastNmeaLocalMs_(0),
      lastPpsEpochUs_(0),
      lastPpsLocalUs_(0),
      lastPpsIntervalUs_(0),
      ppsCount_(0),
      consumedPpsCount_(0) {}

bool GpsTimebase::begin(int8_t rxPin, int8_t txPin, int8_t ppsPin, uint32_t baud) {
  ppsPin_ = ppsPin;
  serial_.begin(baud, SERIAL_8N1, rxPin, txPin);
  pinMode(ppsPin_, INPUT);
  activeInstance_ = this;
  attachInterrupt(digitalPinToInterrupt(ppsPin_), GpsTimebase::onPpsThunk, RISING);
  return true;
}

void GpsTimebase::update() {
  while (serial_.available() > 0) {
    const char c = static_cast<char>(serial_.read());
    if (c == '\r') {
      continue;
    }
    if (c == '\n') {
      line_[lineLen_] = '\0';
      if (lineLen_ > 0) {
#if defined(APP_DEBUG_GPS_NMEA)
        Serial.print(F("GPS NMEA: "));
        Serial.println(line_);
#endif
        processLine(line_);
      }
      lineLen_ = 0;
      line_[0] = '\0';
      continue;
    }
    if (lineLen_ + 1 < sizeof(line_)) {
      line_[lineLen_++] = c;
    } else {
      lineLen_ = 0;
      line_[0] = '\0';
    }
  }

  uint32_t ppsCount = 0;
  uint32_t ppsLocalUs = 0;
  noInterrupts();
  ppsCount = ppsCount_;
  ppsLocalUs = lastPpsLocalUs_;
  interrupts();

  if (ppsCount == consumedPpsCount_) {
    return;
  }
  consumedPpsCount_ = ppsCount;

  if (!fixValid_ || lastNmeaEpochUs_ == 0 || millis() - lastNmeaLocalMs_ > 2000UL) {
    return;
  }

  const uint64_t nmeaSec = lastNmeaEpochUs_ / 1000000ULL;
  lastPpsEpochUs_ = (nmeaSec + 1ULL) * 1000000ULL;
  lastPpsLocalUs_ = ppsLocalUs;
  timeValid_ = true;
}

bool GpsTimebase::fixValid() const { return fixValid_; }

bool GpsTimebase::ppsValid() const { return ppsCount_ >= 2 && lastPpsIntervalUs_ > 900000UL && lastPpsIntervalUs_ < 1100000UL; }

bool GpsTimebase::timeValid() const { return timeValid_ && fixValid() && ppsValid(); }

bool GpsTimebase::localMicrosForEpoch(uint64_t targetEpochUs, uint32_t& outLocalUs) const {
  if (!timeValid() || lastPpsEpochUs_ == 0) {
    return false;
  }

  if (targetEpochUs <= lastPpsEpochUs_) {
    outLocalUs = micros();
    return true;
  }

  const uint64_t deltaUs = targetEpochUs - lastPpsEpochUs_;
  if (deltaUs > 0x7FFFFFFFULL) {
    return false;
  }

  outLocalUs = lastPpsLocalUs_ + static_cast<uint32_t>(deltaUs);
  return true;
}

uint64_t GpsTimebase::epochUsAtLastPps() const { return lastPpsEpochUs_; }

uint32_t GpsTimebase::ppsIntervalUs() const { return lastPpsIntervalUs_; }

void IRAM_ATTR GpsTimebase::onPpsThunk() {
  if (activeInstance_ != nullptr) {
    activeInstance_->handlePpsIsr();
  }
}

void IRAM_ATTR GpsTimebase::handlePpsIsr() {
  const uint32_t nowUs = micros();
  if (lastPpsLocalUs_ != 0) {
    lastPpsIntervalUs_ = nowUs - lastPpsLocalUs_;
  }
  lastPpsLocalUs_ = nowUs;
  ++ppsCount_;
}

void GpsTimebase::processLine(char* line) {
  if (line == nullptr || line[0] != '$') {
    return;
  }
  if (strncmp(line + 3, "RMC", 3) == 0) {
    (void)parseRmc(line);
  }
}

bool GpsTimebase::parseRmc(char* line) {
  char* fields[13] = {};
  size_t count = 0;
  char* token = strtok(line, ",");
  while (token != nullptr && count < sizeof(fields) / sizeof(fields[0])) {
    fields[count++] = token;
    token = strtok(nullptr, ",");
  }

  if (count < 10 || fields[1] == nullptr || fields[2] == nullptr || fields[9] == nullptr) {
    return false;
  }
  if (fields[2][0] != 'A') {
    fixValid_ = false;
    return false;
  }

  uint64_t epochUs = 0;
  if (!parseUtcDateTime(fields[1], fields[9], epochUs)) {
    return false;
  }

  fixValid_ = true;
  lastNmeaEpochUs_ = epochUs;
  lastNmeaLocalMs_ = millis();
  return true;
}

bool GpsTimebase::parseUtcDateTime(const char* timeText, const char* dateText, uint64_t& outEpochUs) {
  outEpochUs = 0;
  if (timeText == nullptr || dateText == nullptr || strlen(timeText) < 6 || strlen(dateText) != 6) {
    return false;
  }

  const int hour = (timeText[0] - '0') * 10 + (timeText[1] - '0');
  const int minute = (timeText[2] - '0') * 10 + (timeText[3] - '0');
  const int second = (timeText[4] - '0') * 10 + (timeText[5] - '0');
  const int day = (dateText[0] - '0') * 10 + (dateText[1] - '0');
  const int month = (dateText[2] - '0') * 10 + (dateText[3] - '0');
  const int yearTwo = (dateText[4] - '0') * 10 + (dateText[5] - '0');
  const int year = yearTwo >= 80 ? 1900 + yearTwo : 2000 + yearTwo;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 60 ||
      month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  uint32_t fractionalUs = 0;
  const char* dot = strchr(timeText, '.');
  if (dot != nullptr) {
    uint32_t scale = 100000UL;
    ++dot;
    while (*dot >= '0' && *dot <= '9' && scale > 0) {
      fractionalUs += static_cast<uint32_t>(*dot - '0') * scale;
      scale /= 10UL;
      ++dot;
    }
  }

  const int64_t days = daysFromCivil(year, static_cast<unsigned>(month), static_cast<unsigned>(day));
  const int64_t secondsOfDay = static_cast<int64_t>(hour) * 3600LL + minute * 60LL + second;
  outEpochUs = static_cast<uint64_t>((days * 86400LL + secondsOfDay) * 1000000LL) + fractionalUs;
  return true;
}

int64_t GpsTimebase::daysFromCivil(int year, unsigned month, unsigned day) {
  year -= month <= 2;
  const int era = (year >= 0 ? year : year - 399) / 400;
  const unsigned yoe = static_cast<unsigned>(year - era * 400);
  const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return static_cast<int64_t>(era) * 146097LL + static_cast<int64_t>(doe) - 719468LL;
}
