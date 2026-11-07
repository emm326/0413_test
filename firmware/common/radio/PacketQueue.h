#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "Config.h"
class PacketQueue {
 public:
  struct Entry {
    uint8_t bytes[180];
    uint16_t length = 0;
    uint32_t sequence = 0, lastSent = 0;
    bool used = false, sent = false;
  };
  bool add(const uint8_t* bytes, size_t length, uint32_t sequence) {
    for (auto& e : entries_)
      if (!e.used) {
        memcpy(e.bytes, bytes, length);
        e.length = length;
        e.sequence = sequence;
        e.used = true;
        e.sent = false;
        e.lastSent = 0;
        return true;
      }
    return false;
  }
  Entry* next(uint32_t now, bool retry) {
    Entry* best = nullptr;
    for (auto& e : entries_)
      if (e.used && (!e.sent || (retry && now - e.lastSent >= 3000)) &&
          (!best || e.sequence < best->sequence))
        best = &e;
    return best;
  }
  void acknowledge(uint32_t first, uint32_t bitmap) {
    for (auto& e : entries_)
      if (e.used && e.sequence >= first && e.sequence - first < 32 &&
          (bitmap & (uint32_t(1) << (e.sequence - first))))
        e.used = false;
  }
  uint16_t size() const {
    uint16_t count = 0;
    for (const auto& e : entries_)
      count += e.used;
    return count;
  }
  void clear() {
    for (auto& e : entries_)
      e.used = false;
  }

 private:
  Entry entries_[Config::queueCapacity];
};
