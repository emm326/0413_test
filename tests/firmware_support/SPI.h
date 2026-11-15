#pragma once
#include <cstdint>
constexpr uint8_t HSPI=2, MSBFIRST=1, SPI_MODE0=0, SPI_MODE3=3;
struct SPISettings {
  uint32_t hz; uint8_t order,mode;
  SPISettings(uint32_t h,uint8_t o,uint8_t m):hz(h),order(o),mode(m){}
};
class SPIClass {
 public:
  explicit SPIClass(uint8_t bus);
  void begin(int sck,int miso,int mosi,int cs);
  void end();
  void beginTransaction(SPISettings settings);
  void endTransaction();
  uint8_t transfer(uint8_t value);
 private:
  bool started_=false,command_=true,reading_=false;
  uint8_t address_=0;
};
