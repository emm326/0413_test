#include <iostream>
#include <stdexcept>
#include <cstring>
#include "simple_wireless_protocol.h"
#include "IIM42352Driver.h"
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
int main(int argc, char** argv) {
  try {
    check(argc == 2, "case required");
    std::string name = argv[1];
    namespace Link = SimpleWireless;
    uint8_t bytes[Link::MAX_PACKET_SIZE] = {};
    size_t length = 0;
    Link::Sample samples[Link::SAMPLES_PER_PACKET] = {};
    for (unsigned i=0;i<Link::SAMPLES_PER_PACKET;++i) samples[i] = {int16_t(i-5),int16_t(i*100),int16_t(-30000+i)};
    const uint64_t timestamp = (1ULL << 40) + 123456;
    Link::Packet decoded = {};
    if (name == "data_roundtrip") {
      check(Link::encode(Link::Status::Data,0x6D,65535,timestamp,samples,10,bytes,sizeof(bytes),length),"encode data");
      check(Link::decode(bytes,length,decoded),"decode data");
      check(decoded.header.firstSampleUs==timestamp && decoded.header.seq==65535 && decoded.header.count==10,"64-bit timestamp/sequence/count changed");
      check(decoded.header.samplePeriodUs==20000 && decoded.header.rangeG==4,"sample configuration changed");
      check(std::memcmp(samples,decoded.samples,sizeof(samples))==0,"sample values changed");
    } else if (name == "crc_and_length") {
      check(Link::encode(Link::Status::Data,0x6D,1,timestamp,samples,10,bytes,sizeof(bytes),length),"encode data");
      for(size_t n=0;n<length;++n) check(!Link::decode(bytes,n,decoded),"truncated packet accepted");
      bytes[sizeof(Link::Header)+2]^=1;
      check(!Link::decode(bytes,length,decoded),"corrupt payload accepted");
    } else if (name == "sensor_fault") {
      check(Link::encode(Link::Status::SensorInitFailed,0xFF,9,0,nullptr,0,bytes,sizeof(bytes),length),"encode fault");
      check(Link::decode(bytes,length,decoded),"decode fault");
      check(decoded.header.count==0 && decoded.header.sensorWho==0xFF && decoded.header.status==1,"fault became sample data");
      check(!Link::encode(Link::Status::SensorInitFailed,0xFF,9,0,samples,1,bytes,sizeof(bytes),length),"fault accepted sample payload");
      check(!Link::encode(Link::Status::Data,0x6D,9,0,nullptr,0,bytes,sizeof(bytes),length),"empty data packet accepted");
    } else if (name == "capacity_and_metadata") {
      check(!Link::encode(Link::Status::Data,0x6D,1,timestamp,samples,10,bytes,5,length),"output buffer overflow");
      check(!Link::encode(Link::Status::Data,0x6D,1,timestamp,samples,11,bytes,sizeof(bytes),length),"oversized count accepted");
      check(Link::encode(Link::Status::Data,0x6D,1,timestamp,samples,10,bytes,sizeof(bytes),length),"encode data");
      bytes[offsetof(Link::Header,rangeG)]=16;
      uint16_t crc=Protocol::crc16Ccitt(bytes,length-2);
      std::memcpy(bytes+length-2,&crc,2);
      check(!Link::decode(bytes,length,decoded),"unexpected range would silently mis-scale samples");
    } else {
      SPIClass spi;
      IIM42352Driver sensor(spi,47,33,48,34);
      if (name == "internal_clock") {
        check(sensor.begin(false),"internal init failed");
        check(sensor.internalClockConfigured() && !sensor.externalClockConfigured(),"internal clock still requires RTC_MODE");
        check((spi.registers[1][0x7B]&0x06)==0x04,"pin9 must remain an input when external clock wire is attached");
        sensor.setDataRate(IIM42352Driver::DataRate::Hz50);
        check(sensor.restartAccel() && sensor.internalClockConfigured(),"restart re-enabled external clock");
        check((spi.registers[0][0x50]&0x0F)==9,"50Hz rate lost after restart");
      } else if (name == "external_clock_compatibility") {
        check(sensor.begin() && sensor.externalClockConfigured(),"existing begin() lost external clock default");
        check(sensor.restartAccel() && sensor.externalClockConfigured(),"external clock restart changed");
      } else if (name == "absent_sensor") {
        spi.registers[0][0x75]=0xFF;
        check(!sensor.begin(false) && sensor.lastDeviceId()==0xFF,"absent sensor accepted as valid data source");
      } else throw std::runtime_error("unknown case");
    }
  } catch(const std::exception& error) { std::cerr<<error.what()<<'\n'; return 1; }
}
