#include <algorithm>
#include <array>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "Arduino.h"
#include "SPI.h"
#include "driver/gpio.h"
#include "sensors/iim42652/Driver.h"
#include "sensor/Registers.h"
#if __has_include("sensor/StartupCheck.h")
#include "sensor/StartupCheck.h"
#define HAVE_STARTUP_CHECK 1
#endif
struct Write { unsigned bank,address,value;uint32_t hz;uint8_t mode; };
struct Bus {
  uint8_t regs[8][128]{};unsigned bank=0;
  uint32_t hz=1000000,maxHz=1000000;int requiredMode=-1;uint8_t mode=0;
  bool selected=false,routeOk=true;unsigned pulls=0,ends=0,begins=0;
  std::vector<Write> writes;std::vector<uint8_t> ids;size_t identityIndex=0;
  uint16_t fifo=0,latched=0;int drop=-1,dropBank=-1;
  unsigned bankWritesToDrop=0;int delayedAddress=-1;unsigned delayedWrites=0;
  uint64_t nowUs=0,blockWritesUntil=0;unsigned timingViolations=0,activeConfigWrites=0;
  bool latchReturnsFresh=false,alwaysChanges=false;std::vector<uint16_t> fifoUpdates;size_t lowReads=0;
  void defaults(){for(auto& row:regs)for(auto& x:row)x=0;bank=0;regs[0][0x4c]=0x30;regs[0][0x4d]=0x91;regs[1][0x7b]=0x20;}
  bool valid()const{return routeOk&&hz<=maxHz&&(requiredMode<0||mode==requiredMode);}
  uint8_t read(unsigned address){
    if(!valid())return 0;
    if(bank==0&&address==0x75){if(ids.empty())return 0x6f;return ids[std::min(identityIndex++,ids.size()-1)];}
    if(bank==0&&address==0x2f){if(alwaysChanges)fifo+=8;else if(lowReads<fifoUpdates.size())fifo=fifoUpdates[lowReads];++lowReads;uint8_t old=latched&255;latched=fifo;return latchReturnsFresh?uint8_t(latched&255):old;}
    if(bank==0&&address==0x2e)return latched>>8;
    if(bank==0&&address==0x2d){auto value=regs[0][address];regs[0][address]=0;return value;}
    if(address==0x76)return bank;
    return regs[bank][address];
  }
  void write(unsigned address,uint8_t value){
    writes.push_back({bank,address,value,hz,mode});
    if(!valid())return;
    if(address==0x76){if(bankWritesToDrop&&value==1){--bankWritesToDrop;return;}bank=value&7;return;}
    if(bank==0&&address==0x11&&value==1){defaults();return;}
    if(nowUs<blockWritesUntil)++timingViolations;
    if((regs[0][0x4e]&0x0f)&&!(bank==0&&(address==0x4e||address==0x50)))++activeConfigWrites;
    if(bank==0&&address==0x4e&&(regs[0][0x4e]&3)==0&&(value&3))blockWritesUntil=nowUs+200;
    if(int(address)==delayedAddress&&delayedWrites){--delayedWrites;return;}
    if(!(int(address)==drop&&(dropBank<0||int(bank)==dropBank)))regs[bank][address]=value;
  }
} bus;
TestSerial Serial;
void pinMode(int,int){}
void digitalWrite(int,int value){bus.selected=value==LOW;}
int digitalRead(int){return 0;}
void delay(unsigned long ms){bus.nowUs+=ms*1000;}
void delayMicroseconds(unsigned int us){bus.nowUs+=us;}
int gpio_set_pull_mode(gpio_num_t,gpio_pull_mode_t){++bus.pulls;return 0;}
SPIClass::SPIClass(uint8_t){}
void SPIClass::begin(int,int,int,int){if(started_)return;started_=true;bus.routeOk=true;++bus.begins;}
void SPIClass::end(){started_=false;++bus.ends;}
void SPIClass::beginTransaction(SPISettings s){bus.hz=s.hz;bus.mode=s.mode;command_=true;}
void SPIClass::endTransaction(){}
uint8_t SPIClass::transfer(uint8_t value){
  if(!bus.selected)throw std::runtime_error("SPI transfer without CS");
  if(command_){address_=value&0x7f;reading_=value&0x80;command_=false;return 0;}
  if(reading_)return bus.read(address_++);
  bus.write(address_++,value);return 0;
}
void resetModel(){bus=Bus{};bus.defaults();Serial.output.clear();}
void require(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
void noReset(){resetModel();Iim42652 s;require(s.begin(false),"stable sensor failed initialization");require(std::none_of(bus.writes.begin(),bus.writes.end(),[](const Write&w){return w.address==0x11&&w.value==1;}),"chip reset was sent");}
void defaultResetCompatibility(){resetModel();Iim42652 s;require(s.begin(),"default initialization failed");require(std::count_if(bus.writes.begin(),bus.writes.end(),[](const Write&w){return w.address==0x11&&w.value==1;})==1,"default reset policy changed");}
void fallback(){resetModel();bus.maxHz=100000;bus.requiredMode=3;Iim42652 s;require(s.begin(false),"no mode/speed fallback for a responsive sensor");require(s.readRegister(0x75)==0x6f,"selected transport not retained for reads");s.configure(50,4);require(bus.hz==100000&&bus.mode==3,"configuration used the wrong transport");}
void isolatedIdentity(){resetModel();bus.ids={0x6f,0x00};Iim42652 s;require(!s.begin(false),"one isolated identity accepted as a stable link");require(std::none_of(bus.writes.begin(),bus.writes.end(),[](const Write&w){return w.address!=0x76;}),"configuration written before stable identity");}
void staleFifo(){resetModel();Iim42652 s;require(s.begin(false),"init failed");bus.fifo=0x0308;bus.latched=0x0200;require(s.available()==0x0308/8,"FIFO count combined stale low byte with fresh high byte");require(s.lastFifoBytes()==0x0308,"raw byte count wrong");}
void configMismatch(){resetModel();bus.drop=0x65;Iim42652 s;require(!s.begin(false),"missing INT1 configuration accepted");}
void retryRoute(){resetModel();Iim42652 s;require(s.begin(false),"first init failed");bus.routeOk=false;require(s.begin(false),"transport route not rebound on retry");require(bus.begins>=2,"SPI.begin returned early without reattaching pins");}
void readOnlyDiagnostic(){resetModel();Iim42652 s;require(s.begin(false),"init failed");auto writes=bus.writes.size();auto pulls=bus.pulls;s.diagnostic();require(bus.writes.size()==writes,"diagnostic wrote sensor configuration");require(bus.pulls==pulls,"diagnostic changed MISO pull configuration");}
void fifoCounterCarry(){
  for(bool fresh:{false,true}){
    resetModel();Iim42652 s;require(s.begin(false),"init failed");
    bus.latchReturnsFresh=fresh;bus.fifoUpdates={0x00f8,0x0100};bus.lowReads=0;
    require(s.available()==0x0100/8&&s.lastFifoBytes()==0x0100,"FIFO high/low carry produced a mixed count");
  }
}
void unstableCount(){
  resetModel();Iim42652 s;require(s.begin(false),"init failed");bus.latchReturnsFresh=true;bus.alwaysChanges=true;bus.lowReads=0;
  require(s.available()==0&&!s.fifoCountValid(),"unstable counter accepted without a coherent snapshot");
}
void transientIdentity(){resetModel();bus.ids={0x00,0x6f,0x00,0x6f,0x6f,0x6f};Iim42652 s;require(s.begin(false),"stable identity after transient reads was not recovered");}
void packetFormatReadback(){resetModel();bus.regs[0][0x5f]=0x51;bus.drop=0x5f;Iim42652 s;require(!s.begin(false),"stale high-resolution FIFO mode was accepted as 8-byte packets");}
void canonicalInterface(){resetModel();bus.regs[0][0x4c]=0x40;Iim42652 s;require(s.begin(false),"non-default count/byte order could not be normalized");require((bus.regs[0][0x4c]&0x70)==0x30,"FIFO count units or byte order remain wrong");}
void fifoPayload(){
  resetModel();Iim42652 s;require(s.begin(false),"init failed");
  const uint8_t packet[]={0x40,0x00,0x01,0xff,0xfe,0x20,0x00,0};
  for(unsigned i=0;i<8;++i)bus.regs[0][0x30+i]=packet[i];
  AccelSample value{};require(s.read(value)&&value.x==1&&value.y==-2&&value.z==8192,"valid acceleration packet decoded incorrectly");
  bus.regs[0][0x30]=0x80;require(!s.read(value),"message packet accepted as acceleration");
  bus.regs[0][0x30]=0x60;require(!s.read(value),"gyro packet accepted as acceleration-only");
  bus.regs[0][0x30]=0x40;bus.regs[0][0x31]=0x80;bus.regs[0][0x32]=0;require(!s.read(value),"invalid acceleration code accepted");
}
void sixAxisSpiSettings(){
  resetModel();bus.regs[1][0x7a]=0x41;bus.regs[1][0x7c]=0x4c;bus.regs[0][0x13]=0x18;bus.regs[0][0x4c]=0;
  Iim42652 s;require(s.begin(false),"non-reset interface configuration failed");
  require((bus.regs[1][0x7a]&0x42)==2,"SPI_AP_4WIRE/I3C_BUS_MODE not set per section 12.3");
  require((bus.regs[1][0x7c]&0x13)==0x13,"section 12.3 I3C enable bits missing for SPI operation");
  require((bus.regs[0][0x13]&0x3f)==5,"SPI/I2C slew rates not set per section 12.3");
  require((bus.regs[0][0x4c]&3)==3,"I2C interface not explicitly disabled for SPI");
  require(bus.bank==0,"interface initialization left a nonzero register bank selected");
}
void interfaceWriteFailure(){
  resetModel();bus.regs[1][0x7a]=0x41;bus.drop=0x7a;bus.dropBank=1;
  Iim42652 s;require(!s.begin(false),"4-wire interface write failure accepted");require(bus.bank==0,"bank0 not restored on interface failure");
}
void enabledClockSelection(){
  resetModel();bus.regs[0][0x4d]=0x93;Iim42652 s;
  require(s.begin(false),"clock selection could not be recovered");require((bus.regs[0][0x4d]&7)==5,"CLKSEL remained 'disable all clocks' after RTC_MODE selection");
}
void startupWriteDelay(){
  resetModel();Iim42652 s;require(s.begin(false),"init failed");s.start();s.configure(50,4);
  require(bus.timingViolations==0,"register write occurred within 200us of OFF-to-LN transition");
}
void accelOnlySixAxis(){
  resetModel();Iim42652 s;require(s.begin(false),"init failed");s.start();
  require((bus.regs[0][0x4e]&0x0f)==3,"gyro unexpectedly enabled");
  require((bus.regs[0][0x5f]&0x13)==1,"six-axis FIFO not in acceleration-only format");
  require(bus.regs[0][0x50]==0x48,"100Hz/4g register encoding incorrect");
  require(s.configure(50,4)&&bus.regs[0][0x50]==0x49,"50Hz/4g register encoding incorrect");
}
void bankWriteRetry(){resetModel();bus.bankWritesToDrop=1;Iim42652 s;require(s.begin(false),"one lost bank-select write prevented recovery");}
void clkinWriteRetry(){resetModel();bus.delayedAddress=0x7b;bus.delayedWrites=1;Iim42652 s;require(s.begin(false),"one lost CLKIN function write prevented recovery");}
void accelWriteRetry(){resetModel();Iim42652 s;require(s.begin(false),"init failed");bus.delayedAddress=0x50;bus.delayedWrites=1;require(s.configure(50,4),"one lost acceleration configuration write prevented recovery");}
void independentSpiDiagnostic(){
  resetModel();Iim42652 s;require(s.begin(false),"init failed");const auto count=bus.writes.size();const auto starts=bus.begins;
  s.linkTest();require(bus.writes.size()==count,"software SPI test wrote a sensor register");
  require(bus.begins==starts+1&&s.readRegister(0x75)==0x6f,"software SPI test did not restore hardware routing");
}
void idleBeforeInterface(){
  resetModel();bus.regs[0][0x4e]=3;Iim42652 s;require(s.begin(false),"warm initialization failed");
  require(bus.activeConfigWrites==0,"non-mode registers were written before stopping sensors");
  require((bus.regs[0][0x4e]&0x1f)==0x10,"RC oscillator not held on during stopped configuration");
}
void internalClockIsolation(){
  resetModel();Iim42652 s;require(s.begin(false,false),"internal-clock initialization failed");
  require((bus.regs[0][0x4d]&7)==1,"internal test still requires external RTC clock");
  require(std::none_of(bus.writes.begin(),bus.writes.end(),[](const Write&w){return w.bank==1&&w.address==0x7b;}),"internal test unnecessarily depends on CLKIN pin");
  require(s.begin(false),"external-clock configuration could not be restored");
  require((bus.regs[0][0x4d]&7)==5&&(bus.regs[1][0x7b]&6)==4,"external formal configuration not restored");
}
void directDataReady(){
  resetModel();Iim42652 s;require(s.begin(false,false),"init failed");AccelSample sample{7,8,9};
  require(!s.readDirect(sample)&&sample.x==7,"sample emitted without data-ready");
  const uint8_t raw[]={0x00,0x01,0xff,0xfe,0x20,0x00};
  for(unsigned i=0;i<6;++i)bus.regs[0][0x1f+i]=raw[i];
  bus.regs[0][0x2d]=8;require(s.readDirect(sample)&&sample.x==1&&sample.y==-2&&sample.z==8192,"direct XYZ decode failed");
  require(!s.readDirect(sample),"same read-clear DRDY was used twice");
}
void directRejectsLostIdentity(){
  resetModel();Iim42652 s;require(s.begin(false,false),"init failed");AccelSample sample{7,8,9};
  bus.regs[0][0x2d]=8;bus.ids={0};bus.identityIndex=0;
  require(!s.readDirect(sample)&&sample.x==7,"direct data accepted after identity lost");
}
void directRejectsInvalidCode(){
  resetModel();Iim42652 s;require(s.begin(false,false),"init failed");AccelSample sample{7,8,9};
  bus.regs[0][0x2d]=8;bus.regs[0][0x1f]=0x80;bus.regs[0][0x20]=0;
  require(!s.readDirect(sample)&&sample.x==7,"invalid raw acceleration accepted");
}
void badFifo(){resetModel();Iim42652 s;require(s.begin(false),"init failed");bus.fifo=0xffff;bus.latched=0xffff;require(s.available()==0&&!s.fifoCountValid(),"invalid capacity admitted as samples");}
#ifdef HAVE_STARTUP_CHECK
void startupPolicy(){
  using SensorChecks::StartupCheck;using SensorChecks::startupPassed;
  StartupCheck good{12,12,3840,true,true,true};require(startupPassed(good),"normal startup rejected");
  auto bad=good;bad.sampleValid=false;require(!startupPassed(bad),"invalid sample passed startup");
  bad=good;bad.fifoSamples=0;require(!startupPassed(bad),"empty FIFO passed startup");
  bad=good;bad.dataReadyEdges=6400;require(!startupPassed(bad),"clock-rate IRQ passed startup");
  bad=good;bad.clockEdges=0;require(!startupPassed(bad),"missing external clock passed startup");
  bad=good;bad.identityValid=false;require(!startupPassed(bad),"lost sensor identity passed startup");
}
void retryPolicy(){
  using namespace WireProtocol;using SensorChecks::canRetry;
  require(canRetry(Fault,false,0)&&canRetry(Waiting,false,0),"idle recovery rejected");
  for(State state:{Boot,Armed,Capturing,Draining})require(!canRetry(state,false,0),"active transition allowed recovery");
  require(!canRetry(Fault,true,0)&&!canRetry(Waiting,false,1),"capture or queued data could be discarded by retry");
}
#endif
int main(){
  struct Test{const char* name;void(*run)();};
  std::vector<Test> tests={{"no_chip_reset",noReset},{"default_reset_compatibility",defaultResetCompatibility},{"transport_fallback",fallback},{"stable_identity",isolatedIdentity},{"fifo_latch",staleFifo},{"configuration_readback",configMismatch},{"retry_routes",retryRoute},{"read_only_diagnostic",readOnlyDiagnostic},{"fifo_capacity_guard",badFifo},{"internal_clock_isolation",internalClockIsolation},{"direct_drdy_freshness",directDataReady},{"direct_lost_identity",directRejectsLostIdentity},{"direct_invalid_code",directRejectsInvalidCode},{"idle_before_interface",idleBeforeInterface},{"independent_spi_restores_bus",independentSpiDiagnostic},{"bank_write_retry",bankWriteRetry},{"clkin_write_retry",clkinWriteRetry},{"accel_write_retry",accelWriteRetry},{"six_axis_spi_settings",sixAxisSpiSettings},{"interface_write_failure",interfaceWriteFailure},{"enabled_clock_selection",enabledClockSelection},{"startup_write_delay",startupWriteDelay},{"six_axis_accel_only",accelOnlySixAxis},{"fifo_counter_carry",fifoCounterCarry},{"unstable_counter_rejected",unstableCount},{"transient_identity_recovery",transientIdentity},{"fifo_packet_format_readback",packetFormatReadback},{"interface_normalization",canonicalInterface},{"fifo_payload_validation",fifoPayload}};
#ifdef HAVE_STARTUP_CHECK
  tests.push_back({"startup_sample_and_clock",startupPolicy});tests.push_back({"recovery_state_guard",retryPolicy});
#endif
  unsigned failures=0;for(const auto& t:tests){try{t.run();std::cout<<"PASS "<<t.name<<'\n';}catch(const std::exception& e){++failures;std::cout<<"FAIL "<<t.name<<": "<<e.what()<<'\n';}}
  std::cout<<"RESULT "<<tests.size()-failures<<'/'<<tests.size()<<" passed\n";return failures?1:0;
}
