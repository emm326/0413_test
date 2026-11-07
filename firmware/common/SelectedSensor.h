#pragma once
#if defined(APP_SENSOR_IIM42352) && !defined(APP_SENSOR_IIM42652)
#include "sensors/iim42352/Driver.h"
using AccelDriver = Iim42352;
#elif defined(APP_SENSOR_IIM42652) && !defined(APP_SENSOR_IIM42352)
#include "sensors/iim42652/Driver.h"
using AccelDriver = Iim42652;
#else
#error "Select exactly one accelerometer"
#endif
