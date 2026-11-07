#pragma once
#if defined(APP_BOARD_HELTEC) && !defined(APP_BOARD_EORA)
#include "boards/heltec_v32/Board.h"
#elif defined(APP_BOARD_EORA) && !defined(APP_BOARD_HELTEC)
#include "boards/eora_s3_400/Board.h"
#else
#error "Select exactly one board"
#endif
