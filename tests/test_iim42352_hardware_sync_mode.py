import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


class Iim42352HardwareSyncModeTest(unittest.TestCase):
    def test_clkout_feature_exists_but_default_builds_do_not_enable_it(self):
        board_pins = read_text("include/board_pins.h")
        config = read_text("include/config.h")
        platformio = read_text("platformio.ini")

        self.assertIn("#define PIN_IIM42352_CLKOUT 3", board_pins)
        self.assertIn("APP_IIM42352_GPS_DISCIPLINED_CLKIN", config)
        self.assertIn("APP_IIM42352_CLKOUT_DPLL", config)
        self.assertIn("IIM42352_CLKOUT_DPLL_MAX_PPM", config)
        self.assertIn("APP_IIM42352_CLKOUT_HZ_VALUE 32768UL", config)
        self.assertIn("IIM42352_CLKOUT_HZ", config)
        self.assertIn("IIM42352_SYNC_WARMUP_MS", config)
        default_block = platformio.split("[common]", 1)[0]
        self.assertNotIn("clk_sync", default_block)
        self.assertNotIn("APP_IIM42352_GPS_DISCIPLINED_CLKIN", default_block)

    def test_firmware_emits_clksync_and_aligns_start_to_gps_second(self):
        main = read_text("src/main.cpp")

        self.assertIn("ClockSync clkSync", main)
        self.assertIn("ledcWrite", main)
        self.assertIn("alignStartEpochToGpsSecond", main)
        self.assertIn("TX START_AT epoch aligned", main)
        self.assertIn("TX USB START_AT epoch aligned", main)
        self.assertIn("CLKSYNC,%u,%llu,%lu,%lu,%ld,%ld,%u,%ld,%lu,%u", main)

    def test_clkout_dpll_tracks_pps_phase_and_expected_cycles(self):
        main = read_text("src/main.cpp")

        self.assertIn("void update(const GpsTimebase& timebase)", main)
        self.assertIn("dpllPpm()", main)
        self.assertIn("expectedCycles()", main)
        self.assertIn("stable()", main)
        self.assertIn("IIM42352_CLKOUT_DPLL_PHASE_GAIN_PPM_PER_US", main)

    def test_capture_start_restarts_sensor_phase_and_drops_warmup_samples(self):
        main = read_text("src/main.cpp")
        iim_header = read_text("lib/IIM42352Driver/src/IIM42352Driver.h")
        iim_cpp = read_text("lib/IIM42352Driver/src/IIM42352Driver.cpp")

        self.assertIn("restartAccel", iim_header)
        self.assertIn("bool IIM42352Driver::restartAccel", iim_cpp)
        self.assertIn("adxl.restartAccel()", main)
        self.assertIn("txRunWarmupUntilLocalUs", main)
        self.assertIn("TX sync warmup drop", main)
        self.assertIn("first_int1_offset_us", main)

    def test_start_at_carries_iim_range_in_reserved_field(self):
        config = read_text("include/config.h")
        protocol_h = read_text("include/protocol.h")
        protocol_cpp = read_text("src/protocol.cpp")
        main = read_text("src/main.cpp")

        self.assertIn("APP_IIM42352_RANGE_G_VALUE", config)
        self.assertIn("uint16_t reserved;", protocol_h)
        self.assertIn("frame.reserved = reserved;", protocol_cpp)
        self.assertIn("frame.reserved == 0 ? AppConfig::IIM42352_RANGE_G : frame.reserved", main)
        self.assertIn("requestedRangeG != activeIimRangeG", main)
        self.assertIn("applyIimRangeG(requestedRangeG, true)", main)
        self.assertIn("rxRunIimRangeG", main)
        self.assertIn("rxRunIimRangeG))", main)
        self.assertIn("START_AT epoch_us sample_rate_hz [delay_ms] [range_g]", main)

    def test_iim_range_is_read_back_before_firmware_accepts_it(self):
        main = read_text("src/main.cpp")
        iim_header = read_text("lib/IIM42352Driver/src/IIM42352Driver.h")
        iim_cpp = read_text("lib/IIM42352Driver/src/IIM42352Driver.cpp")

        self.assertIn("accelConfig0", iim_header)
        self.assertIn("rangeConfigured", iim_header)
        self.assertIn("MASK_ACCEL_FS_SEL", iim_header)
        self.assertIn("IIM42352Driver::rangeConfigured", iim_cpp)
        self.assertIn("range verify failed", main)
        self.assertIn("ACCEL_CONFIG0=0x%02X,verified=1", main)

    def test_iim_startup_self_check_is_strict_and_uses_slow_spi(self):
        config = read_text("include/config.h")
        iim_header = read_text("lib/IIM42352Driver/src/IIM42352Driver.h")
        iim_cpp = read_text("lib/IIM42352Driver/src/IIM42352Driver.cpp")
        main = read_text("src/main.cpp")

        self.assertIn("IIM42352_SPI_CLOCK_HZ = 1000000UL", config)
        self.assertIn("uint32_t spiClockHz = 1000000UL", iim_header)
        self.assertGreaterEqual(iim_cpp.count("checkDeviceIdConsecutive(2)"), 3)
        self.assertIn("delay(50);", iim_cpp)
        self.assertIn("iimInt1EdgesGrowing", main)
        self.assertIn("int1EdgeRecent", main)
        self.assertIn("SENSOR_READY,%u,0x%02X,%u,%u,%lu", main)
        self.assertIn("TX USB ignored START_AT sensor not ready", main)


if __name__ == "__main__":
    unittest.main()
