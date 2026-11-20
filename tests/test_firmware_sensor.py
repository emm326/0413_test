"""Execute the formal IIM42652 driver against deterministic transport and vendor stubs."""
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class FirmwareSensorTransportTests(unittest.TestCase):
    def test_driver_and_startup_contracts(self):
        compiler = shutil.which("g++")
        if not compiler:
            self.skipTest("g++ is required to execute native firmware regression tests")
        output = ROOT / "tmp" / "firmware_sensor_tests"
        output.mkdir(parents=True, exist_ok=True)
        executable = output / "sensor_tests.exe"
        command = [compiler, "-std=c++17", "-Wall", "-Wextra", "-Werror",
                   "-DAPP_BOARD_EORA=1", "-I" + str(ROOT / "tests/firmware_support"),
                   "-I" + str(ROOT / "firmware"), "-I" + str(ROOT / "firmware/common"),
                   str(ROOT / "tests/firmware_sensor_transport.cpp"),
                   str(ROOT / "firmware/sensors/iim42652/SensorSpiTransport.cpp"),
                   str(ROOT / "firmware/sensors/iim42652/Driver.cpp"), "-o", str(executable)]
        built = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
        result = subprocess.run([str(executable)], cwd=ROOT, capture_output=True, text=True)
        (output / "result.log").write_text(result.stdout + result.stderr, encoding="utf-8")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_formal_driver_has_no_legacy_base(self):
        driver = (ROOT / "firmware/sensors/iim42652/Driver.h").read_text(encoding="utf-8")
        self.assertNotIn("IimBase", driver)
        vendor = (ROOT / "firmware/sensors/iim42652_vendor/Ixm42xxxDriver_HL.h").read_text(
            encoding="utf-8"
        )
        self.assertIn("defined(IIM42652)", vendor)


    def test_driver_verifies_hardware_not_cache(self):
        header = (ROOT / "firmware/sensors/iim42652/Driver.h").read_text(encoding="utf-8")
        driver = (ROOT / "firmware/sensors/iim42652/Driver.cpp").read_text(encoding="utf-8")
        self.assertIn("directRead", header)
        self.assertIn("physical_read=%u", driver)
        self.assertIn("soft_reset=1", driver)
        self.assertIn("packetBytes_", driver)

    def test_iim42352_independent_driver_contracts(self):
        compiler = shutil.which("g++")
        if not compiler:
            self.skipTest("g++ is required to execute native firmware regression tests")
        output = ROOT / "tmp" / "iim42352_tests"
        output.mkdir(parents=True, exist_ok=True)
        executable = output / "driver_tests.exe"
        command = [compiler, "-std=c++17", "-Wall", "-Wextra", "-Werror",
                   "-DAPP_BOARD_HELTEC=1", "-I" + str(ROOT / "tests/firmware_support"),
                   "-I" + str(ROOT / "firmware"), "-I" + str(ROOT / "firmware/common"),
                   str(ROOT / "tests/firmware_iim42352_driver.cpp"),
                   str(ROOT / "firmware/sensors/iim42352/Driver.cpp"),
                   "-o", str(executable)]
        built = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
        result = subprocess.run([str(executable)], cwd=ROOT, capture_output=True, text=True)
        (output / "result.log").write_text(result.stdout + result.stderr, encoding="utf-8")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

        header = (ROOT / "firmware/sensors/iim42352/Driver.h").read_text(encoding="utf-8")
        self.assertNotIn(": public IimBase", header)
        self.assertIn("timestampSupported", header)
if __name__ == "__main__":
    unittest.main()



