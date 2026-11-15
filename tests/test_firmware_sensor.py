"""Execute the real sensor driver against a deterministic SPI peripheral model."""
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
                   str(ROOT / "firmware/common/sensor/IimBase.cpp"),
                   str(ROOT / "firmware/sensors/iim42652/Driver.cpp"), "-o", str(executable)]
        built = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
        result = subprocess.run([str(executable)], cwd=ROOT, capture_output=True, text=True)
        (output / "result.log").write_text(result.stdout + result.stderr, encoding="utf-8")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
