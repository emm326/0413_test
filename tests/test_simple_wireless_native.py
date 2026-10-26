"""Compile production framing and sensor driver against simulated registers."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

@unittest.skipUnless(shutil.which("g++"), "native driver tests require g++")
class SimpleWirelessNativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        build_root = ROOT / ".pio"
        build_root.mkdir(exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="native-simple-", dir=build_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.exe = Path(cls.temp.name) / "simple_wireless.exe"
        native = ROOT / "tests" / "native"
        sensor = ROOT / "lib" / "IIM42352Driver" / "src"
        result = subprocess.run([
            shutil.which("g++"), "-std=c++17", "-Wall", "-Wextra",
            "-I", str(native / "stubs"), "-I", str(ROOT / "include"), "-I", str(sensor),
            str(native / "simple_wireless_test.cpp"), str(ROOT / "src" / "simple_wireless_protocol.cpp"),
            str(ROOT / "src" / "protocol.cpp"), str(sensor / "IIM42352Driver.cpp"), "-o", str(cls.exe),
        ], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)

    def test_protocol_and_clock_modes(self):
        for case in ("data_roundtrip", "crc_and_length", "sensor_fault", "capacity_and_metadata",
                     "internal_clock", "external_clock_compatibility", "absent_sensor"):
            with self.subTest(case=case):
                result = subprocess.run([str(self.exe), case], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
