"""Exercise the production radio/GPS drivers with simulated hardware faults."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("g++"), "native driver tests require g++")
class WirelessRecoveryNativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        build_root = ROOT / ".pio"
        build_root.mkdir(exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="native-recovery-", dir=build_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.exe = Path(cls.temp.name) / "wireless_recovery.exe"
        native = ROOT / "tests" / "native"
        gps = ROOT / "lib" / "GpsTimebase" / "src"
        subprocess.run([
            shutil.which("g++"), "-std=c++17", "-Wall", "-Wextra",
            "-I", str(native / "stubs"), "-I", str(ROOT / "include"), "-I", str(gps),
            str(native / "wireless_recovery.cpp"), str(ROOT / "src" / "lora_transport.cpp"),
            str(gps / "GpsTimebase.cpp"), "-o", str(cls.exe),
        ], check=True, capture_output=True, text=True)

    def test_radio_faults_and_gps_fairness(self):
        for case in ("dio_fallback", "crc_error", "timeout_recovery", "header_recovery",
                     "rearm_failure", "oversized_packet", "gps_budget"):
            with self.subTest(case=case):
                result = subprocess.run([str(self.exe), case], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
