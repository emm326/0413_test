import unittest

from tools.lorawan_gateway_to_csv import parse_args as parse_lorawan_args
from tools.lorawan_gateway_to_csv import resolved_scale as resolved_lorawan_scale
from tools.lorawan_gateway_to_csv import scale_for_range_g as lorawan_scale_for_range_g
from tools.serial_gateway_csv_http import parse_args as parse_gateway_args
from tools.serial_gateway_csv_http import scale_for_range_g as gateway_scale_for_range_g
from tools.serial_rx_to_tcp_json import parse_args as parse_tcp_json_args
from tools.serial_rx_to_tcp_json import scale_for_range_g as tcp_json_scale_for_range_g


class GatewayRangeScaleTest(unittest.TestCase):
    def test_lorawan_gateway_range_g_sets_default_scale(self):
        args = parse_lorawan_args(["--stdin", "--range-g", "8"])

        self.assertEqual(args.range_g, 8)
        self.assertAlmostEqual(resolved_lorawan_scale(args), 8 / 32768.0)
        self.assertAlmostEqual(lorawan_scale_for_range_g(16), 16 / 32768.0)

    def test_lorawan_gateway_explicit_scale_overrides_range_g(self):
        args = parse_lorawan_args(["--stdin", "--range-g", "16", "--scale", "0.123"])

        self.assertAlmostEqual(resolved_lorawan_scale(args), 0.123)

    def test_serial_gateway_range_g_sets_default_scale(self):
        config = parse_gateway_args(
            [
                "--ports",
                "COM6",
                "--output-root",
                "data",
                "--upload-url",
                "http://127.0.0.1/upload",
                "--range-g",
                "16",
            ]
        )

        self.assertEqual(config.range_g, 16)
        self.assertAlmostEqual(config.scale, 16 / 32768.0)
        self.assertAlmostEqual(gateway_scale_for_range_g(8), 8 / 32768.0)

    def test_serial_gateway_explicit_scale_overrides_range_g(self):
        config = parse_gateway_args(
            [
                "--ports",
                "COM6",
                "--output-root",
                "data",
                "--upload-url",
                "http://127.0.0.1/upload",
                "--range-g",
                "8",
                "--scale",
                "0.123",
            ]
        )

        self.assertAlmostEqual(config.scale, 0.123)

    def test_tcp_json_range_g_sets_default_scale(self):
        config = parse_tcp_json_args(
            [
                "--port",
                "COM6",
                "--host",
                "127.0.0.1",
                "--server-port",
                "9000",
                "--range-g",
                "8",
            ]
        )

        self.assertEqual(config.range_g, 8)
        self.assertAlmostEqual(config.scale, 8 / 32768.0)
        self.assertAlmostEqual(tcp_json_scale_for_range_g(16), 16 / 32768.0)

    def test_tcp_json_explicit_scale_overrides_range_g(self):
        config = parse_tcp_json_args(
            [
                "--port",
                "COM6",
                "--host",
                "127.0.0.1",
                "--server-port",
                "9000",
                "--range-g",
                "16",
                "--scale",
                "0.123",
            ]
        )

        self.assertAlmostEqual(config.scale, 0.123)


if __name__ == "__main__":
    unittest.main()
