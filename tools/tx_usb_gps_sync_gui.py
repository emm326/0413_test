#!/usr/bin/env python3
"""Open the TX USB+GPS two-node capture GUI.

This is a small launcher for the current bench setup:
  COM6  -> device 1001
  COM12 -> device 1002
"""

from __future__ import annotations

from tx_usb_capacity_test import parse_args, run_gui


def main() -> int:
    config = parse_args(["--gui", "--gui-profile", "usb-gps-sync"])
    return run_gui(config)


if __name__ == "__main__":
    raise SystemExit(main())
