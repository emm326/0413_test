"""Merge the project settings into an exported factory configuration; never invent RF calibration."""

import argparse
import copy
import json
from pathlib import Path

CONTROL_FREQUENCY_HZ = 470300000


def strip_comments(text):
    out = []
    i = 0
    quoted = False
    escape = False
    while i < len(text):
        c = text[i]
        if quoted:
            out.append(c)
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                quoted = False
            i += 1
            continue
        if c == '"':
            quoted = True
            out.append(c)
            i += 1
            continue
        if text[i : i + 2] == "//":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        if text[i : i + 2] == "/*":
            end = text.find("*/", i + 2)
            if end < 0:
                raise ValueError("unterminated JSON comment")
            out.append(" ")
            i = end + 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def merge(target, source):
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def supports_control_tx(config):
    rf = config.get("SX130x_conf", {}).get("radio_0", {})
    return bool(
        rf.get("tx_enable")
        and rf.get("tx_freq_min", float("inf"))
        <= CONTROL_FREQUENCY_HZ
        <= rf.get("tx_freq_max", 0)
    )


def verify_gateway_file(path):
    if not path:
        return False
    source = Path(path)
    if not source.is_file():
        return False
    config = json.loads(strip_comments(source.read_text(encoding="utf-8-sig")))
    return supports_control_tx(config)


def configure(factory, server):
    result = copy.deepcopy(factory)
    if "SX130x_conf" not in result or "gateway_conf" not in result:
        raise ValueError("expected the gateway's SX130x_conf / gateway_conf factory file")
    rf = result["SX130x_conf"].get("radio_0", {})
    if "type" not in rf:
        raise ValueError("factory radio_0 type/calibration configuration required")
    overlay = json.loads(
        (Path(__file__).resolve().parent.parent / "gateway/project.overlay.json").read_text(
            encoding="utf-8"
        )
    )
    merge(result, overlay)
    result["gateway_conf"]["server_address"] = server
    supports = supports_control_tx(result)
    return result, supports


def main():
    parser = argparse.ArgumentParser(description="从E870原配置生成项目配置，保留射频校准和发射范围")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--server", required=True)
    args = parser.parse_args()
    source = Path(args.input)
    target = Path(args.output)
    if source.resolve() == target.resolve():
        parser.error("output must differ from the original backup")
    factory = json.loads(strip_comments(source.read_text(encoding="utf-8-sig")))
    result, supports = configure(factory, args.server)
    if not supports:
        parser.error(
            "原配置的发射路径或频率范围不支持470.3MHz；未写入项目配置，请先确认E870硬件版本"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("已生成：", target)
    print("配置允许470.3MHz下发：", supports)
    print("本工具保留原发射限制和射频校准；配置允许仍需验证实际下发。")


if __name__ == "__main__":
    main()
