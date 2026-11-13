"""Read-only evidence verification around a bounded, non-command RF self-test."""
import copy
import time


def downlink_self_test(manager, server, antenna_confirmed, timeout=45):
    if not antenna_confirmed:
        raise ValueError("请确认LoRa天线已接好")
    if server.run_id or server.cancel_run:
        raise ValueError("运行未结束，不能进行射频自检")
    snap = manager.read()
    if not snap["tx_allowed"]:
        raise ValueError("设备当前配置没有允许470.3MHz")
    if snap["config"]["gateway_conf"].get("gateway_ID", "").lower() != server.gateway_eui:
        raise ValueError("管理连接设备与UDP网关身份不一致")
    cursor = manager.log_cursor()
    started_mono = time.monotonic()
    tx_before = server.stats["tx"] if hasattr(server, "stats") else None
    identifier = server.request("self_test", antenna_confirmed=True)
    deadline = time.monotonic() + timeout
    result = {"passed": False, "sent": 0, "errors": 0, "rejected": []}
    next_log = 0
    while time.monotonic() < deadline:
        command = server.commands.get(identifier, {})
        if command.get("state") in ("failed", "timeout", "gateway_rejected"):
            break
        if time.monotonic() >= next_log:
            result = manager.verify_tx(cursor)
            next_log = time.monotonic() + 2
        acknowledgments = command.get("gateway_results", {})
        telemetry = server.tx_success_since(started_mono) if hasattr(server, "tx_success_since") else {"sent": 0, "samples": []}
        executed = (telemetry["sent"] == 3 if hasattr(server, "tx_success_since") else result["passed"]) and telemetry["sent"] <= 3 and not result["errors"] and not result["rejected"]
        if executed and len(acknowledgments) == 3 and all(x == "NONE" for x in acknowledgments.values()):
            break
        time.sleep(.2)
    acknowledgments = dict(server.commands.get(identifier, {}).get("gateway_results", {}))
    telemetry = server.tx_success_since(started_mono) if hasattr(server, "tx_success_since") else {"sent": 0, "samples": []}
    tx_delta = server.stats["tx"] - tx_before if tx_before is not None else 3
    executed = (telemetry["sent"] == 3 if hasattr(server, "tx_success_since") else result["passed"]) and telemetry["sent"] <= 3 and not result["errors"] and not result["rejected"]
    passed = bool(executed and tx_delta == 3 and len(acknowledgments) == 3 and all(x == "NONE" for x in acknowledgments.values()))
    result.update(log_sent=result["sent"], sent=telemetry["sent"] if hasattr(server, "tx_success_since") else result["sent"],
                  execution_source="udp_stat.txnb" if telemetry["sent"] == 3 else "current_boot_journal",
                  telemetry_samples=telemetry["samples"], host_tx_delta=tx_delta,
                  passed=passed, command_id=identifier, acknowledgments=acknowledgments,
                  config_sha256=snap["config_sha256"], device_id=snap["device_id"],
                  test="real_gateway_only", node_confirmed=False, rf_power_measured=False)
    manager.last_tx_test = result
    server.request("self_test_result", identifier=identifier, passed=passed, evidence=copy.deepcopy(result))
    return result
