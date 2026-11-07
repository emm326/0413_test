import argparse
import time
from .configure_gateway import verify_gateway_file
from .gateway import GatewayServer


def main():
    parser = argparse.ArgumentParser(description="E870 加速度采集上位机（v3原始LoRa）")
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=1700)
    parser.add_argument("--output", default="data/e870")
    parser.add_argument("--nodes", default=",".join(str(n) for n in range(1001, 1009)))
    parser.add_argument(
        "--gateway-config",
        default="gateway/e870.local.json",
        help="E870原始或已生成配置；用于验证470.3MHz下行范围",
    )
    parser.add_argument("--headless", action="store_true", help="仅接收和保存；默认打开桌面界面")
    args = parser.parse_args()
    nodes = [int(n.strip()) for n in args.nodes.split(",")]
    tx_range_verified = verify_gateway_file(args.gateway_config)
    if not tx_range_verified:
        print(
            f"警告：{args.gateway_config} 未证明E870支持470.3MHz下行；仅允许接收，查询/开始/停止将被阻止。",
            flush=True,
        )
    if args.headless:
        server = GatewayServer(
            args.bind,
            args.port,
            args.output,
            nodes,
            lambda kind, p: print(kind, p, flush=True),
            tx_range_verified=tx_range_verified,
        )
        server.start()
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
    else:
        import tkinter as tk
        from .gui import App

        root = tk.Tk()
        App(
            root,
            args.bind,
            args.port,
            args.output,
            nodes,
            tx_range_verified=tx_range_verified,
            gateway_config=args.gateway_config,
        )
        root.mainloop()


if __name__ == "__main__":
    main()
