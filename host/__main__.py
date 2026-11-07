import argparse
import time
from .gateway import GatewayServer


def main():
    parser = argparse.ArgumentParser(description="E870 加速度采集上位机（v3原始LoRa）")
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=1700)
    parser.add_argument("--output", default="data/e870")
    parser.add_argument("--nodes", default="1001,1002")
    parser.add_argument("--headless", action="store_true", help="仅接收和保存；默认打开桌面界面")
    args = parser.parse_args()
    nodes = [int(n.strip()) for n in args.nodes.split(",")]
    if args.headless:
        server = GatewayServer(
            args.bind, args.port, args.output, nodes, lambda kind, p: print(kind, p, flush=True)
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
        App(root, args.bind, args.port, args.output, nodes)
        root.mainloop()


if __name__ == "__main__":
    main()
