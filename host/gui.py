"""Tk desktop entry; networking and disk IO run in the gateway worker."""

import queue
import tkinter as tk
import time
from tkinter import ttk, filedialog, messagebox
from .gateway import GatewayServer
from .wire import NODE_FREQUENCIES, STATE_NAMES


class App:
    def __init__(
        self,
        root,
        bind,
        port,
        output,
        nodes,
        tx_range_verified=False,
        gateway_config="gateway/e870.local.json",
    ):
        self.root = root
        self.events = queue.Queue()
        self.server = None
        self.tx_range_verified = tx_range_verified
        self.gateway_config = gateway_config
        root.title("E870 三轴加速度采集")
        root.geometry("1500x720")
        top = ttk.Frame(root, padding=10)
        top.pack(fill="x")
        self.bind = tk.StringVar(value=bind)
        self.port = tk.StringVar(value=str(port))
        self.output = tk.StringVar(value=output)
        self.ids = tk.StringVar(value=",".join(map(str, nodes)))
        self.rate = tk.StringVar(value="50")
        self.sf = tk.StringVar(value="6")
        self.range = tk.StringVar(value="4")
        self.duration = tk.StringVar(value="0")
        for label, var, width in [
            ("监听地址", self.bind, 15),
            ("端口", self.port, 6),
            ("目标节点", self.ids, 25),
        ]:
            ttk.Label(top, text=label).pack(side="left", padx=4)
            ttk.Entry(top, textvariable=var, width=width).pack(side="left")
        ttk.Button(top, text="连接网关", command=self.connect).pack(side="left", padx=10)
        settings = ttk.Frame(root, padding=10)
        settings.pack(fill="x")
        ttk.Label(settings, text="采样率Hz").pack(side="left")
        ttk.Combobox(
            settings, textvariable=self.rate, values=[50, 100], state="readonly", width=6
        ).pack(side="left", padx=5)
        ttk.Label(settings, text="扩频因子").pack(side="left")
        ttk.Combobox(
            settings, textvariable=self.sf, values=[5, 6], state="readonly", width=4
        ).pack(side="left", padx=5)
        ttk.Label(settings, text="量程±g").pack(side="left")
        ttk.Combobox(
            settings, textvariable=self.range, values=[2, 4, 8, 16], state="readonly", width=5
        ).pack(side="left", padx=5)
        ttk.Label(settings, text="时长秒（0=手动停止）").pack(side="left")
        ttk.Entry(settings, textvariable=self.duration, width=7).pack(side="left", padx=5)
        for label, cmd in [
            ("查询节点", lambda: self.command("discover")),
            ("开始采集", self.start_capture),
            ("停止采集", lambda: self.command("stop")),
        ]:
            ttk.Button(settings, text=label, command=cmd).pack(side="left", padx=6)
        paths = ttk.Frame(root, padding=10)
        paths.pack(fill="x")
        ttk.Label(paths, text="保存目录").pack(side="left")
        ttk.Entry(paths, textvariable=self.output).pack(side="left", fill="x", expand=True)
        ttk.Button(paths, text="选择", command=self.choose).pack(side="left")
        verification = "下行范围已验证" if tx_range_verified else "下行范围未验证，仅接收"
        self.gateway = tk.StringVar(
            value=f"网关未连接；{verification}（{gateway_config}）。"
        )
        ttk.Label(root, textvariable=self.gateway, padding=10).pack(fill="x")
        cols = (
            "node",
            "channel",
            "online",
            "state",
            "confirmed",
            "gps",
            "clock",
            "queued",
            "packets",
            "samples",
            "rssi",
            "snr",
            "lost",
            "retransmissions",
        )
        self.table = ttk.Treeview(root, columns=cols, show="headings", height=10)
        for key, text in zip(
            cols,
            [
                "节点",
                "上行MHz",
                "在线",
                "状态",
                "已确认",
                "GNSS/PPS",
                "时钟ppm",
                "队列",
                "包数",
                "样本数",
                "RSSI",
                "SNR",
                "丢包/丢样",
                "补传",
            ],
        ):
            self.table.heading(key, text=text)
            self.table.column(key, width=92, anchor="center")
        self.table.pack(fill="both", expand=True, padx=10)
        for node in nodes:
            channel = NODE_FREQUENCIES.get(node)
            self.table.insert(
                "",
                "end",
                iid=str(node),
                values=(
                    node,
                    f"{channel:.1f}" if channel is not None else "-",
                    "否",
                    "未发现",
                    "否",
                    "等待/失锁",
                    "-",
                    0,
                    0,
                    0,
                    0,
                    0,
                    "0/0",
                    0,
                ),
            )
        self.log = tk.Text(root, height=10, state="disabled")
        self.log.pack(fill="both", padx=10, pady=10)
        root.protocol("WM_DELETE_WINDOW", self.close)
        root.after(100, self.refresh)

    def choose(self):
        folder = filedialog.askdirectory()
        if folder:
            self.output.set(folder)

    def connect(self):
        if self.server:
            messagebox.showinfo("连接", "接收服务已运行；修改配置后请重新打开上位机。")
            return
        try:
            self.server = GatewayServer(
                self.bind.get(),
                int(self.port.get()),
                self.output.get(),
                [int(n.strip()) for n in self.ids.get().split(",")],
                lambda k, p: self.events.put((k, p)),
                tx_range_verified=self.tx_range_verified,
            )
            self.server.start()
        except (ValueError, OSError) as exc:
            self.server = None
            messagebox.showerror("无法启动", str(exc))

    def command(self, action, **options):
        if not self.server:
            messagebox.showinfo("连接", "请先连接网关接收服务。")
            return
        self.server.request(action, **options)

    def start_capture(self):
        try:
            self.command(
                "start",
                rate=int(self.rate.get()),
                sf=int(self.sf.get()),
                range_g=int(self.range.get()),
                duration=int(self.duration.get()),
            )
        except ValueError:
            messagebox.showerror("参数", "时长应为非负整数秒。")

    def refresh(self):
        while True:
            try:
                kind, p = self.events.get_nowait()
            except queue.Empty:
                break
            if kind == "node":
                values = (
                    p["node"],
                    f'{p["channel"]:.1f}',
                    "是" if p["last_seen"] and time.monotonic() - p["last_seen"] <= 30 else "否",
                    STATE_NAMES.get(p["state"], p["state"]),
                    "是" if p["confirmed"] else "否",
                    "正常" if p["flags"] & 1 else "等待/失锁",
                    f'{p["clock_ppm"]:.2f}',
                    p["queued"],
                    p["packets"],
                    p["samples"],
                    p["rssi"],
                    p["snr"],
                    f'{p["lost_packets"]}/{p["lost_samples"]}',
                    p["retransmissions"],
                )
                key = str(p["node"])
                if self.table.exists(key):
                    self.table.item(key, values=values)
                else:
                    self.table.insert("", "end", iid=key, values=values)
            else:
                if kind == "gateway":
                    mode = "双向已启用" if self.tx_range_verified else "仅接收"
                    self.gateway.set(f'网关 {p["eui"]} 已连接 {p["peer"]}；{mode}')
                self.log.configure(state="normal")
                self.log.insert("end", str(p) + "\n")
                self.log.see("end")
                self.log.configure(state="disabled")
        self.root.after(100, self.refresh)

    def close(self):
        if self.server:
            self.server.close()
        self.root.destroy()
