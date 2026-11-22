"""Chinese desktop: configuration, control and gateway diagnostics."""
import json
import os
import queue
import threading
import time
import tkinter as tk
from collections import deque
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from .configure_gateway import verify_gateway_file
from .gateway import GatewayServer
from .wire import NODE_FREQUENCIES, STATE_NAMES

STATES = {"queued": "已排队", "sent": "已发送，等待网关", "gateway_accepted": "网关已接受，待验证",
          "gateway_rejected": "网关拒绝", "waiting_nodes": "等待节点回复", "nodes_confirmed": "节点确认，等待预约时间",
          "completed": "完成", "failed": "失败", "timeout": "超时/未确认", "cancelled": "开始取消，等待停止确认"}
ACTIONS = {"discover": "查询节点", "start": "预约开始", "stop": "停止采集", "self_test": "网关下发自检"}


class App:
    def __init__(self, root, bind="0.0.0.0", port=1700, output="data/e870", nodes=tuple(range(1001,1009)),
                 tx_range_verified=False, gateway_config="gateway/state/active.local.json"):
        self.root = root
        self.events = queue.Queue()
        self.server = None
        self.manager = None
        self.preview_data = None
        self.busy = False
        self.closing = False
        self.close_deadline = None
        self.tx_range_verified = tx_range_verified
        self.gateway_config = gateway_config
        self.history = deque(maxlen=3000)
        self.command_rows = {}
        self.last_stats = {}
        self.state_dir = Path("gateway/state")
        self.preferences = self.state_dir / "preferences.local.json"
        try:
            saved = json.loads(self.preferences.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            saved = {}
        self.mode = tk.StringVar(value="USB2串口")
        self.serial_port = tk.StringVar(value=saved.get("serial_port", "COM11"))
        self.ssh_host = tk.StringVar(value=saved.get("ssh_host", "192.168.1.10"))
        self.username = tk.StringVar(value="root")
        self.password = tk.StringVar()
        self.computer = tk.StringVar(value=saved.get("computer", "192.168.1.22"))
        self.cidr = tk.StringVar(value=saved.get("cidr", "192.168.1.10/24"))
        self.bind = tk.StringVar(value=bind)
        self.port = tk.StringVar(value=str(port))
        self.output = tk.StringVar(value=output)
        sync_mode = saved.get("sync_mode", "原始节点时间")
        self.sync_mode = tk.StringVar(value=sync_mode if sync_mode in ("原始节点时间", "PPS同步") else "原始节点时间")
        self.ids = tk.StringVar(value=",".join(map(str,nodes)))
        self.rate = tk.StringVar(value="50")
        self.sf = tk.StringVar(value="6")
        self.range = tk.StringVar(value="4")
        self.duration = tk.StringVar(value="0")
        self.management_status = tk.StringVar(value="管理连接：未连接")
        self.gateway = tk.StringVar(value="接收服务未启动；网关未连接")
        self.progress = tk.StringVar(value="先连接USB2并读取设备，再预览配置差异。")
        self.start_reason = tk.StringVar(value="没有节点在线，正式采集不可开始。")
        self.stats_text = tk.StringVar(value="尚无真实UDP报文")
        self.root.title("E870 · 网关配置与采集控制")
        self.root.geometry("1390x900")
        self.root.minsize(1100,740)
        self._build()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        self.root.after(100,self.refresh)
        self.root.after(200,self.refresh_inventory)

    def _build(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(".", font=("Microsoft YaHei UI",10))
        style.configure("TFrame",background="#f3f5f8")
        style.configure("TLabel",background="#f3f5f8")
        style.configure("TButton",padding=(12,7))
        style.configure("Accent.TButton",background="#2463a7",foreground="white")
        style.configure("Treeview",rowheight=29,font=("Microsoft YaHei UI",9))
        header = tk.Frame(self.root,bg="#163352",padx=22,pady=15)
        header.pack(fill="x")
        tk.Label(header,text="E870 网关工作台",fg="white",bg="#163352",font=("Microsoft YaHei UI",19,"bold")).pack(anchor="w")
        tk.Label(header,text="配置与恢复   /   节点控制   /   真实网关诊断",fg="#c5d9ed",bg="#163352").pack(anchor="w",pady=(4,0))
        self.tabs=ttk.Notebook(self.root)
        self.tabs.pack(fill="both",expand=True,padx=15,pady=12)
        config=ttk.Frame(self.tabs,padding=16);control=ttk.Frame(self.tabs,padding=16);diag=ttk.Frame(self.tabs,padding=16)
        for frame,title in [(config,"网关配置"),(control,"采集控制"),(diag,"诊断与测试")]:self.tabs.add(frame,text=title)
        row=ttk.Frame(config);row.pack(fill="x")
        self._field(row,"连接方式",self.mode,values=["USB2串口","SSH网线"],width=13)
        self.port_combo=self._field(row,"串口",self.serial_port,values=[],width=11)
        self._field(row,"SSH地址",self.ssh_host,width=23)
        ttk.Button(row,text="刷新设备",command=self.refresh_inventory).pack(side="left",padx=5)
        row=ttk.Frame(config);row.pack(fill="x",pady=9)
        self._field(row,"账号",self.username,width=12)
        self._field(row,"密码",self.password,width=17,show="●")
        ttk.Button(row,text="连接并读取",style="Accent.TButton",command=self.connect_management).pack(side="left",padx=6)
        ttk.Button(row,text="断开管理",command=self.disconnect_management).pack(side="left")
        ttk.Label(config,textvariable=self.management_status).pack(anchor="w",pady=5)
        settings=ttk.LabelFrame(config,text="项目网络与无线配置",padding=12);settings.pack(fill="x",pady=8)
        row=ttk.Frame(settings);row.pack(fill="x")
        self.adapter_combo=self._field(row,"电脑有线IPv4",self.computer,values=[],width=18)
        self._field(row,"网关地址/掩码",self.cidr,width=21)
        ttk.Label(settings,text="E870-L470LG12-O显式设备档 · 470–510 MHz · 私有LoRa · 8信道 · 下行470.3 MHz / SF6 / 14 dBm").pack(anchor="w",pady=(8,0))
        row=ttk.Frame(config);row.pack(fill="x",pady=8)
        for title,callback in [("备份当前配置",self.backup),("读取并预览差异",self.preview),("应用网口设置",self.apply_network),("应用无线/转发设置",self.apply_project),("恢复备份",self.restore)]:
            ttk.Button(row,text=title,command=callback).pack(side="left",padx=(0,7))
        self.diff=ttk.Treeview(config,columns=("field","before","after"),show="headings",height=7)
        for key,title,width in [("field","配置项",350),("before","当前值",300),("after","拟应用值",300)]:
            self.diff.heading(key,text=title);self.diff.column(key,width=width)
        self.diff.pack(fill="both",expand=True)
        self.device_text=tk.Text(config,height=5,wrap="word",font=("Consolas",10),state="disabled",bg="#e9eef4",relief="flat")
        self.device_text.pack(fill="x",pady=(10,0))
        ttk.Label(config,text="应用前显示具体差异；网口修改/恢复使用USB2。首次SSH记录主机指纹，以后拒绝指纹变化。密码不保存。").pack(anchor="w",pady=(7,0))
        # Control page
        row=ttk.Frame(control);row.pack(fill="x")
        self._field(row,"监听地址",self.bind,width=15);self._field(row,"UDP端口",self.port,width=7)
        ttk.Button(row,text="启动接收服务",command=self.connect).pack(side="left",padx=5)
        ttk.Button(row,text="断开接收",command=self.disconnect_receiver).pack(side="left")
        ttk.Button(row,text="查询节点",command=lambda:self.command("discover")).pack(side="left",padx=5)
        row=ttk.Frame(control);row.pack(fill="x",pady=(0,8))
        self.sync_mode_combo=self._field(row,"接收保存模式",self.sync_mode,values=["原始节点时间","PPS同步"],width=14)
        self.sync_mode_combo.configure(state="readonly")
        ttk.Label(row,text="PPS同步使用预约START时刻生成统一时间轴；原始节点时间文件始终保留。").pack(side="left")
        ttk.Label(control,textvariable=self.gateway).pack(anchor="w",pady=10)
        row=ttk.Frame(control);row.pack(fill="x")
        self._field(row,"目标节点",self.ids,width=43)
        self._field(row,"采样Hz",self.rate,values=[50,100],width=6)
        self.rate.trace_add("write",lambda *_:self.sf.set("6" if self.rate.get()=="50" else "5"))
        self._field(row,"SF",self.sf,values=[6,5],width=4)
        self._field(row,"量程±g",self.range,values=[2,4,8,16],width=5)
        row=ttk.Frame(control);row.pack(fill="x",pady=10)
        self._field(row,"时长秒（0手动）",self.duration,width=9)
        self.start_button=ttk.Button(row,text="预约开始采集",style="Accent.TButton",command=self.start_capture)
        self.start_button.pack(side="left",padx=7)
        self.start_button.configure(state="disabled")
        ttk.Button(row,text="停止并等待收尾",command=lambda:self.command("stop")).pack(side="left")
        ttk.Label(control,textvariable=self.start_reason,wraplength=1100,foreground="#8b4d11").pack(anchor="w",pady=5)
        cols=("node","channel","online","state","confirmed","gps","clock","queued","packets","rssi","snr")
        self.table=ttk.Treeview(control,columns=cols,show="headings",selectmode="extended",height=10)
        for key,title in zip(cols,["节点","频率MHz","在线","状态","开始确认","GNSS/PPS","时钟ppm","队列","包数","RSSI","SNR"]):
            self.table.heading(key,text=title);self.table.column(key,width=91,anchor="center")
        self.table.pack(fill="both",expand=True)
        for n in NODE_FREQUENCIES:self.table.insert("","end",iid=str(n),values=(n,NODE_FREQUENCIES[n],"否","未发现","否","等待","—",0,0,"—","—"))
        self.table.bind("<<TreeviewSelect>>",self.select_nodes)
        row=ttk.Frame(control);row.pack(fill="x",pady=10)
        self._field(row,"保存目录",self.output,width=65)
        ttk.Button(row,text="选择目录",command=self.choose).pack(side="left",padx=6)
        ttk.Label(control,text="无真实节点/GNSS/时钟就绪时不允许开始；100 Hz在50 Hz验收之前保持禁用。").pack(anchor="w")
        # Diagnostic page
        row=ttk.Frame(diag);row.pack(fill="x")
        ttk.Button(row,text="读取网关服务日志",command=self.read_logs).pack(side="left")
        ttk.Button(row,text="470.3 MHz下发自检",style="Accent.TButton",command=self.self_test).pack(side="left",padx=8)
        ttk.Button(row,text="导出检查报告",command=self.export).pack(side="left")
        ttk.Label(diag,textvariable=self.stats_text).pack(anchor="w",pady=10)
        ttk.Label(diag,text="自检只发送专用非控制报文；TX_ACK与实际发送计数同时通过，才显示网关自检通过。节点确认单独验收。",wraplength=1150).pack(anchor="w",pady=5)
        self.commands=ttk.Treeview(diag,columns=("id","action","run","state","detail"),show="headings",height=7)
        for key,title,width in [("id","命令编号",120),("action","操作",120),("run","运行编号",115),("state","反馈",200),("detail","结果/缺失节点",500)]:
            self.commands.heading(key,text=title);self.commands.column(key,width=width)
        self.commands.pack(fill="x",pady=7)
        self.log=tk.Text(diag,height=12,state="disabled",wrap="word",bg="#10263d",fg="#d8e5f4",font=("Consolas",10))
        self.log.pack(fill="both",expand=True)
        bottom=ttk.Frame(self.root,padding=(18,0,18,10));bottom.pack(fill="x")
        self.bar=ttk.Progressbar(bottom,mode="indeterminate",length=140);self.bar.pack(side="right")
        ttk.Label(bottom,textvariable=self.progress,wraplength=1050).pack(side="left")

    def _field(self,row,label,var,values=None,width=16,show=None):
        ttk.Label(row,text=label).pack(side="left",padx=(0,5))
        widget=ttk.Combobox(row,textvariable=var,values=values,width=width,state="normal") if values is not None else ttk.Entry(row,textvariable=var,width=width,show=show or "")
        widget.pack(side="left",padx=(0,12))
        return widget

    def _job(self,title,function,callback=None):
        if self.busy:
            messagebox.showinfo("正在处理","请等待当前操作结束。")
            return
        self.busy=True;self.progress.set(title);self.bar.start(12)
        def work():
            try:self.events.put(("job_done",(title,function(),callback,None)))
            except Exception as exc:self.events.put(("job_done",(title,None,callback,str(exc))))
        threading.Thread(target=work,name="e870-management",daemon=True).start()

    def refresh_inventory(self):
        from .management import inventory
        def ready(info):
            self.port_combo["values"]=[p["port"] for p in info["ports"]]
            self.adapter_combo["values"]=[a["address"] for a in info["adapters"]]
            self._log("识别到串口："+(", ".join(p["port"] for p in info["ports"]) or "暂无，请检查USB2数据线"))
            if info["ports"] and self.serial_port.get() not in self.port_combo["values"]:self.serial_port.set(info["ports"][0]["port"])
        self._job("正在刷新串口和网卡…",inventory,ready)

    def connect_management(self):
        if self.manager:
            self._job("读取网关…",self.manager.read,self._show_device);return
        from .transports import SerialTransport, SSHTransport
        from .management import GatewayManager
        mode,port,host,user,password=self.mode.get(),self.serial_port.get(),self.ssh_host.get(),self.username.get(),self.password.get()
        def connect():
            transport=SerialTransport(port,user,password) if mode=="USB2串口" else SSHTransport(host,user,password,self.state_dir/"known_hosts")
            manager=GatewayManager(transport,self.state_dir,lambda k,v:self.events.put((k,v)))
            try:return manager,manager.read()
            except Exception:manager.close();raise
        def ready(value):
            self.manager,snap=value;self.preview_data=None;self._show_device(snap)
        self._job("正在连接并读取网关…",connect,ready)

    def _show_device(self,snap):
        self.management_status.set(f"管理已连接：{snap['hostname']} · {self.manager.transport.kind} · 设备 {snap['device_id'][:12]}")
        rf=snap["config"]["SX130x_conf"]["radio_0"];gw=snap["config"]["gateway_conf"]
        text=f"{snap['status']}\n{snap['network']}\n当前目标 {gw.get('server_address')}:{gw.get('serv_port_up')}；TX配置允许范围 {rf.get('tx_freq_min',0)/1e6:g}–{rf.get('tx_freq_max',0)/1e6:g} MHz\n设备日期 {snap['date']}；本机备份/报告使用电脑时间。"
        self.device_text.configure(state="normal");self.device_text.delete("1.0","end");self.device_text.insert("end",text);self.device_text.configure(state="disabled")

    def _need_manager(self):
        if not self.manager:messagebox.showinfo("连接网关","请先建立管理连接。");return False
        return True

    def _idle_for_config(self):
        if self.server:
            messagebox.showinfo("配置操作","请先结束运行并断开接收服务，再修改或恢复配置。");return False
        return self._need_manager()

    def disconnect_management(self):
        if not self.manager or self.busy:return
        manager=self.manager
        def done(_):self.manager=None;self.preview_data=None;self.management_status.set("管理连接：未连接")
        self._job("关闭管理连接…",manager.close,done)

    def backup(self):
        if self._need_manager():self._job("备份并核验配置…",self.manager.backup,lambda p:self._log("备份完成："+str(p)))

    def preview(self):
        if not self._need_manager():return
        computer,cidr=self.computer.get(),self.cidr.get()
        def ready(data):
            self.preview_data=data
            for item in self.diff.get_children():self.diff.delete(item)
            self.diff.insert("","end",values=("eth0网络配置",data["network_before"].replace("\n"," "),data["network_after"].replace("\n"," ")))
            for change in data["changes"]:self.diff.insert("","end",values=(change["field"],str(change["before"]),str(change["after"])))
            self._show_device(self.manager.snapshot)
        self._job("读取并生成配置差异…",lambda:self.manager.preview(computer,cidr),ready)

    def _review(self,title):
        if not self._idle_for_config():return False
        if not self.preview_data or self.preview_data["server"]!=self.computer.get() or self.preview_data["cidr"]!=self.cidr.get():
            messagebox.showinfo("先预览","请先读取并预览当前参数的差异。");return False
        return messagebox.askyesno(title,"是否应用已预览的改动？程序会先备份，失败时恢复；不会启动节点采集。")

    def apply_network(self):
        if self._review("应用网口设置"):
            data=self.preview_data
            self._job("应用网口并核验，失败时恢复…",lambda:self.manager.apply_network(data),self._applied)

    def apply_project(self):
        if self._review("应用项目无线与转发配置"):
            data=self.preview_data
            self._job("应用项目配置并检查服务…",lambda:self.manager.apply_project(data),self._applied)

    def _applied(self,result):
        self.preview_data=None;self._log(result)
        if "config_path" in result:self.gateway_config=result["config_path"];self.tx_range_verified=verify_gateway_file(self.gateway_config)
        self._save_preferences()
        self._show_device(self.manager.snapshot)
        self.progress.set("应用完成。需要下一项改动时请重新读取并预览。")

    def restore(self):
        if not self._idle_for_config():return
        folder=filedialog.askdirectory(title="选择包含manifest.json的备份目录",initialdir=str(self.state_dir/"backups"))
        if folder and messagebox.askyesno("恢复设备配置","将恢复所选备份的网口和网关配置，并核验设备身份。继续？"):
            self._job("恢复备份并验证…",lambda:self.manager.restore(folder),self._applied)
            self.tx_range_verified=False

    def connect(self):
        if self.server:messagebox.showinfo("接收服务","接收服务已运行。");return
        try:
            targets=[int(x.strip()) for x in self.ids.get().split(",")]
            allowed=verify_gateway_file(self.gateway_config)
            sync_mode="pps" if self.sync_mode.get()=="PPS同步" else "raw"
            server=GatewayServer(self.bind.get(),int(self.port.get()),self.output.get(),targets,lambda k,v:self.events.put((k,v)),tx_range_verified=allowed,sync_mode=sync_mode)
            if Path(self.gateway_config).is_file():
                from .configure_gateway import strip_comments
                config=json.loads(strip_comments(Path(self.gateway_config).read_text(encoding="utf-8")))
                server.expected_eui=config.get("gateway_conf",{}).get("gateway_ID","").lower() or None
            server.start();self.server=server;self.tx_range_verified=allowed
            self.sync_mode_combo.configure(state="disabled")
            self.gateway.set(f"电脑接收服务已启动；保存模式：{self.sync_mode.get()}；等待真实网关心跳；"+("文件允许下发，实测单列" if allowed else "仅接收"))
        except Exception as exc:messagebox.showerror("接收服务启动失败",str(exc))

    def disconnect_receiver(self):
        if not self.server:return
        if self.server.run_id or self.server.cancel_run:messagebox.showinfo("仍有运行","请先停止并确认节点回到待机。");return
        if self.busy:messagebox.showinfo("正在测试","请等待当前诊断结束。");return
        server=self.server
        def done(_):self.server=None;self.sync_mode_combo.configure(state="readonly");self.gateway.set("接收服务已停止；端口已释放")
        self._job("关闭接收服务…",server.close,done)

    def command(self,action,**options):
        if not self.server:messagebox.showinfo("接收服务","请先启动接收服务并等待网关心跳。");return
        identifier=self.server.request(action,**options);self._log(f"操作已提交：{identifier}，{ACTIONS.get(action,action)}")

    def start_capture(self):
        try:
            if self.rate.get()!="50":raise ValueError("100Hz需完成50Hz验收后再启用，本阶段仅50Hz")
            duration=int(self.duration.get())
            if duration<0:raise ValueError("时长不能为负数")
            self.command("start",nodes=[int(x) for x in self.ids.get().split(",")],rate=50,sf=6,range_g=int(self.range.get()),duration=duration)
        except ValueError as exc:messagebox.showerror("参数检查",str(exc))

    def select_nodes(self,_=None):
        selected=self.table.selection()
        if selected:self.ids.set(",".join(selected))

    def choose(self):
        folder=filedialog.askdirectory()
        if folder:self.output.set(folder)

    def read_logs(self):
        if self._need_manager():self._job("读取服务日志…",self.manager.logs,lambda value:self._log(value))

    def self_test(self):
        if not self._need_manager() or not self.server:messagebox.showinfo("自检条件","需要管理连接及真实UDP网关连接。");return
        if not messagebox.askyesno("确认LoRa天线","LoRa天线是否已可靠接在网关上？确认后将在470.3 MHz以14 dBm发送3个非控制测试报文，不启动采集。"):return
        from .diagnostics import downlink_self_test
        manager,server=self.manager,self.server
        def done(result):
            self._log(result);self.progress.set("网关下发自检通过，节点收包仍待验收。" if result["passed"] else "网关下发自检未通过，请查看证据与日志。")
        self._job("真实网关下发自检，最多45秒…",lambda:downlink_self_test(manager,server,True),done)

    def export(self):
        path=filedialog.asksaveasfilename(title="导出检查报告",defaultextension=".json",initialfile="E870检查报告.json",filetypes=[("JSON","*.json")])
        if not path:return
        report={"generated_utc":time.time(),"mode":"real_gateway", "sync_mode":self.sync_mode.get(), "stats":self.last_stats,
                "commands":self.command_rows,"self_test":self.manager.last_tx_test if self.manager else None,
                "node_hardware_acceptance":"待实测","events":list(self.history)}
        Path(path).write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        self.progress.set("报告已导出："+path)

    def _log(self,value):
        text=str(value);self.history.append({"utc":time.time(),"text":text})
        self.log.configure(state="normal");self.log.insert("end",time.strftime("%H:%M:%S ")+text+"\n")
        if int(self.log.index("end-1c").split(".")[0])>2000:self.log.delete("1.0","300.0")
        self.log.see("end");self.log.configure(state="disabled")

    def _save_preferences(self):
        self.state_dir.mkdir(parents=True,exist_ok=True)
        self.preferences.write_text(json.dumps({"serial_port":self.serial_port.get(),"ssh_host":self.ssh_host.get(),"computer":self.computer.get(),"cidr":self.cidr.get(),"sync_mode":self.sync_mode.get()},ensure_ascii=False,indent=2),encoding="utf-8")

    def refresh(self):
        for _ in range(400):
            try:kind,data=self.events.get_nowait()
            except queue.Empty:break
            if kind=="job_done":
                title,result,callback,error=data;self.busy=False;self.bar.stop()
                if error:
                    self.progress.set(title+"失败："+error);self._log(error)
                    messagebox.showerror("操作未完成",error)
                else:
                    self.progress.set(title+"完成")
                    if callback:
                        try:callback(result)
                        except Exception as exc:self._log(exc);messagebox.showerror("结果显示失败",str(exc))
            elif kind=="node":
                online=bool(data["last_seen"] and time.monotonic()-data["last_seen"]<=30)
                self.table.item(str(data["node"]),values=(data["node"],data["channel"],"是" if online else "否",STATE_NAMES.get(data["state"],data["state"]),"是" if data["confirmed"] else "否","正常" if data["flags"]&1 else "等待",f'{data["clock_ppm"]:.2f}',data["queued"],data["packets"],data["rssi"],data["snr"]))
            elif kind=="command":
                identifier=data["id"];self.command_rows[identifier]=data
                values=(identifier,ACTIONS.get(data["action"],data["action"]),data.get("run",0),STATES.get(data["state"],data["state"]),data.get("error") or data.get("message") or str(data.get("missing",data.get("found",""))))
                if self.commands.exists(identifier):self.commands.item(identifier,values=values)
                else:self.commands.insert("","end",iid=identifier,values=values)
                if len(self.command_rows)>500:
                    oldest=next(iter(self.command_rows));self.command_rows.pop(oldest);self.commands.delete(oldest)
            elif kind=="transport":
                self.last_stats=data;self.stats_text.set(f'PUSH {data["push"]}   PULL {data["pull"]}   下发 {data["tx"]}   TX_ACK {data["tx_ack"]}   非项目/异常 {data["invalid"]}')
                if self.server:self.gateway.set("电脑接收服务已启动；"+("网关心跳正常" if data["connected"] else "等待网关/心跳过期")+"；节点确认单独显示")
            elif kind=="gateway":pass
            else:self._log(data)
        if self.server:
            try:
                targets=[int(x) for x in self.ids.get().split(",")];reasons=self.server.readiness(targets)
                if any(n not in self.server.nodes for n in targets):reasons.append("目标列表改变后请断开并重新启动接收服务")
            except ValueError:reasons=["目标节点格式错误"]
            if self.rate.get()!="50":reasons.append("本阶段100Hz尚未解锁")
            self.start_reason.set("；".join(reasons) if reasons else "目标节点就绪，可预约开始。")
            self.start_button.configure(state="disabled" if reasons else "normal")
            # Refresh online indicators even when an offline node sends no events.
            for n,node in self.server.nodes.items():
                self.table.set(str(n),"online","是" if node.last_seen and time.monotonic()-node.last_seen<=30 else "否")
                if not node.last_status_seen or time.monotonic()-node.last_status_seen>30:
                    self.table.set(str(n),"gps","状态过期")
                    self.table.set(str(n),"clock","状态过期")
        else:self.start_button.configure(state="disabled")
        if self.close_deadline:
            if self.server and not self.server.run_id and not self.server.cancel_run:
                self.close_deadline=None;self.close()
            elif time.monotonic()>self.close_deadline:
                self.close_deadline=None
                if messagebox.askyesno("停止尚未确认","部分节点未确认停止。是否强制退出并记录此状态？"):
                    self._log("用户强制关闭：停止未确认")
                    self._shutdown(force=True)
        if not self.closing:self.root.after(100,self.refresh)

    def close(self):
        if self.busy:messagebox.showinfo("正在处理","请等待配置或自检完成，再关闭软件。");return
        if self.server and (self.server.run_id or self.server.cancel_run):
            if messagebox.askyesno("仍有采集运行","先下发停止并等待节点收尾，再关闭软件？"):
                self.command("stop");self.close_deadline=time.monotonic()+31
            return
        self._shutdown()

    def _shutdown(self,force=False):
        self._save_preferences()
        self.state_dir.mkdir(parents=True,exist_ok=True)
        (self.state_dir/"last_session.local.json").write_text(json.dumps({"forced":force,"commands":self.command_rows,"events":list(self.history)},ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        server,manager=self.server,self.manager
        self.closing=True
        def close_all():
            try:
                if server:server.close()
                if manager:manager.close()
            finally:self.events.put(("shutdown_done",None))
        threading.Thread(target=close_all,daemon=True).start()
        def wait():
            try:
                while True:
                    event,_=self.events.get_nowait()
                    if event=="shutdown_done":self.password.set("");self.root.destroy();return
            except queue.Empty:pass
            self.root.after(100,wait)
        wait()
