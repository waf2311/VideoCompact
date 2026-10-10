import os
import queue
import sys
import threading
import tkinter as tk
import webbrowser
from datetime import datetime
from tkinter import filedialog, messagebox, scrolledtext, ttk

import core as cv

try:
    import pystray
    from PIL import Image
    HAS_TRAY = True
except Exception:
    HAS_TRAY = False


def resource_path(name):
    if getattr(sys, "frozen", False):
        base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(sys.executable)))
        candidate = os.path.join(base, name)
        if os.path.exists(candidate):
            return candidate
        candidate = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), name)
        if os.path.exists(candidate):
            return candidate
    return os.path.join(cv.ASSETS_DIR, name)


class VideoCompactApp:
    GITHUB_URL = "https://github.com/waf2311/VideoCompact"

    def __init__(self, root):
        self.root = root
        self.worker = None
        self.log_queue = queue.Queue()
        self.action_queue = queue.Queue()
        self.tray_icon = None
        self.tray_thread = None
        self._running = False

        root.title("小米摄像机录像压缩工具")
        root.geometry("920x780")
        root.minsize(760, 560)

        icon_path = resource_path("app_icon.ico")
        if os.path.exists(icon_path):
            try:
                root.iconbitmap(icon_path)
            except Exception:
                pass

        self._build_ui()
        cv.set_log_sink(self.log_queue.put)
        root.protocol("WM_DELETE_WINDOW", self.on_close)
        root.bind("<Unmap>", self.on_unmap)
        root.after(100, self._drain_log_queue)

    # ---------------- UI ----------------
    def _build_ui(self):
        pad = {"padx": 8, "pady": 6}

        options = ttk.LabelFrame(self.root, text="处理选项")
        options.pack(fill="x", **pad)

        ttk.Label(options, text="输出模式:").grid(row=0, column=0, sticky="w", padx=8, pady=8)
        self.mode_var = tk.StringVar(value="reencode")
        ttk.Radiobutton(
            options, text="重编码（体积最小，画质极小损失）",
            variable=self.mode_var, value="reencode",
        ).grid(row=0, column=1, sticky="w", padx=4)
        ttk.Radiobutton(
            options, text="无损裁剪（画质与源一致，省得少）",
            variable=self.mode_var, value="lossless",
        ).grid(row=0, column=2, sticky="w", padx=4)

        paths = ttk.LabelFrame(self.root, text="目录设置")
        paths.pack(fill="x", **pad)
        paths.columnconfigure(1, weight=1)

        ttk.Label(paths, text="输入目录:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.input_var = tk.StringVar(value=cv.INPUT_DIR)
        ttk.Entry(paths, textvariable=self.input_var).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(paths, text="浏览...", command=self.pick_input).grid(row=0, column=2, padx=8)

        ttk.Label(paths, text="输出目录:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.output_var = tk.StringVar(value=cv.OUTPUT_DIR)
        ttk.Entry(paths, textvariable=self.output_var).grid(row=1, column=1, sticky="ew", padx=4)
        ttk.Button(paths, text="浏览...", command=self.pick_output).grid(row=1, column=2, padx=8)

        self._build_advanced()

        controls = ttk.Frame(self.root)
        controls.pack(fill="x", **pad)
        self.controls_frame = controls
        self.start_btn = ttk.Button(controls, text="开始", command=self.start)
        self.start_btn.pack(side="left", padx=4)
        self.pause_btn = ttk.Button(controls, text="暂停", command=self.toggle_pause, state="disabled")
        self.pause_btn.pack(side="left", padx=4)
        self.stop_btn = ttk.Button(controls, text="停止", command=self.stop, state="disabled")
        self.stop_btn.pack(side="left", padx=4)
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(controls, textvariable=self.status_var).pack(side="right", padx=8)

        # footer 先用 side="bottom" 从底部预留空间，避免窗口空间不足时被日志框挤掉
        self._build_footer()

        log_frame = ttk.LabelFrame(self.root, text="日志输出")
        log_frame.pack(fill="both", expand=True, **pad)
        self.log_frame = log_frame
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap="word", state="disabled", font=("Consolas", 9))
        self.log_text.pack(fill="both", expand=True, padx=6, pady=6)

    def _build_footer(self):
        footer = ttk.Frame(self.root)
        footer.pack(side="bottom", fill="x", padx=8, pady=(0, 6))

        icon_image = None
        try:
            from PIL import Image as PILImage, ImageTk
            icon_path = resource_path("github_mark.png")
            if os.path.exists(icon_path):
                icon_image = ImageTk.PhotoImage(PILImage.open(icon_path).resize((22, 22)))
        except Exception:
            icon_image = None
        self._github_icon_image = icon_image

        link = ttk.Label(
            footer,
            text="免费使用，帮忙点个 Star。",
            image=icon_image,
            compound="left",
            foreground="#0969da",
            cursor="hand2",
        )
        link.pack(side="left")
        link.bind("<Button-1>", lambda _event: webbrowser.open(self.GITHUB_URL))

    # ---------------- 高级选项 ----------------
    def _build_advanced(self):
        pad = {"padx": 8, "pady": 6}
        # 整张卡片仅在“重编码”模式下显示
        advanced = ttk.LabelFrame(self.root, text="高级选项")
        self.advanced = advanced

        self.static_min_row = ttk.Frame(advanced)
        ttk.Label(self.static_min_row, text="静止判定阈值(秒):").pack(side="left")
        self.static_min_var = tk.StringVar(value=str(cv.STATIC_MIN_SECONDS))
        ttk.Spinbox(
            self.static_min_row, from_=0.5, to=600.0, increment=0.5, width=6, textvariable=self.static_min_var
        ).pack(side="left", padx=(8, 4))
        ttk.Label(self.static_min_row, text="连续静止超过该秒数才处理", foreground="#666").pack(side="left")

        self.segment_mode_row = ttk.Frame(advanced)
        ttk.Label(self.segment_mode_row, text="静止段处理:").pack(side="left")
        self.segment_mode_var = tk.StringVar(value=cv.STATIC_SEGMENT_MODE)
        ttk.Radiobutton(
            self.segment_mode_row, text="直接裁掉", variable=self.segment_mode_var, value="drop",
            command=self._update_segment_mode_state,
        ).pack(side="left", padx=4)
        ttk.Radiobutton(
            self.segment_mode_row, text="倍速保留", variable=self.segment_mode_var, value="speedup",
            command=self._update_segment_mode_state,
        ).pack(side="left", padx=4)

        self.static_speed_row = ttk.Frame(advanced)
        ttk.Label(self.static_speed_row, text="倍速倍数:").pack(side="left")
        self.static_speed_var = tk.StringVar(value=str(cv.STATIC_SPEED))
        self.static_speed_spin = ttk.Spinbox(
            self.static_speed_row, from_=1.0, to=64.0, increment=0.5, width=6, textvariable=self.static_speed_var
        )
        self.static_speed_spin.pack(side="left", padx=(8, 4))
        ttk.Label(self.static_speed_row, text="仅「倍速保留」时有效", foreground="#666").pack(side="left")

        self.mode_var.trace_add("write", lambda *_: self._update_mode_visibility())
        self._update_mode_visibility()
        self._update_segment_mode_state()

    def _update_segment_mode_state(self):
        state = "normal" if self.segment_mode_var.get() == "speedup" else "disabled"
        self.static_speed_spin.configure(state=state)

    def _update_mode_visibility(self):
        if self.mode_var.get() == "reencode":
            for row in (self.static_min_row, self.segment_mode_row, self.static_speed_row):
                row.pack(fill="x", padx=8, pady=6)
            pack_args = {"fill": "x", "padx": 8, "pady": 6}
            # 切换显示时插回按钮行上方，保持原始布局顺序
            if getattr(self, "controls_frame", None) is not None:
                pack_args["before"] = self.controls_frame
            self.advanced.pack(**pack_args)
        else:
            self.advanced.pack_forget()

    # ---------------- actions ----------------
    def pick_input(self):
        path = filedialog.askdirectory(title="选择输入目录", initialdir=self.input_var.get() or cv.BASE_DIR)
        if path:
            self.input_var.set(path)

    def pick_output(self):
        path = filedialog.askdirectory(title="选择输出目录", initialdir=self.output_var.get() or cv.BASE_DIR)
        if path:
            self.output_var.set(path)

    def append_log(self, message):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _drain_log_queue(self):
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.append_log(str(message))
        except queue.Empty:
            pass
        try:
            while True:
                action = self.action_queue.get_nowait()
                self._handle_action(action)
        except queue.Empty:
            pass
        self.root.after(100, self._drain_log_queue)

    def _handle_action(self, action):
        if action == "show":
            self.restore_window()
        elif action == "pause":
            if self._running and not cv.CONTROL.is_paused():
                cv.CONTROL.pause()
                self.pause_btn.configure(text="继续")
                self.status_var.set("已暂停")
                cv.log(">>> 已暂停（来自托盘）。")
        elif action == "resume":
            if self._running and cv.CONTROL.is_paused():
                cv.CONTROL.resume()
                self.pause_btn.configure(text="暂停")
                self.status_var.set("运行中")
                cv.log(">>> 已继续（来自托盘）。")
        elif action == "quit":
            self._quit()
        elif action == "finished":
            self._on_finished()

    def start(self):
        if self._running:
            return
        input_dir = self.input_var.get().strip()
        output_dir = self.output_var.get().strip()
        if not input_dir:
            messagebox.showerror("错误", "请选择输入目录。")
            return
        if not output_dir:
            messagebox.showerror("错误", "请选择输出目录。")
            return
        if not os.path.isfile(cv.FFMPEG_PATH) or not os.path.isfile(cv.FFPROBE_PATH):
            messagebox.showerror("错误", f"未找到 ffmpeg/ffprobe:\n{cv.FFMPEG_PATH}\n{cv.FFPROBE_PATH}")
            return
        # 默认目录（exe 同级的 input/output）首次运行可能还不存在，自动创建。
        try:
            os.makedirs(input_dir, exist_ok=True)
            os.makedirs(output_dir, exist_ok=True)
        except OSError as exc:
            messagebox.showerror("错误", f"无法创建目录: {exc}")
            return

        has_video = any(
            name.lower().endswith(".mp4") for name in os.listdir(input_dir)
        )
        if not has_video:
            messagebox.showinfo("提示", f"输入目录里还没有 mp4 文件：\n{input_dir}\n\n请把视频放进去后再开始。")
            return

        # 高级选项校验
        try:
            static_min = float(self.static_min_var.get())
            if static_min <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("错误", "静止判定阈值必须是大于 0 的数字。")
            return
        try:
            static_speed = float(self.static_speed_var.get())
            if static_speed <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("错误", "倍速倍数必须是大于 0 的数字。")
            return

        cv.INPUT_DIR = input_dir
        cv.OUTPUT_DIR = output_dir
        cv.DETECT_DIR = os.path.join(cv.BASE_DIR, "detect")
        cv.OUTPUT_MODE = self.mode_var.get()
        cv.STATIC_MIN_SECONDS = static_min
        cv.STATIC_SEGMENT_MODE = self.segment_mode_var.get()
        cv.STATIC_SPEED = static_speed
        cv.CONTROL.reset()

        self._running = True
        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal", text="暂停")
        self.stop_btn.configure(state="normal")
        self.status_var.set("运行中")

        self.append_log("=" * 40)
        self.append_log(f"开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.append_log(f"输入: {input_dir}")
        self.append_log(f"输出: {output_dir}")
        self.append_log(f"模式: {'无损裁剪' if cv.OUTPUT_MODE == 'lossless' else '重编码'}")
        self.append_log(f"静止判定阈值: {static_min}s")
        if cv.OUTPUT_MODE == "reencode":
            self.append_log(
                f"静止段处理: {'直接裁掉' if cv.STATIC_SEGMENT_MODE == 'drop' else f'倍速保留 x{static_speed}'}"
            )
        self.append_log("=" * 40)

        self.worker = threading.Thread(target=self._run_worker, daemon=True)
        self.worker.start()

    def _run_worker(self):
        try:
            cv.run_pipeline()
            cv.log("")
            cv.log(">>> 全部完成。")
        except cv.JobCancelled:
            cv.log(">>> 已停止。")
        except Exception as exc:
            cv.log(f">>> 运行出错: {exc}")
        finally:
            self.action_queue.put("finished")

    def _on_finished(self):
        self._running = False
        self.start_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled", text="暂停")
        self.stop_btn.configure(state="disabled")
        self.status_var.set("就绪")

    def toggle_pause(self):
        if not self._running:
            return
        if cv.CONTROL.is_paused():
            cv.CONTROL.resume()
            self.pause_btn.configure(text="暂停")
            self.status_var.set("运行中")
            cv.log(">>> 已继续。")
        else:
            cv.CONTROL.pause()
            self.pause_btn.configure(text="继续")
            self.status_var.set("已暂停")
            cv.log(">>> 已暂停（当前编码进程已挂起）。")

    def stop(self):
        if not self._running:
            return
        cv.CONTROL.stop()
        self.status_var.set("正在停止...")
        cv.log(">>> 正在停止...")

    # ---------------- tray ----------------
    def on_unmap(self, event):
        if event.widget is self.root and self.root.state() == "iconic" and HAS_TRAY:
            self.root.after(10, self.hide_to_tray)

    def hide_to_tray(self):
        if not HAS_TRAY:
            self.root.iconify()
            return
        self.root.withdraw()
        if self.tray_icon is None:
            self._create_tray()
        if self.tray_thread is None or not self.tray_thread.is_alive():
            self.tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
            self.tray_thread.start()

    def _create_tray(self):
        icon_path = resource_path("app_icon.ico")
        try:
            image = Image.open(icon_path)
        except Exception:
            image = Image.new("RGB", (64, 64), (37, 99, 235))

        menu = pystray.Menu(
            pystray.MenuItem("显示主界面", self._tray_show, default=True),
            pystray.MenuItem("暂停", self._tray_pause),
            pystray.MenuItem("继续", self._tray_resume),
            pystray.MenuItem("退出", self._tray_quit),
        )
        self.tray_icon = pystray.Icon("VideoCompact", image, "VideoCompact", menu)

    def _tray_show(self, icon=None, item=None):
        self.action_queue.put("show")

    def _tray_pause(self, icon=None, item=None):
        self.action_queue.put("pause")

    def _tray_resume(self, icon=None, item=None):
        self.action_queue.put("resume")

    def _tray_quit(self, icon=None, item=None):
        cv.CONTROL.stop()
        if self.tray_icon is not None:
            try:
                self.tray_icon.stop()
            except Exception:
                pass
            self.tray_icon = None
        self.action_queue.put("quit")

    def restore_window(self):
        self.root.deiconify()
        self.root.state("normal")
        self.root.lift()
        self.root.focus_force()

    def on_close(self):
        if self._running:
            confirmed = messagebox.askyesno(
                "确认退出",
                "当前有任务正在进行，退出将停止任务。\n确定要退出吗？",
            )
            if not confirmed:
                return
        self._quit()

    def _quit(self):
        cv.CONTROL.stop()
        if self.tray_icon is not None:
            try:
                self.tray_icon.stop()
            except Exception:
                pass
        self.root.destroy()


def main():
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    root = tk.Tk()
    app = VideoCompactApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
