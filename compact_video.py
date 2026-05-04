import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ================= 配置区 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FFMPEG_PATH = os.path.join(BASE_DIR, "ffmpeg.exe")
FFPROBE_PATH = os.path.join(BASE_DIR, "ffprobe.exe")
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# 沿用原脚本的 mpdecimate 判静止思路。
DECIMATE_PARAMS = "hi=5000:lo=3000:frac=0.02"

# 静止段处理模式：
# "drop"   = 直接从时间线上裁掉静止段（默认，最快）
# "speedup" = 保留静止画面，但压缩成倍速段
STATIC_SEGMENT_MODE = "drop"

# 仅用于静止检测，不参与最终输出。
# 先降采样到 5fps，再缩小并轻微模糊，能明显减少大批量处理的检测成本。
DETECTION_FPS = 5
DETECTION_PRE_FILTER = (
    f"fps={DETECTION_FPS},"
    "scale=640:-1:flags=fast_bilinear,"
    "avgblur=3:3"
)

# 连续静止超过 3 秒才进入压缩流程。
STATIC_MIN_SECONDS = 3.0

# 静止段压缩倍数。仅在 STATIC_SEGMENT_MODE="speedup" 时生效。
STATIC_SPEED = 8.0

# 为了避免把运动边缘误切进静止段，前后各留一点保护时间。
MOTION_GUARD_SECONDS = 0.20

# 输出帧率固定到 20fps。
OUTPUT_FPS = 20

# NVENC 速度优先。p5 比 p7 明显更快，结合体积兜底更适合大批量任务。
ENCODE_PRESET = "p5"

# 固定使用 CQ 20；若输出仍比原文件大，则直接复制原文件到 output。
FIXED_CQ = 20

# 并发处理数量（按视频）。
MAX_WORKERS = 3

# 编码并发槽位（NVENC 限流，建议 1-2）。
MAX_ENCODE_JOBS = 1

SHOWINFO_RE = re.compile(r"pts_time:(\d+(?:\.\d+)?)")
# ========================================


ENCODE_SEMAPHORE = threading.Semaphore(MAX_ENCODE_JOBS)


class TeeWriter:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
        return len(data)

    def flush(self):
        for stream in self.streams:
            stream.flush()


def run_command(cmd, check=True):
    return subprocess.run(
        cmd,
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def ensure_log_dir():
    os.makedirs(LOG_DIR, exist_ok=True)


def validate_environment():
    if not os.path.isfile(FFMPEG_PATH):
        raise FileNotFoundError(f"未找到 ffmpeg: {FFMPEG_PATH}")
    if not os.path.isfile(FFPROBE_PATH):
        raise FileNotFoundError(f"未找到 ffprobe: {FFPROBE_PATH}")

    if not os.path.isdir(INPUT_DIR):
        raise FileNotFoundError(f"未找到输入目录: {INPUT_DIR}")

    test_paths = ((FFMPEG_PATH, "ffmpeg"), (FFPROBE_PATH, "ffprobe"))
    for tool_path, tool_name in test_paths:
        result = run_command([tool_path, "-version"], check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"{tool_name} 无法执行，退出码 {result.returncode}。"
            )


def probe_media(video_path):
    cmd = [
        FFPROBE_PATH,
        "-hide_banner",
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        video_path,
    ]
    result = run_command(cmd)
    return json.loads(result.stdout)


def pick_stream(info, codec_type):
    for stream in info.get("streams", []):
        if stream.get("codec_type") == codec_type:
            return stream
    return None


def detect_kept_frame_times(video_path):
    cmd = [
        FFMPEG_PATH,
        "-hide_banner",
        "-i",
        video_path,
        "-vf",
        f"{DETECTION_PRE_FILTER},mpdecimate={DECIMATE_PARAMS},showinfo",
        "-an",
        "-f",
        "null",
        "NUL",
    ]
    result = run_command(cmd)
    timestamps = []
    for line in result.stderr.splitlines():
        match = SHOWINFO_RE.search(line)
        if match:
            timestamps.append(float(match.group(1)))
    return timestamps


def build_segments(duration, kept_times):
    if not kept_times:
        return [{"kind": "normal", "start": 0.0, "end": duration}]

    segments = []
    cursor = 0.0
    previous_kept = kept_times[0]

    if previous_kept > STATIC_MIN_SECONDS:
        static_end = max(previous_kept - MOTION_GUARD_SECONDS, 0.0)
        if static_end > STATIC_MIN_SECONDS:
            segments.append({"kind": "static", "start": 0.0, "end": static_end})
            cursor = static_end

    for current_kept in kept_times[1:]:
        gap = current_kept - previous_kept
        static_start = previous_kept + MOTION_GUARD_SECONDS
        static_end = current_kept - MOTION_GUARD_SECONDS
        static_duration = static_end - static_start

        if gap >= STATIC_MIN_SECONDS and static_duration >= STATIC_MIN_SECONDS:
            if static_start > cursor:
                segments.append({"kind": "normal", "start": cursor, "end": static_start})
            segments.append({"kind": "static", "start": static_start, "end": static_end})
            cursor = static_end

        previous_kept = current_kept

    tail_static_start = previous_kept + MOTION_GUARD_SECONDS
    tail_static_duration = duration - tail_static_start
    if tail_static_duration >= STATIC_MIN_SECONDS:
        if tail_static_start > cursor:
            segments.append({"kind": "normal", "start": cursor, "end": tail_static_start})
        segments.append({"kind": "static", "start": tail_static_start, "end": duration})
        cursor = duration

    if cursor < duration:
        segments.append({"kind": "normal", "start": cursor, "end": duration})

    cleaned = []
    for segment in segments:
        start = max(0.0, segment["start"])
        end = min(duration, segment["end"])
        if end - start >= 0.05:
            cleaned.append({"kind": segment["kind"], "start": start, "end": end})

    if not cleaned:
        cleaned.append({"kind": "normal", "start": 0.0, "end": duration})

    merged = [cleaned[0]]
    for segment in cleaned[1:]:
        last = merged[-1]
        same_kind = segment["kind"] == last["kind"]
        contiguous = abs(segment["start"] - last["end"]) < 0.02
        if same_kind and contiguous:
            last["end"] = segment["end"]
        else:
            merged.append(segment)
    return merged


def format_seconds(value):
    return f"{value:.6f}".rstrip("0").rstrip(".") or "0"


def format_hms(total_seconds):
    hours, remainder = divmod(int(total_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} 小时 {minutes} 分 {seconds} 秒"


def format_eta(seconds):
    if seconds <= 0:
        return "0 分 0 秒"
    minutes, remain_seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours} 小时 {minutes} 分 {remain_seconds} 秒"
    return f"{minutes} 分 {remain_seconds} 秒"


def get_effective_segments(segments):
    if STATIC_SEGMENT_MODE != "drop":
        return list(segments)

    kept_segments = [segment for segment in segments if segment["kind"] == "normal"]
    if kept_segments:
        return kept_segments

    # 整段都被判定为静止时，保留极短占位片段，避免输出空文件。
    first = segments[0]
    placeholder_end = min(first["start"] + 0.10, first["end"])
    return [{"kind": "normal", "start": first["start"], "end": placeholder_end}]


def is_entire_video_static(segments, duration):
    if len(segments) != 1:
        return False
    segment = segments[0]
    return (
        segment["kind"] == "static"
        and segment["start"] <= 0.001
        and abs(segment["end"] - duration) <= 0.001
    )


def build_filter_complex(segments, has_audio, audio_stream):
    segments = get_effective_segments(segments)
    filter_parts = []
    concat_inputs = []
    sample_rate = int(audio_stream.get("sample_rate", 48000)) if audio_stream else 48000
    channel_layout = audio_stream.get("channel_layout") if audio_stream else None
    if not channel_layout:
        channels = int(audio_stream.get("channels", 1)) if audio_stream else 1
        channel_layout = "mono" if channels == 1 else "stereo"

    for index, segment in enumerate(segments):
        start = format_seconds(segment["start"])
        end = format_seconds(segment["end"])
        duration = segment["end"] - segment["start"]
        output_duration = duration if segment["kind"] == "normal" else (duration / STATIC_SPEED)

        if segment["kind"] == "normal":
            video_chain = (
                f"[0:v]trim=start={start}:end={end},"
                f"setpts=PTS-STARTPTS[v{index}]"
            )
        else:
            video_chain = (
                f"[0:v]trim=start={start}:end={end},"
                f"setpts=(PTS-STARTPTS)/{STATIC_SPEED}[v{index}]"
            )
        filter_parts.append(video_chain)
        concat_inputs.append(f"[v{index}]")

        if not has_audio:
            continue

        if segment["kind"] == "normal":
            audio_chain = (
                f"[0:a]atrim=start={start}:end={end},"
                f"asetpts=PTS-STARTPTS[a{index}]"
            )
        else:
            silence_duration = format_seconds(output_duration)
            audio_chain = (
                f"anullsrc=r={sample_rate}:cl={channel_layout},"
                f"atrim=duration={silence_duration},"
                f"asetpts=N/SR/TB[a{index}]"
            )
        filter_parts.append(audio_chain)
        concat_inputs.append(f"[a{index}]")

    if has_audio:
        filter_parts.append(
            "".join(concat_inputs)
            + f"concat=n={len(segments)}:v=1:a=1[vcat][acat]"
        )
        filter_parts.append(f"[vcat]fps={OUTPUT_FPS},format=yuv420p[vout]")
        return ";".join(filter_parts), "vout", "acat"

    filter_parts.append(
        "".join(concat_inputs) + f"concat=n={len(segments)}:v=1:a=0[vcat]"
    )
    filter_parts.append(f"[vcat]fps={OUTPUT_FPS},format=yuv420p[vout]")
    return ";".join(filter_parts), "vout", None


def build_ffmpeg_command(video_path, output_path, media_info, segments, cq_value):
    format_info = media_info["format"]
    video_stream = pick_stream(media_info, "video")
    audio_stream = pick_stream(media_info, "audio")
    has_audio = audio_stream is not None

    filter_complex, video_label, audio_label = build_filter_complex(
        segments, has_audio, audio_stream
    )

    source_video_bitrate = int(video_stream.get("bit_rate") or format_info.get("bit_rate") or 1500000)
    source_audio_bitrate = int(audio_stream.get("bit_rate") or 64000) if has_audio else 0
    maxrate = max(int(source_video_bitrate * 1.2), 1000000)
    bufsize = max(int(source_video_bitrate * 2), 2000000)

    cmd = [
        FFMPEG_PATH,
        "-hide_banner",
        "-y",
        "-i",
        video_path,
        "-filter_complex",
        filter_complex,
        "-map",
        f"[{video_label}]",
    ]

    if audio_label:
        cmd.extend(["-map", f"[{audio_label}]"])

    cmd.extend(
        [
            "-map_metadata",
            "0",
            "-c:v",
            "hevc_nvenc",
            "-preset",
            ENCODE_PRESET,
            "-rc",
            "vbr",
            "-cq",
            str(cq_value),
            "-b:v",
            str(source_video_bitrate),
            "-maxrate",
            str(maxrate),
            "-bufsize",
            str(bufsize),
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "main",
            "-tag:v",
            "hvc1",
            "-movflags",
            "+faststart",
        ]
    )

    if has_audio:
        sample_rate = audio_stream.get("sample_rate", "48000")
        channels = audio_stream.get("channels", 1)
        cmd.extend(
            [
                "-c:a",
                "libopus",
                "-b:a",
                str(source_audio_bitrate),
                "-ar",
                str(sample_rate),
                "-ac",
                str(channels),
            ]
        )
    else:
        cmd.append("-an")

    cmd.append(output_path)
    return cmd


def summarize_segments(segments):
    static_segments = [item for item in segments if item["kind"] == "static"]
    normal_segments = [item for item in segments if item["kind"] == "normal"]
    static_input_total = sum(item["end"] - item["start"] for item in static_segments)
    if STATIC_SEGMENT_MODE == "drop":
        static_output_total = 0.0
    else:
        static_output_total = sum((item["end"] - item["start"]) / STATIC_SPEED for item in static_segments)
    return {
        "normal_count": len(normal_segments),
        "static_count": len(static_segments),
        "static_input_total": static_input_total,
        "static_output_total": static_output_total,
    }


def render_with_size_guard(video_path, output_path, media_info, segments, logger):
    source_size = os.path.getsize(video_path)
    temp_dir = tempfile.mkdtemp(prefix="compact_video_")

    try:
        temp_output = os.path.join(temp_dir, f"render_cq_{FIXED_CQ}.mp4")
        cmd = build_ffmpeg_command(video_path, temp_output, media_info, segments, FIXED_CQ)
        logger(f"    - 使用 CQ={FIXED_CQ} 编码中...")
        result = run_command(cmd, check=False)
        if result.returncode != 0:
            logger(result.stderr)
            raise RuntimeError(f"ffmpeg 编码失败，退出码 {result.returncode}")

        output_size = os.path.getsize(temp_output)
        logger(f"    - 输出大小 {output_size / 1024 / 1024:.2f} MB，原始大小 {source_size / 1024 / 1024:.2f} MB")

        if output_size <= source_size:
            shutil.move(temp_output, output_path)
            return "encoded"
        else:
            shutil.copy2(video_path, output_path)
            logger("    - CQ=20 输出大于原文件，已回退为直接复制源文件到 output。")
            return "copied_fallback"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def process_one_video(video_path):
    started_at = time.perf_counter()
    filename = os.path.basename(video_path)
    output_path = os.path.join(OUTPUT_DIR, f"compact_{filename}")

    def vlog(message):
        print(f"[{filename}] {message}")

    print(f"\n[{filename}] >>> 正在处理")
    try:
        media_info = probe_media(video_path)
        duration = float(media_info["format"]["duration"])

        kept_times = detect_kept_frame_times(video_path)
        segments = build_segments(duration, kept_times)
        summary = summarize_segments(segments)

        vlog(
            "    - 检测结果: "
            f"{summary['static_count']} 段静止区间, "
            f"合计 {summary['static_input_total']:.2f}s -> {summary['static_output_total']:.2f}s"
        )
        vlog(
            "    - 处理模式: "
            + ("直接裁剪静止段" if STATIC_SEGMENT_MODE == "drop" else f"静止段 {STATIC_SPEED}x 倍速")
        )

        if is_entire_video_static(segments, duration):
            vlog("    - 整个视频均为静止，已跳过，不输出到 output。")
            vlog(f"    - 未生成输出文件: {os.path.basename(output_path)}")
            return "all_static"

        if summary["static_count"] == 0:
            shutil.copy2(video_path, output_path)
            vlog("    - 未发现超过 3 秒的静止区间，已直接复制源文件到 output。")
            return "copied_no_static"

        with ENCODE_SEMAPHORE:
            result = render_with_size_guard(video_path, output_path, media_info, segments, vlog)
        vlog(f"    - 完成输出: {os.path.basename(output_path)}")
        return result
    finally:
        elapsed_seconds = int(time.perf_counter() - started_at)
        minutes, seconds = divmod(elapsed_seconds, 60)
        vlog(f"    - 当前视频耗时: {minutes} 分 {seconds} 秒")


def process_videos():
    started_at = time.perf_counter()
    ensure_output_dir()
    video_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.mp4")))

    if not video_files:
        print(f"错误: 未在 {INPUT_DIR} 中找到 mp4 文件。")
        return

    existing_outputs = {
        name for name in os.listdir(OUTPUT_DIR)
        if name.lower().endswith(".mp4") and name.startswith("compact_")
    }
    skipped_existing_count = 0
    pending_video_files = []
    for video_path in video_files:
        filename = os.path.basename(video_path)
        compact_name = f"compact_{filename}"
        if compact_name in existing_outputs:
            skipped_existing_count += 1
            print(f"[{filename}] >>> 已跳过，output 已存在同名文件: {compact_name}")
        else:
            pending_video_files.append(video_path)

    print(f"找到 {len(video_files)} 个视频，其中 {len(pending_video_files)} 个待处理，{skipped_existing_count} 个已存在输出而跳过。")
    if not pending_video_files:
        print("无需处理，全部视频在 output 中已有对应 compact 文件。")
        return

    success_count = 0
    all_static_count = 0
    copied_no_static_count = 0
    copied_fallback_count = 0
    encoded_count = 0
    failed_videos = []
    completed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(process_one_video, video_path): video_path for video_path in pending_video_files}
        for future in as_completed(future_map):
            video_path = future_map[future]
            completed_count += 1
            try:
                result = future.result()
                success_count += 1
                if result == "all_static":
                    all_static_count += 1
                elif result == "copied_no_static":
                    copied_no_static_count += 1
                elif result == "copied_fallback":
                    copied_fallback_count += 1
                elif result == "encoded":
                    encoded_count += 1
            except Exception as exc:
                failed_videos.append((os.path.basename(video_path), str(exc)))
                print(f"[{os.path.basename(video_path)}]    - 失败: {exc}")

            elapsed = time.perf_counter() - started_at
            avg_per_video = elapsed / completed_count if completed_count else 0
            remaining = len(pending_video_files) - completed_count
            eta_seconds = avg_per_video * remaining
            print(
                f"[进度] {completed_count}/{len(pending_video_files)} 已完成, "
                f"ETA 约 {format_eta(eta_seconds)}"
            )

    total_elapsed_seconds = int(time.perf_counter() - started_at)
    total_elapsed_hours = total_elapsed_seconds / 3600.0
    normal_processed_count = success_count - all_static_count
    copied_source_count = copied_no_static_count + copied_fallback_count

    print("\n" + "=" * 36)
    print(f"处理完成: {success_count}/{len(pending_video_files)}")
    print(f"总视频数: {len(video_files)}")
    print(f"已存在输出跳过数: {skipped_existing_count}")
    print(f"正常处理数: {normal_processed_count}")
    print(f"编码压缩输出数: {encoded_count}")
    print(f"直接复制源文件数(合计): {copied_source_count}")
    print(f"  - 无静止区间直接复制: {copied_no_static_count}")
    print(f"  - 编码变大回退复制: {copied_fallback_count}")
    print(f"全静止跳过数: {all_static_count}")
    print(f"总耗时: {total_elapsed_hours:.2f} 小时")
    print(f"总耗时(时分秒): {format_hms(total_elapsed_seconds)}")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"日志目录: {LOG_DIR}")
    if failed_videos:
        print("失败清单:")
        for failed_name, failed_reason in failed_videos:
            print(f"  - {failed_name}: {failed_reason}")
    else:
        print("失败清单: 无")
    print("=" * 36)


if __name__ == "__main__":
    ensure_output_dir()
    ensure_log_dir()
    log_filename = f"run_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_path = os.path.join(LOG_DIR, log_filename)
    with open(log_path, "w", encoding="utf-8") as log_file:
        tee = TeeWriter(sys.stdout, log_file)
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        sys.stdout = tee
        sys.stderr = tee
        try:
            print(f"运行日志文件: {log_path}")
            validate_environment()
            process_videos()
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr
