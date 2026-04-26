import glob
import json
import os
import re
import shutil
import subprocess
import tempfile

# ================= 配置区 =================
FFMPEG_PATH = r"D:\VideoCompact\ffmpeg.exe"
FFPROBE_PATH = r"D:\VideoCompact\ffprobe.exe"
INPUT_DIR = r"D:\VideoCompact\input"
OUTPUT_DIR = r"D:\VideoCompact\output"

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

SHOWINFO_RE = re.compile(r"pts_time:(\d+(?:\.\d+)?)")
# ========================================


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


def render_with_size_guard(video_path, output_path, media_info, segments):
    source_size = os.path.getsize(video_path)
    temp_dir = tempfile.mkdtemp(prefix="compact_video_", dir=OUTPUT_DIR)
    best_output = None

    try:
        temp_output = os.path.join(temp_dir, f"render_cq_{FIXED_CQ}.mp4")
        cmd = build_ffmpeg_command(video_path, temp_output, media_info, segments, FIXED_CQ)
        print(f"    - 使用 CQ={FIXED_CQ} 编码中...")
        result = run_command(cmd, check=False)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"ffmpeg 编码失败，退出码 {result.returncode}")

        output_size = os.path.getsize(temp_output)
        print(f"    - 输出大小 {output_size / 1024 / 1024:.2f} MB，原始大小 {source_size / 1024 / 1024:.2f} MB")

        if output_size <= source_size:
            shutil.move(temp_output, output_path)
            best_output = output_path
        else:
            shutil.copy2(video_path, output_path)
            best_output = output_path
            print("    - CQ=20 输出大于原文件，已回退为复制原文件。")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def process_one_video(video_path):
    filename = os.path.basename(video_path)
    output_path = os.path.join(OUTPUT_DIR, f"compact_{filename}")
    empty_marker_path = output_path + ".empty"

    print(f"\n>>> 正在处理: {filename}")
    media_info = probe_media(video_path)
    duration = float(media_info["format"]["duration"])

    kept_times = detect_kept_frame_times(video_path)
    segments = build_segments(duration, kept_times)
    summary = summarize_segments(segments)

    print(
        "    - 检测结果: "
        f"{summary['static_count']} 段静止区间, "
        f"合计 {summary['static_input_total']:.2f}s -> {summary['static_output_total']:.2f}s"
    )
    print(
        "    - 处理模式: "
        + ("直接裁剪静止段" if STATIC_SEGMENT_MODE == "drop" else f"静止段 {STATIC_SPEED}x 倍速")
    )

    if is_entire_video_static(segments, duration):
        if os.path.exists(output_path):
            os.remove(output_path)
        with open(empty_marker_path, "w", encoding="utf-8") as marker_file:
            marker_file.write("entire video detected as static\n")
        print(f"    - 整个视频均为静止，已跳过并生成标记: {os.path.basename(empty_marker_path)}")
        return

    if summary["static_count"] == 0:
        if os.path.exists(empty_marker_path):
            os.remove(empty_marker_path)
        shutil.copy2(video_path, output_path)
        print("    - 未发现超过 3 秒的静止区间，已直接复制到 output。")
        return

    if os.path.exists(empty_marker_path):
        os.remove(empty_marker_path)
    render_with_size_guard(video_path, output_path, media_info, segments)
    print(f"    - 完成输出: {os.path.basename(output_path)}")


def process_videos():
    ensure_output_dir()
    video_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.mp4")))

    if not video_files:
        print(f"错误: 未在 {INPUT_DIR} 中找到 mp4 文件。")
        return

    print(f"找到 {len(video_files)} 个视频，准备开始处理...")
    success_count = 0

    for video_path in video_files:
        try:
            process_one_video(video_path)
            success_count += 1
        except Exception as exc:
            print(f"    - 失败: {os.path.basename(video_path)}")
            print(f"      原因: {exc}")

    print("\n" + "=" * 36)
    print(f"处理完成: {success_count}/{len(video_files)}")
    print(f"输出目录: {OUTPUT_DIR}")
    print("=" * 36)


if __name__ == "__main__":
    process_videos()
