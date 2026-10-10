# 一键打包脚本：生成 dist\VideoCompact\VideoCompact.exe
#
# 最终目录布局：
#   dist\VideoCompact\
#     VideoCompact.exe      外部唯一的 exe
#     bin\                  核心依赖（Python 运行时 + ffmpeg.exe + ffprobe.exe + 图标）
#     input\                默认输入目录
#     output\               默认输出目录
#
# 用法（在项目根目录）：
#   powershell -ExecutionPolicy Bypass -File build.ps1
#   powershell -ExecutionPolicy Bypass -File build.ps1 -PipIndexUrl ""   # 使用官方 PyPI
# 前置条件：已安装 Python 3，且 ffmpeg.exe / ffprobe.exe 已解压到项目根目录。

param(
    [string]$PipIndexUrl = "https://pypi.tuna.tsinghua.edu.cn/simple"
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

if (-not (Test-Path "ffmpeg.exe") -or -not (Test-Path "ffprobe.exe")) {
    Write-Error "未找到 ffmpeg.exe / ffprobe.exe，请先把 ffmpeg.7z 和 ffprobe.7z 解压到项目根目录。"
}

Write-Host "==> 安装打包依赖 (PyInstaller / pystray / pillow)"
$pipArgs = @("install", "--disable-pip-version-check", "pyinstaller", "pystray", "pillow")
if ($PipIndexUrl) {
    $pipArgs += @("-i", $PipIndexUrl)
}
python -m pip @pipArgs

Write-Host "==> 开始打包"
python -m PyInstaller --noconfirm --clean VideoCompact.spec

$out = Join-Path $PSScriptRoot "dist\VideoCompact"
Write-Host "==> 准备默认输入/输出目录"
New-Item -ItemType Directory -Force -Path (Join-Path $out "input")  | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $out "output") | Out-Null

Write-Host ""
Write-Host "==> 完成，产物目录：$out"
Write-Host "    布局："
Write-Host "      VideoCompact.exe"
Write-Host "      bin\   (ffmpeg.exe / ffprobe.exe 及运行时依赖)"
Write-Host "      input\ (放视频)"
Write-Host "      output\"
Write-Host "    把整个 VideoCompact 文件夹复制到目标电脑，双击 VideoCompact.exe 即可，无需 Python 环境。"
