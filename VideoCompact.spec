# -*- mode: python ; coding: utf-8 -*-
# 打包配置：
#   - 入口 gui.py（GUI 版本）
#   - 目录版（onedir），但把运行时目录改名为 bin
#   - 布局：
#       dist\VideoCompact\
#         VideoCompact.exe      外部唯一的 exe
#         bin\                  所有核心依赖（Python 运行时 + ffmpeg.exe + ffprobe.exe + 图标）
#         input\  output\       默认输入输出目录（由 build.bat / 首次运行创建）
#         detect\ logs\         运行后自动生成

block_cipher = None

a = Analysis(
    ["src/gui.py"],
    pathex=["src"],
    binaries=[
        ("ffmpeg.exe", "."),
        ("ffprobe.exe", "."),
    ],
    datas=[
        ("assets/app_icon.ico", "."),
        ("assets/app_icon.png", "."),
        ("assets/github_mark.png", "."),
    ],
    hiddenimports=[
        "pystray._win32",
        "PIL.Image",
        "PIL.ImageDraw",
        "PIL.ImageFont",
        "PIL._tkinter_finder",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "matplotlib",
        "numpy",
        "pandas",
        "scipy",
        "PyQt5",
        "PySide2",
        "PySide6",
        "IPython",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="VideoCompact",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon="assets/app_icon.ico",
    # 把默认的 _internal 目录改名为 bin
    contents_directory="bin",
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="VideoCompact",
)
