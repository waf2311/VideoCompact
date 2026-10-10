@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

echo === VideoCompact build ===
echo.

if not exist "ffmpeg.exe" (
    echo [ERROR] ffmpeg.exe not found. Extract ffmpeg.7z to the project root first.
    goto :end
)
if not exist "ffprobe.exe" (
    echo [ERROR] ffprobe.exe not found. Extract ffprobe.7z to the project root first.
    goto :end
)

rem Default PyPI mirror is Tsinghua. Pass "default" for official PyPI, or a custom mirror URL.
set "PIP_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple"
if not "%~1"=="" set "PIP_INDEX=%~1"
if /I "%~1"=="default" set "PIP_INDEX="

echo ==^> Installing build deps (PyInstaller / pystray / pillow)
if defined PIP_INDEX (
    python -m pip install --disable-pip-version-check -i "%PIP_INDEX%" pyinstaller pystray pillow
) else (
    python -m pip install --disable-pip-version-check pyinstaller pystray pillow
)
if errorlevel 1 goto :fail

echo ==^> Building with PyInstaller
python -m PyInstaller --noconfirm --clean VideoCompact.spec
if errorlevel 1 goto :fail

echo ==^> Preparing default input / output folders
if not exist "dist\VideoCompact\input"  mkdir "dist\VideoCompact\input"
if not exist "dist\VideoCompact\output" mkdir "dist\VideoCompact\output"

echo.
echo ==^> Done. Output: %CD%\dist\VideoCompact
echo     Layout:
echo       VideoCompact.exe
echo       bin\    (ffmpeg.exe / ffprobe.exe + runtime deps)
echo       input\  (put videos here)
echo       output\
echo     Copy the whole VideoCompact folder to the target PC and run VideoCompact.exe.
goto :end

:fail
echo.
echo [ERROR] Build failed. See output above.

:end
echo.
pause
endlocal
