@echo off
chcp 65001 >nul
echo === VideoCompact build ===
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0build.ps1"
pause
