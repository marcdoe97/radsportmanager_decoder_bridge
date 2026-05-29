@echo off
cd /d "%~dp0"
where py >nul 2>nul
if %errorlevel%==0 (
    py -3 mylaps_bridge.py
) else (
    python mylaps_bridge.py
)
pause
