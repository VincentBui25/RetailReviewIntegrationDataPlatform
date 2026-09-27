@echo off
setlocal

set "ANACONDA_PYTHON=C:\Users\MSI\anaconda3\python.exe"

if not exist "%ANACONDA_PYTHON%" (
    echo ERROR: Anaconda Python was not found at:
    echo   %ANACONDA_PYTHON%
    exit /b 1
)

cd /d "%~dp0"
"%ANACONDA_PYTHON%" -m scripts.google_maps_cooldown_probe %*
exit /b %errorlevel%
