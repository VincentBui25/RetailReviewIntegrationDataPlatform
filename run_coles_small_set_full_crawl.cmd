@echo off
setlocal

set "ANACONDA_PYTHON=C:\Users\MSI\anaconda3\python.exe"

if not exist "%ANACONDA_PYTHON%" (
    echo ERROR: Anaconda Python was not found at:
    echo   %ANACONDA_PYTHON%
    exit /b 1
)

cd /d "%~dp0"

"%ANACONDA_PYTHON%" -c "import crawlee, selenium, seleniumbase" >nul 2>&1
if errorlevel 1 (
    echo ERROR: The Anaconda base environment is missing required packages.
    echo Install them with:
    echo   "%ANACONDA_PYTHON%" -m pip install -r requirements.txt
    exit /b 1
)

echo Using Anaconda Python: %ANACONDA_PYTHON%
"%ANACONDA_PYTHON%" -m scripts.coles_small_set_full_crawl %*
exit /b %errorlevel%
