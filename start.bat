@echo off
title Telegram Scraper
cd /d "%~dp0"

echo ================================================
echo   Telegram Scraper - Starting...
echo ================================================
echo.

:: Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ from https://python.org
    pause
    exit /b 1
)

:: Check if dependencies are installed
python -c "import flask" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies...
    pip install -r requirements.txt
    echo.
)

:: Start the web app
echo Starting Telegram Scraper GUI...
echo.
echo Opening browser in 3 seconds...
timeout /t 3 /nobreak >nul

:: Open browser
start http://localhost:5000

:: Run the app
python web_app.py

pause
