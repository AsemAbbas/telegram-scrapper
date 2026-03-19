@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "create-shortcut.ps1"
