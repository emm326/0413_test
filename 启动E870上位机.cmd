@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\pythonw.exe" (
  echo Python environment missing. Run the project setup first.
  pause
  exit /b 1
)
".venv\Scripts\python.exe" -c "import tkinter,serial,paramiko,psutil"
if errorlevel 1 (
  echo Dependencies missing. Please contact the project maintainer.
  pause
  exit /b 1
)
start "" ".venv\Scripts\pythonw.exe" -X utf8 -m host
