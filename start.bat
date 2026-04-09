@echo off
cd /d "%~dp0"
echo Starting Phieu CK App at http://localhost:5050
echo Press Ctrl+C to stop.
start "" http://localhost:5050
python app.py
pause
