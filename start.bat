@echo off
cd /d "%~dp0"
rem Cloudflare Turnstile test keys: chỉ dùng cho app local tại 127.0.0.1.
if not defined CUSTOMER_LOOKUP_TURNSTILE_SITEKEY set "CUSTOMER_LOOKUP_TURNSTILE_SITEKEY=1x00000000000000000000AA"
if not defined CUSTOMER_LOOKUP_TURNSTILE_SECRET set "CUSTOMER_LOOKUP_TURNSTILE_SECRET=1x0000000000000000000000000000000AA"
echo Starting Phieu CK App at http://localhost:5050
echo Press Ctrl+C to stop.
start "" http://localhost:5050
python app.py
pause
