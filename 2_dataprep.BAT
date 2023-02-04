@echo off
cd /D "%~dp0"
if not "%1"=="am_admin" (powershell start -verb runas '%0' am_admin & exit /b)
"C:\Users\drbon\anaconda3\python.exe" "C:\Users\drbon\OneDrive\_trading_new\python\workings\projects\tl_sql\streamlit app\prep.py"
pause