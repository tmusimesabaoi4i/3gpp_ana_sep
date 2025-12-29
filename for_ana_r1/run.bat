@echo off
setlocal enabledelayedexpansion

echo === Running main.py ===
python main.py
if errorlevel 1 (
    echo [ERROR] main.py failed.
    goto end
)

echo === Running main_jp.py ===
python main_jp.py
if errorlevel 1 (
    echo [ERROR] main_jp.py failed.
    goto end
)

echo === Running main_ts_tr.py ===
python main_ts_tr.py
if errorlevel 1 (
    echo [ERROR] main_ts_tr.py failed.
    goto end
)

echo === Running main_ts_tr_jp.py ===
python main_ts_tr_jp.py
if errorlevel 1 (
    echo [ERROR] main_ts_tr_jp.py failed.
    goto end
)

:end
echo.
echo === All scripts finished ===
pause