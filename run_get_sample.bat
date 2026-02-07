@echo off
setlocal

REM --- Python (py launcher) を優先して使う ---
where py >nul 2>nul
if %errorlevel%==0 (
  set "PY=py -3"
) else (
  set "PY=python"
)

echo [1/2] Running NTT sample...
%PY% "C:\Users\yohei\Downloads\3gpp_ana_sep\get_sample_data_ntt\get_sample.py"
if errorlevel 1 (
  echo [ERROR] NTT sample failed.
  exit /b 1
)

echo [2/2] Running OPPO sample...
%PY% "C:\Users\yohei\Downloads\3gpp_ana_sep\get_sample_data_oppo\get_sample.py"
if errorlevel 1 (
  echo [ERROR] OPPO sample failed.
  exit /b 1
)

echo [OK] All samples finished successfully.
exit /b 0
