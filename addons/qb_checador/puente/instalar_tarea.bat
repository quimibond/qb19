@echo off
rem Instala el puente ADMS -> Odoo como tarea programada de Windows que arranca
rem con la PC (sin que nadie inicie sesion) y abre el puerto en el firewall.
rem Ejecutar como administrador desde la carpeta donde esta puente_adms.py.

setlocal
set CARPETA=%~dp0
set PUERTO=8081

for /f "delims=" %%P in ('where pythonw.exe 2^>nul') do set PYW=%%P
if "%PYW%"=="" (
  echo No se encontro pythonw.exe. Instalar Python 3 desde python.org marcando "Add python.exe to PATH".
  pause
  exit /b 1
)

echo Python: %PYW%
echo Carpeta: %CARPETA%

schtasks /Create /F /TN "QB Puente ADMS" /SC ONSTART /RU SYSTEM /RL HIGHEST ^
  /TR "\"%PYW%\" \"%CARPETA%puente_adms.py\""
if errorlevel 1 (
  echo No se pudo crear la tarea. Ejecutar este archivo como administrador.
  pause
  exit /b 1
)

netsh advfirewall firewall delete rule name="QB Puente ADMS" >nul 2>&1
netsh advfirewall firewall add rule name="QB Puente ADMS" dir=in action=allow protocol=TCP localport=%PUERTO% profile=any

schtasks /Run /TN "QB Puente ADMS"
echo.
echo Listo. El puente arranca con Windows. Estado: http://localhost:%PUERTO%/
echo Bitacora: %CARPETA%puente_adms.log
pause
