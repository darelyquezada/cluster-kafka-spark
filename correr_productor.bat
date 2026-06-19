@echo off
setlocal
REM ============================================================
REM  Lanza el PRODUCTOR de Kafka (envia datos a 3 topics).
REM  Uso:
REM    correr_productor.bat              (100,000 registros, modo batch)
REM    correr_productor.bat 50000        (custom cantidad)
REM    correr_productor.bat 0 0.3        (streaming continuo, 0.3s delay)
REM ============================================================

REM --- Leer KAFKA_BROKERS del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "KAFKA_BROKERS" "%~dp0.env"') do set "KAFKA_BROKERS=%%B"
if "%KAFKA_BROKERS%"=="" (
  echo ERROR: No se encontro KAFKA_BROKERS en .env
  exit /b 1
)

set "TOTAL=%~1"
if "%TOTAL%"=="" set "TOTAL=100000"
set "DELAY=%~2"
if "%DELAY%"=="" set "DELAY=0"

REM 1. Instalar dependencia
docker exec -u root spark-master pip install kafka-python-ng -q

REM 2. Correr el productor
docker exec -u root -e PYTHONUNBUFFERED=1 -e KAFKA=%KAFKA_BROKERS% -e TOTAL=%TOTAL% -e DELAY=%DELAY% spark-master python -u /jobs/productor_personas.py
endlocal