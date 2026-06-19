@echo off
setlocal
REM ============================================================
REM  Lanza el CONSUMIDOR de Kafka (lee 3 topics, guarda archivos).
REM  Uso:
REM    correr_consumidor.bat             (espera 100,000 registros)
REM    correr_consumidor.bat 50000       (custom cantidad)
REM
REM  IMPORTANTE: Ejecutar DESPUES del productor.
REM  Los archivos se guardan en ./data/ (personas.json, .csv, .sql)
REM ============================================================

REM --- Leer KAFKA_BROKERS del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "KAFKA_BROKERS" "%~dp0.env"') do set "KAFKA_BROKERS=%%B"
if "%KAFKA_BROKERS%"=="" (
  echo ERROR: No se encontro KAFKA_BROKERS en .env
  exit /b 1
)

set "TOTAL=%~1"
if "%TOTAL%"=="" set "TOTAL=100000"

REM 1. Instalar dependencia
docker exec -u root spark-master pip install kafka-python-ng -q

REM 2. Correr el consumidor
docker exec -u root -e PYTHONUNBUFFERED=1 -e KAFKA=%KAFKA_BROKERS% -e TOTAL=%TOTAL% -e OUTDIR=/data spark-master python -u /jobs/consumidor_kafka.py
endlocal
