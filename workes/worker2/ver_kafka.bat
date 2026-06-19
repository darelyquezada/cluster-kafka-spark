@echo off
setlocal
REM ============================================================
REM  Monitor de Kafka para Worker 2.
REM  Muestra los registros que hay en Kafka (desde el inicio).
REM  Uso:
REM    ver_kafka.bat              (primeros 50 mensajes)
REM    ver_kafka.bat 200          (primeros 200 mensajes)
REM    ver_kafka.bat 0            (todos, sin limite)
REM    ver_kafka.bat 100 0.1      (100 mensajes, 0.1s entre cada uno)
REM ============================================================

REM --- Leer KAFKA_BROKERS del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "KAFKA_BROKERS" "%~dp0.env"') do set "KAFKA_BROKERS=%%B"
if "%KAFKA_BROKERS%"=="" (
  echo ERROR: No se encontro KAFKA_BROKERS en .env
  exit /b 1
)

set "MAX=%~1"
if "%MAX%"=="" set "MAX=50"
set "DELAY=%~2"
if "%DELAY%"=="" set "DELAY=0"

REM 1. Instalar dependencia
docker exec -u root spark-worker-2 pip install kafka-python-ng -q

REM 2. Copiar el script al contenedor
docker cp "%~dp0ver_kafka.py" spark-worker-2:/data/ver_kafka.py

REM 3. Correr el monitor
docker exec -it -u root -e KAFKA=%KAFKA_BROKERS% -e MAX=%MAX% -e DELAY=%DELAY% spark-worker-2 python3 -u /data/ver_kafka.py
endlocal
