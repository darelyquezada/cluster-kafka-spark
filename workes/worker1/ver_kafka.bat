@echo off
setlocal
REM ============================================================
REM  Monitor de Kafka para Worker 1.
REM  Muestra en tiempo real los mensajes que llegan a Kafka.
REM  Uso:
REM    ver_kafka.bat              (ver todos los topics, sin limite)
REM    ver_kafka.bat 200          (ver solo los primeros 200 mensajes)
REM ============================================================

REM --- Leer KAFKA_BROKERS del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "KAFKA_BROKERS" "%~dp0.env"') do set "KAFKA_BROKERS=%%B"
if "%KAFKA_BROKERS%"=="" (
  echo ERROR: No se encontro KAFKA_BROKERS en .env
  exit /b 1
)

set "MAX=%~1"
if "%MAX%"=="" set "MAX=0"

REM 1. Instalar dependencia
docker exec -u root spark-worker-1 pip install kafka-python-ng -q

REM 2. Copiar el script al contenedor
docker cp "%~dp0ver_kafka.py" spark-worker-1:/data/ver_kafka.py

REM 3. Correr el monitor
docker exec -it -u root -e KAFKA=%KAFKA_BROKERS% -e MAX=%MAX% spark-worker-1 python3 -u /data/ver_kafka.py
endlocal
