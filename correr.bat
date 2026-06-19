@echo off
setlocal
REM ============================================================
REM  Lanza un job de Spark en el cluster.
REM  Uso:    correr.bat <script.py> [master]
REM  Cluster: correr.bat 01_wordcount.py
REM  Local:   correr.bat 04_benchmark.py local[*]
REM ============================================================

REM --- Leer SPARK_MASTER_IP del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "SPARK_MASTER_IP" "%~dp0.env"') do set "SPARK_MASTER_IP=%%B"

if "%SPARK_MASTER_IP%"=="" (
  echo ERROR: No se encontro SPARK_MASTER_IP en .env
  echo Ejecuta: docker exec ts-spark-master tailscale ip -4
  echo y pega la IP en el archivo .env
  exit /b 1
)

if "%~1"=="" (
  echo Uso: correr.bat ^<script.py^> [master]
  echo   Cluster: correr.bat 01_wordcount.py
  echo   Local:   correr.bat 04_benchmark.py local[*]
  exit /b 1
)
set "MASTER=%~2"
if "%MASTER%"=="" set "MASTER=spark://%SPARK_MASTER_IP%:7077"
docker exec -u root -e HOME=/tmp -e SPARK_LOCAL_IP=%SPARK_MASTER_IP% spark-master spark-submit --master %MASTER% --conf spark.driver.host=%SPARK_MASTER_IP% --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.log.level=ERROR /jobs/%~1
endlocal