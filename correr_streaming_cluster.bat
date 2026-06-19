@echo off
setlocal
REM Spark Structured Streaming DISTRIBUIDO y optimizado (Con paquete de Kafka agregado).

REM --- Leer IPs del .env ---
for /f "tokens=1,* delims==" %%A in ('findstr /B "SPARK_MASTER_IP" "%~dp0.env"') do set "SPARK_MASTER_IP=%%B"
for /f "tokens=1,* delims==" %%A in ('findstr /B "KAFKA_BROKERS" "%~dp0.env"') do set "KAFKA_BROKERS=%%B"

if "%SPARK_MASTER_IP%"=="" (
  echo ERROR: No se encontro SPARK_MASTER_IP en .env
  exit /b 1
)
if "%KAFKA_BROKERS%"=="" (
  echo ERROR: No se encontro KAFKA_BROKERS en .env
  exit /b 1
)

docker exec -it -u root -e HOME=/tmp -e SPARK_LOCAL_IP=%SPARK_MASTER_IP% -e KAFKA=%KAFKA_BROKERS% spark-master spark-submit --master spark://%SPARK_MASTER_IP%:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 --conf spark.driver.host=%SPARK_MASTER_IP% --conf spark.cores.max=4 --conf spark.executor.cores=1 --conf spark.sql.shuffle.partitions=4 --conf spark.log.level=WARN /jobs/05_streaming_kafka.py
endlocal