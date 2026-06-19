#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job 5: Spark Structured Streaming leyendo de Kafka EN TIEMPO REAL.
Se suscribe al topic "personas" (datos personales) y cada pocos segundos
recalcula cuantas personas van por estado, mostrando el conteo en vivo.

Compatible con el nuevo productor de 3 topics.
Se lanza con el paquete del conector de Kafka (ver correr_streaming_cluster.bat).
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("05-StreamingKafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

KAFKA = os.environ.get("KAFKA", "172.30.0.10:9092")
TOPIC = "personas"
print("==> Leyendo de Kafka:", KAFKA, "topico:", TOPIC)

# Estructura del topic "personas" (datos personales)
esquema = StructType([
    StructField("id_persona", IntegerType()),
    StructField("nombre", StringType()),
    StructField("apellido", StringType()),
    StructField("edad", IntegerType()),
    StructField("genero", StringType()),
    StructField("ciudad", StringType()),
    StructField("estado", StringType()),
])

# 1) leer el flujo de Kafka (readStream, no read)
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")
         .load())

# 2) el mensaje viene en la columna "value" como texto JSON -> lo desempacamos
personas = (crudo.select(F.from_json(F.col("value").cast("string"), esquema).alias("p"))
                 .select("p.*"))

# 3) agregacion en vivo: cuantas personas por estado
conteo = (personas.groupBy("estado")
          .agg(F.count("*").alias("personas"),
               F.round(F.avg("edad"), 1).alias("edad_prom"))
          .orderBy(F.desc("personas")))

# 4) mostrar el resultado en consola, actualizandose cada 15 segundos
q = (conteo.writeStream
     .outputMode("complete")
     .format("console")
     .option("truncate", False)
     .trigger(processingTime="15 seconds")
     .start())

q.awaitTermination()