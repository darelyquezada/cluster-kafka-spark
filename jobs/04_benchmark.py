#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job 4: Benchmark distribuido vs local.
Corre la MISMA agregacion pesada y mide el tiempo. Ejecutalo dos veces:
  - cluster:  --master spark://100.93.255.118:7077
  - local:    --master local[*]
y compara los tiempos que imprime.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("04-Benchmark").getOrCreate()
modo = spark.sparkContext.master
print("==> Modo de ejecucion:", modo)

df = spark.read.json("/data/personas.json")

t0 = time.time()
# agregacion pesada: varias agrupaciones que fuerzan shuffle
r1 = df.groupBy("estado", "ocupacion").agg(
        F.avg("ingreso_mensual").alias("ing"),
        F.avg("edad").alias("ed"),
        F.count("*").alias("n")).collect()
r2 = df.groupBy("nivel_estudios", "genero").count().collect()
total = df.count()
elapsed = time.time() - t0

print("==> Registros procesados:", total)
print("==> Combinaciones estado+ocupacion:", len(r1))
print("==> Combinaciones nivel+genero:", len(r2))
print("============================================")
print("   MODO: %-25s TIEMPO: %.2f s" % (modo, elapsed))
print("============================================")
spark.stop()