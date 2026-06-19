#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job 2: Lectura y procesamiento de los formatos CSV y JSON.
Lee ambos, muestra el esquema inferido, cuenta registros y aplica filtros
sencillos. Demuestra que el cluster procesa los dos formatos pedidos.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("02-ProcesarCSV-JSON").getOrCreate()
print("==> Master:", spark.sparkContext.master)
t0 = time.time()

# ---- JSON ----
print("\n========== JSON (/data/personas.json) ==========")
dj = spark.read.json("/data/personas.json")
dj.printSchema()
print("Total registros JSON:", dj.count())
print("Muestra:")
dj.show(5, truncate=False)

# ---- CSV ----
print("\n========== CSV (/data/personas.csv) ==========")
dc = spark.read.option("header", True).option("inferSchema", True).csv("/data/personas.csv")
dc.printSchema()
print("Total registros CSV:", dc.count())

# ---- Filtros de ejemplo ----
print("\n========== Filtros ==========")
activos = dc.filter(F.col("activo") == True).count()
mayores = dc.filter(F.col("edad") >= 30).count()
ricos = dc.filter(F.col("ingreso_mensual") > 50000).count()
print("Personas activas:           ", activos)
print("Personas de 30 anios o mas: ", mayores)
print("Ingreso mensual > 50,000:   ", ricos)

print("\n==> Tiempo total: %.2f s" % (time.time() - t0))
spark.stop()