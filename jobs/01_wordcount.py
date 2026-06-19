#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job 1: Conteo de palabras (word count) clasico sobre los campos de texto.
Tokeniza nombre, apellido, ciudad, estado, ocupacion y nivel_estudios de los
100k registros y cuenta la frecuencia de cada palabra. Usa shuffle (groupBy),
asi que demuestra procesamiento distribuido real entre los workers.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("01-WordCount").getOrCreate()
sc = spark.sparkContext
print("==> Master:", sc.master)

t0 = time.time()
df = spark.read.json("/data/personas.json")

texto = F.concat_ws(" ",
                    F.col("nombre"), F.col("apellido"), F.col("ciudad"),
                    F.col("estado"), F.col("ocupacion"), F.col("nivel_estudios"))

palabras = (df.select(F.explode(F.split(F.lower(texto), r"\s+")).alias("palabra"))
              .filter(F.col("palabra") != ""))

conteo = palabras.groupBy("palabra").count().orderBy(F.desc("count"))

print("==> Top 20 palabras mas frecuentes:")
conteo.show(20, truncate=False)
print("==> Palabras distintas:", conteo.count())
print("==> Registros leidos:", df.count())
print("==> Tiempo total: %.2f s" % (time.time() - t0))

spark.stop()