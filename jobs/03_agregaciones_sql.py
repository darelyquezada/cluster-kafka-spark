#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job 3: Operaciones de agregacion (group by) + consultas Spark SQL.
Agrupa por estado, ocupacion y nivel de estudios calculando conteos y
promedios de ingreso. Tambien registra una vista temporal y corre SQL.
Estas operaciones usan shuffle => trabajo distribuido entre los workers.
"""
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("03-Agregaciones-SQL").getOrCreate()
print("==> Master:", spark.sparkContext.master)
t0 = time.time()

df = spark.read.json("/data/personas.json")
df.cache()

print("\n== Personas por estado (top 10) ==")
(df.groupBy("estado").count().orderBy(F.desc("count"))).show(10, truncate=False)

print("\n== Ingreso mensual promedio por ocupacion ==")
(df.groupBy("ocupacion")
   .agg(F.round(F.avg("ingreso_mensual"), 2).alias("ingreso_prom"),
        F.count("*").alias("personas"))
   .orderBy(F.desc("ingreso_prom"))).show(20, truncate=False)

print("\n== Edad promedio por nivel de estudios ==")
(df.groupBy("nivel_estudios")
   .agg(F.round(F.avg("edad"), 1).alias("edad_prom"),
        F.count("*").alias("personas"))
   .orderBy(F.desc("edad_prom"))).show(truncate=False)

# ---- Spark SQL (Consultas requeridas) ----
print("\n========== Spark SQL (Consultas sobre datos combinados) ==========")
df.createOrReplaceTempView("personas")

print("\n[Consulta 1] Búsqueda/Filtro: Personas ACTIVAS con maestría o doctorado:")
spark.sql("""
    SELECT id_persona, nombre, apellido, nivel_estudios, ocupacion, ingreso_mensual
    FROM personas
    WHERE activo = true AND nivel_estudios IN ('Maestria', 'Doctorado')
    ORDER BY ingreso_mensual DESC
    LIMIT 10
""").show(truncate=False)

print("\n[Consulta 2] Agrupación: Salario promedio y total de personas por ocupación y género:")
spark.sql("""
    SELECT ocupacion, genero, COUNT(*) as total_personas,
           ROUND(AVG(ingreso_mensual), 2) as salario_promedio
    FROM personas
    GROUP BY ocupacion, genero
    ORDER BY ocupacion, genero
""").show(truncate=False)

print("\n[Consulta 3] Búsqueda: Top 5 ciudades con mayor cantidad de ingenieros o programadores:")
spark.sql("""
    SELECT ciudad, estado, COUNT(*) as cantidad_tech
    FROM personas
    WHERE ocupacion IN ('Ingeniero', 'Programador')
    GROUP BY ciudad, estado
    ORDER BY cantidad_tech DESC
    LIMIT 5
""").show(truncate=False)

print("\n==> Tiempo total: %.2f s" % (time.time() - t0))
spark.stop()