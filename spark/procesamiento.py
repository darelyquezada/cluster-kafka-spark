from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

# 1. Iniciar sesión conectándose al nodo Maestro
spark = SparkSession.builder \
    .appName("ProyectoDistribuidoSpark") \
    .master("spark://10.13.140.92:7077") \
    .getOrCreate()

print("=====================================================")
print("Iniciando Procesamiento Distribuido con Apache Spark")
print("=====================================================")

# 2. Leer el archivo CSV masivo (procesamiento de archivos CSV)
df = spark.read.csv("/app/data_masiva.csv", header=True, inferSchema=True)

print(f"Total de registros cargados en el clúster: {df.count()}")

# 3. Operaciones de agregación
print("\n---> INGRESO PROMEDIO POR OCUPACIÓN <---")
agregacion = df.groupBy("ocupacion").agg(
    count("id_persona").alias("total_personas"),
    avg("ingreso_mensual").alias("ingreso_promedio")
)
agregacion.show()

# 4. Análisis de datos simples
print("\n---> DISTRIBUCIÓN DE USUARIOS POR GÉNERO <---")
df.groupBy("genero").count().show()

print("\n---> DISTRIBUCIÓN POR NIVEL DE ESTUDIOS <---")
df.groupBy("nivel_estudios").count().show()

print("=====================================================")
print("Procesamiento Finalizado. Revisa la interfaz web en el puerto 8080.")
print("=====================================================")

spark.stop()