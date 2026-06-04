from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg
import re

# 1. Iniciar sesión conectándose al nodo Maestro
spark = SparkSession.builder \
    .appName("ProyectoDistribuidoSpark") \
    .master("spark://10.13.140.189:7077") \
    .getOrCreate()

print("=====================================================")
print("Iniciando Procesamiento Distribuido con Apache Spark")
print("=====================================================")

# =====================================================
# 2. PROCESAMIENTO DEL ARCHIVO CSV Y JSON
# =====================================================
print("\n[CARGA] Leyendo archivos CSV y JSON distribuidos...")
df_csv = spark.read.csv("/app/data_masiva.csv", header=True, inferSchema=True)
df_json = spark.read.json("/app/data_masiva.json")

# =====================================================
# 3. PROCESAMIENTO DEL ARCHIVO SQL (¡Integrado!)
# =====================================================
print("[CARGA] Procesando archivo SQL de manera distribuida...")

# Leemos las líneas del archivo SQL como un RDD/DataFrame de texto plano
raw_sql_rdd = spark.sparkContext.textFile("/app/data_masiva.sql")

# Filtramos solo las líneas que contienen los INSERTS reales para extraer la data
inserts_rdd = raw_sql_rdd.filter(lambda line: line.startswith("INSERT INTO"))

def parse_sql_line(line):
    # Extraemos el contenido que está dentro de los paréntesis: VALUES (...)
    match = re.search(r"VALUES \((.+)\);", line)
    if match:
        valores_raw = match.group(1)
        # Dividimos por comas, limpiamos espacios y quitamos las comillas simples de los textos
        columnas = [col.strip().strip("'") for col in valores_raw.split(",")]
        
        # Casteamos los tipos de datos correspondientes para el esquema
        return (
            int(columnas[0]),     # id_persona
            columnas[1],          # nombre
            columnas[2],          # apellido
            int(columnas[3]),     # edad
            columnas[4],          # genero
            columnas[5],          # ciudad
            columnas[6],          # estado
            columnas[7],          # ocupacion
            columnas[8],          # nivel_estudios
            float(columnas[9]),   # ingreso_mensual
            columnas[10].lower() == 'true', # activo
            columnas[11]          # fecha_registro
        )
    return None

# Mapeamos las líneas del archivo SQL estructurándolas con el esquema correcto
parsed_sql_rdd = inserts_rdd.map(parse_sql_line).filter(lambda x: x is not None)

# Convertimos a DataFrame de Spark asignando los mismos nombres de columna que el CSV
df_sql = parsed_sql_rdd.toDF(df_csv.columns)

# =====================================================
# 4. VERIFICACIÓN DE VOLUMEN DE DATOS
# =====================================================
print(f"-> Registros cargados desde CSV: {df_csv.count()}")
print(f"-> Registros cargados desde JSON: {df_json.count()}")
print(f"-> Registros cargados e interpretados desde SQL: {df_sql.count()}")

# =====================================================
# 5. CONSULTAS USANDO SPARK SQL (Vistas Temporales)
# =====================================================
print("\n=====================================================")
print("EJECUTANDO CONSULTAS DISTRIBUIDAS CON SPARK SQL")
print("=====================================================")

# Registramos los DataFrames como tablas/vistas temporales en el motor de Spark SQL
df_csv.createOrReplaceTempView("tabla_csv")
df_json.createOrReplaceTempView("tabla_json")
df_sql.createOrReplaceTempView("tabla_sql")

# Consulta 1: Agregación usando Sintaxis SQL pura sobre la data extraída del archivo .sql
print("\n---> INGRESO PROMEDIO POR OCUPACIÓN (Desde archivo SQL usando Spark SQL) <---")
consulta_sql = spark.sql("""
    SELECT ocupacion, 
           COUNT(id_persona) as total_personas, 
           ROUND(AVG(ingreso_mensual), 2) as ingreso_promedio
    FROM tabla_sql
    GROUP BY ocupacion
""")
consulta_sql.show()

# Consulta 2: Demostración de cruce/unión de los 3 formatos en una sola consulta SQL
print("\n---> TOTAL DE REGISTROS COMBINADOS (CSV + JSON + SQL) <---")
consulta_global = spark.sql("""
    SELECT 'CSV' as origen, COUNT(*) as cantidad FROM tabla_csv
    UNION ALL
    SELECT 'JSON' as origen, COUNT(*) as cantidad FROM tabla_json
    UNION ALL
    SELECT 'SQL' as origen, COUNT(*) as cantidad FROM tabla_sql
""")
consulta_global.show()

print("=====================================================")
print("Procesamiento Finalizado. Revisa la interfaz web en el puerto 8080.")
print("=====================================================")

spark.stop()