#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consumidor de Kafka: lee de 3 topics, visualiza los datos recibidos y los
almacena en archivos .json, .csv y .sql.

Topics que consume:
  - personas:  datos personales  (id, nombre, apellido, edad, genero, ciudad, estado)
  - empleos:   datos laborales   (id, ocupacion, nivel_estudios, ingreso_mensual)
  - actividad: estado y registro (id, activo, fecha_registro)

El consumidor:
  1. Lee mensajes de los 3 topics
  2. Los une por id_persona (JOIN)
  3. Muestra en pantalla los datos recibidos
  4. Al alcanzar el objetivo (100,000), guarda en:
     - /data/personas.json  (JSON Lines)
     - /data/personas.csv   (CSV con header)
     - /data/personas.sql   (CREATE TABLE + INSERTs)

Variables de entorno:
  KAFKA  = brokers de Kafka (default: 172.30.0.10:9092)
  TOTAL  = registros objetivo (default: 100000)
"""
import json, os, csv, sys, time
from collections import defaultdict
from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
KAFKA = os.environ.get("KAFKA", "172.30.0.10:9092")
TOTAL = int(os.environ.get("TOTAL", "100000"))
OUTDIR = os.environ.get("OUTDIR", "/data")
TOPICS = ["personas", "empleos", "actividad"]

COLUMNAS = ["id_persona", "nombre", "apellido", "edad", "genero", "ciudad",
            "estado", "ocupacion", "nivel_estudios", "ingreso_mensual",
            "activo", "fecha_registro"]

print("=" * 60)
print("  CONSUMIDOR DE KAFKA - 3 TOPICS")
print("=" * 60)
print(f"  Kafka:     {KAFKA}")
print(f"  Topics:    {', '.join(TOPICS)}")
print(f"  Objetivo:  {TOTAL:,} registros completos")
print(f"  Salida:    {OUTDIR}/personas.[json|csv|sql]")
print("=" * 60)
print()

# ---------------------------------------------------------------------------
# Conectar al consumidor
# ---------------------------------------------------------------------------
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumidor-archivos",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=15000,   # 15s sin mensajes = salir
)

# ---------------------------------------------------------------------------
# Acumular mensajes y unir por id_persona
# ---------------------------------------------------------------------------
datos = defaultdict(dict)          # {id_persona: {campo: valor, ...}}
recibidos = {"personas": 0, "empleos": 0, "actividad": 0}
completos = 0                      # registros con los 12 campos

t0 = time.time()

try:
    for msg in consumer:
        topic = msg.topic
        valor = msg.value
        id_p = valor["id_persona"]

        # Contar antes de merge para detectar nuevos completos
        campos_antes = len(datos[id_p])

        # Merge: unir campos del mensaje al registro
        datos[id_p].update(valor)
        recibidos[topic] += 1

        campos_despues = len(datos[id_p])

        # Si ahora tiene 12 campos, es un registro completo nuevo
        if campos_antes < 12 and campos_despues >= 12:
            completos += 1

        # --- Visualizar datos recibidos ---
        total_msgs = sum(recibidos.values())
        if total_msgs <= 30:
            # Primeros mensajes: mostrar detalle completo
            print(f"  [{topic:>10}] {json.dumps(valor, ensure_ascii=False)}")
        elif total_msgs == 31:
            print(f"\n  ... (mostrando progreso cada 5,000 registros) ...\n")

        if completos % 5000 == 0 and completos > 0 and campos_antes < 12:
            elapsed = time.time() - t0
            rate = completos / elapsed if elapsed > 0 else 0
            print(f"  Completos: {completos:>7,}/{TOTAL:,}  "
                  f"| msgs: personas={recibidos['personas']:,}  "
                  f"empleos={recibidos['empleos']:,}  "
                  f"actividad={recibidos['actividad']:,}  "
                  f"| {rate:,.0f} reg/s")

        # Verificar objetivo
        if completos >= TOTAL:
            print(f"\n  Objetivo alcanzado: {completos:,} registros completos!")
            break

except KeyboardInterrupt:
    print(f"\n  Interrumpido manualmente. Registros completos: {completos:,}")

consumer.close()

# ---------------------------------------------------------------------------
# Filtrar solo registros completos (12 campos)
# ---------------------------------------------------------------------------
registros = [d for d in datos.values() if len(d) >= 12]
registros.sort(key=lambda x: x["id_persona"])

if not registros:
    print("\n  ERROR: No se recibieron registros completos.")
    print("  Asegurate de que el productor haya enviado datos primero.")
    sys.exit(1)

elapsed = time.time() - t0
print(f"\n  Total recibido: {sum(recibidos.values()):,} mensajes en {elapsed:.1f}s")
print(f"  Registros completos: {len(registros):,}")

# ---------------------------------------------------------------------------
# Guardar en .json (JSON Lines)
# ---------------------------------------------------------------------------
os.makedirs(OUTDIR, exist_ok=True)
ruta_json = os.path.join(OUTDIR, "personas.json")
with open(ruta_json, "w", encoding="utf-8") as f:
    for p in registros:
        f.write(json.dumps(p, ensure_ascii=False) + "\n")
size_json = os.path.getsize(ruta_json) / 1048576.0
print(f"\n  Guardado: {ruta_json}  ({size_json:.1f} MB)")

# ---------------------------------------------------------------------------
# Guardar en .csv
# ---------------------------------------------------------------------------
ruta_csv = os.path.join(OUTDIR, "personas.csv")
with open(ruta_csv, "w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=COLUMNAS)
    w.writeheader()
    for p in registros:
        fila = dict(p)
        fila["activo"] = "true" if p["activo"] else "false"
        w.writerow(fila)
size_csv = os.path.getsize(ruta_csv) / 1048576.0
print(f"  Guardado: {ruta_csv}  ({size_csv:.1f} MB)")

# ---------------------------------------------------------------------------
# Guardar en .sql (CREATE TABLE + INSERTs por lotes)
# ---------------------------------------------------------------------------
def val_sql(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


ruta_sql = os.path.join(OUTDIR, "personas.sql")
with open(ruta_sql, "w", encoding="utf-8") as f:
    f.write("CREATE TABLE IF NOT EXISTS personas (\n")
    f.write("    id_persona      INT PRIMARY KEY,\n")
    f.write("    nombre          VARCHAR(50),\n")
    f.write("    apellido        VARCHAR(50),\n")
    f.write("    edad            INT,\n")
    f.write("    genero          CHAR(1),\n")
    f.write("    ciudad          VARCHAR(50),\n")
    f.write("    estado          VARCHAR(50),\n")
    f.write("    ocupacion       VARCHAR(50),\n")
    f.write("    nivel_estudios  VARCHAR(30),\n")
    f.write("    ingreso_mensual DECIMAL(10,2),\n")
    f.write("    activo          BOOLEAN,\n")
    f.write("    fecha_registro  DATE\n")
    f.write(");\n\n")

    lote = 500
    for i in range(0, len(registros), lote):
        grupo = registros[i:i + lote]
        f.write("INSERT INTO personas (" + ", ".join(COLUMNAS) + ") VALUES\n")
        filas = []
        for p in grupo:
            vals = ", ".join(val_sql(p[c]) for c in COLUMNAS)
            filas.append("    (" + vals + ")")
        f.write(",\n".join(filas) + ";\n\n")

size_sql = os.path.getsize(ruta_sql) / 1048576.0
print(f"  Guardado: {ruta_sql}  ({size_sql:.1f} MB)")

print("\n" + "=" * 60)
print(f"  COMPLETADO: {len(registros):,} registros en 3 formatos")
print(f"  Archivos en: {os.path.abspath(OUTDIR)}")
print("=" * 60)
