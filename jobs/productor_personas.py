#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Productor de Kafka: genera personas y las envia a 3 TOPICS diferentes.

Topics:
  - personas:  datos personales  (id, nombre, apellido, edad, genero, ciudad, estado)
  - empleos:   datos laborales   (id, ocupacion, nivel_estudios, ingreso_mensual)
  - actividad: estado y registro (id, activo, fecha_registro)

Modos de ejecucion:
  - Batch (default):     genera TOTAL registros lo mas rapido posible
  - Streaming (DELAY>0): genera registros con pausa entre cada uno

Variables de entorno:
  KAFKA  = brokers de Kafka (default: 172.30.0.10:9092)
  TOTAL  = registros a generar (default: 100000)
  DELAY  = segundos entre registros, 0=batch (default: 0)
"""
import json, os, time, random, sys
from datetime import date, timedelta
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Catalogos base (datos realistas de Mexico)
# ---------------------------------------------------------------------------
NOMBRES_M = ["Carlos", "Luis", "Jose", "Juan", "Miguel", "Jorge", "Diego",
             "Fernando", "Ricardo", "Eduardo", "Alejandro", "Roberto",
             "Francisco", "Manuel", "Sergio", "Axel", "Emiliano", "Mateo"]
NOMBRES_F = ["Ana", "Maria", "Laura", "Sofia", "Lucia", "Fernanda", "Daniela",
             "Valeria", "Andrea", "Gabriela", "Patricia", "Rosa", "Carmen",
             "Alejandra", "Veronica", "Ximena", "Regina", "Camila"]
APELLIDOS = ["Lopez", "Garcia", "Martinez", "Hernandez", "Gonzalez", "Perez",
             "Rodriguez", "Sanchez", "Ramirez", "Cruz", "Flores", "Gomez",
             "Diaz", "Reyes", "Morales", "Jimenez", "Torres", "Vazquez",
             "Castillo", "Mendoza", "Ortiz", "Romero", "Alvarez", "Ruiz"]
CIUDAD_ESTADO = [
    ("Aguascalientes", "Aguascalientes"), ("Jesus Maria", "Aguascalientes"),
    ("Guadalajara", "Jalisco"), ("Zapopan", "Jalisco"),
    ("Monterrey", "Nuevo Leon"), ("San Nicolas", "Nuevo Leon"),
    ("Ciudad de Mexico", "CDMX"), ("Puebla", "Puebla"),
    ("Queretaro", "Queretaro"), ("Leon", "Guanajuato"),
    ("Merida", "Yucatan"), ("Tijuana", "Baja California"),
    ("Cancun", "Quintana Roo"), ("Toluca", "Estado de Mexico"),
    ("Morelia", "Michoacan"), ("San Luis Potosi", "San Luis Potosi"),
]
OCUPACIONES = ["Estudiante", "Ingeniero", "Doctor", "Maestro", "Abogado",
               "Contador", "Comerciante", "Empleado", "Disenador",
               "Programador", "Enfermero", "Arquitecto", "Mecanico",
               "Chef", "Electricista", "Administrador"]
NIVELES = ["Primaria", "Secundaria", "Preparatoria", "Licenciatura",
           "Maestria", "Doctorado"]


def fecha_aleatoria():
    inicio = date(2024, 1, 1)
    fin = date(2026, 6, 1)
    dias = (fin - inicio).days
    return (inicio + timedelta(days=random.randint(0, dias))).isoformat()


def generar_persona(i):
    genero = random.choice(["M", "F"])
    nombre = random.choice(NOMBRES_F if genero == "F" else NOMBRES_M)
    ciudad, estado = random.choice(CIUDAD_ESTADO)
    return {
        "id_persona": i,
        "nombre": nombre,
        "apellido": random.choice(APELLIDOS),
        "edad": random.randint(18, 80),
        "genero": genero,
        "ciudad": ciudad,
        "estado": estado,
        "ocupacion": random.choice(OCUPACIONES),
        "nivel_estudios": random.choice(NIVELES),
        "ingreso_mensual": round(random.uniform(3000, 80000), 2),
        "activo": random.choice([True, False]),
        "fecha_registro": fecha_aleatoria(),
    }


# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
KAFKA = os.environ.get("KAFKA", "172.30.0.10:9092")
TOTAL = int(os.environ.get("TOTAL", "100000"))
DELAY = float(os.environ.get("DELAY", "0"))

TOPICS = ["personas", "empleos", "actividad"]

# Persistencia de estado
OUTDIR = "/data"
ESTADO_FILE = os.path.join(OUTDIR, "productor_ultimo_id.txt")

ultimo_id = 0
if os.path.exists(ESTADO_FILE):
    try:
        with open(ESTADO_FILE, "r") as f:
            ultimo_id = int(f.read().strip())
    except Exception as e:
        print(f"No se pudo leer el estado del productor: {e}")

print("=" * 60)
print("  PRODUCTOR DE KAFKA - 3 TOPICS (CON PERSISTENCIA)")
print("=" * 60)
print(f"  Kafka:    {KAFKA}")
print(f"  Topics:   {', '.join(TOPICS)}")
print(f"  Total:    {TOTAL:,} registros")
print(f"  Modo:     {'Streaming (delay=' + str(DELAY) + 's)' if DELAY > 0 else 'Batch (rapido)'}")
if ultimo_id > 0:
    print(f"  Estado:   Resumiendo desde registro {ultimo_id + 1:,}")
else:
    print(f"  Estado:   Iniciando desde el registro 1")
print("=" * 60)

random.seed(42)

prod = KafkaProducer(
    bootstrap_servers=KAFKA,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

t0 = time.time()
i = ultimo_id
try:
    for i in range(ultimo_id + 1, TOTAL + 1):
        p = generar_persona(i)

        # --- Topic 1: PERSONAS (datos personales) ---
        msg_persona = {
            "id_persona": p["id_persona"],
            "nombre":     p["nombre"],
            "apellido":   p["apellido"],
            "edad":       p["edad"],
            "genero":     p["genero"],
            "ciudad":     p["ciudad"],
            "estado":     p["estado"],
        }
        prod.send("personas", msg_persona)

        # --- Topic 2: EMPLEOS (datos laborales) ---
        msg_empleo = {
            "id_persona":      p["id_persona"],
            "ocupacion":       p["ocupacion"],
            "nivel_estudios":  p["nivel_estudios"],
            "ingreso_mensual": p["ingreso_mensual"],
        }
        prod.send("empleos", msg_empleo)

        # --- Topic 3: ACTIVIDAD (estado y registro) ---
        msg_actividad = {
            "id_persona":     p["id_persona"],
            "activo":         p["activo"],
            "fecha_registro": p["fecha_registro"],
        }
        prod.send("actividad", msg_actividad)

        # Progreso y guardado de estado periódico
        if i % 5000 == 0 or i == TOTAL:
            elapsed = time.time() - t0
            rate = (i - ultimo_id) / elapsed if elapsed > 0 else 0
            print(f"  [{i:>7,}/{TOTAL:,}]  {i*100//TOTAL:>3}%  "
                  f"({rate:,.0f} reg/s)  topics: personas, empleos, actividad")
            try:
                with open(ESTADO_FILE, "w") as f:
                    f.write(str(i))
            except:
                pass

        if DELAY > 0:
            time.sleep(DELAY)

    prod.flush()
    elapsed = time.time() - t0
    print("\n" + "=" * 60)
    print(f"  COMPLETADO: {TOTAL:,} registros enviados")
    print(f"  Mensajes totales: {TOTAL * 3:,} (3 por registro)")
    print(f"  Tiempo: {elapsed:.1f}s  ({(TOTAL - ultimo_id)/elapsed:,.0f} reg/s)")
    print("=" * 60)

    # Limpiar estado si se completó con éxito
    if os.path.exists(ESTADO_FILE):
        try:
            os.remove(ESTADO_FILE)
        except:
            pass

except KeyboardInterrupt:
    prod.flush()
    print(f"\nInterrumpido manualmente. Último registro enviado: {i:,}")
    try:
        with open(ESTADO_FILE, "w") as f:
            f.write(str(i))
    except:
        pass

except Exception as e:
    prod.flush()
    print(f"\nERROR: Falló el envío en el registro {i}. Detalle: {e}")
    try:
        with open(ESTADO_FILE, "w") as f:
            f.write(str(i - 1))
    except:
        pass