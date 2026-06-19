#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de datos de personas para el cluster Spark.
Crea 100,000+ registros UNICOS en tres formatos: JSON (lines), CSV y SQL.

Es Python puro (no usa PySpark), asi que corre sin problema en Python 3.14.
Uso:
    python generar_personas.py                 # 100,000 registros en ../data
    python generar_personas.py --count 150000  # mas registros
    python generar_personas.py --outdir ../data --seed 42

Salidas (en --outdir):
    personas.json   -> JSON Lines: un objeto JSON por linea (formato que lee Spark)
    personas.csv    -> CSV con encabezado, 12 columnas
    personas.sql    -> CREATE TABLE + INSERTs por lotes
"""

import argparse
import csv
import json
import os
import random
from datetime import date, timedelta

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

# ciudad -> estado (consistentes)
CIUDAD_ESTADO = [
    ("Aguascalientes", "Aguascalientes"),
    ("Jesus Maria", "Aguascalientes"),
    ("Guadalajara", "Jalisco"),
    ("Zapopan", "Jalisco"),
    ("Monterrey", "Nuevo Leon"),
    ("San Nicolas", "Nuevo Leon"),
    ("Ciudad de Mexico", "CDMX"),
    ("Puebla", "Puebla"),
    ("Queretaro", "Queretaro"),
    ("Leon", "Guanajuato"),
    ("Merida", "Yucatan"),
    ("Tijuana", "Baja California"),
    ("Cancun", "Quintana Roo"),
    ("Toluca", "Estado de Mexico"),
    ("Morelia", "Michoacan"),
    ("San Luis Potosi", "San Luis Potosi"),
]

OCUPACIONES = ["Estudiante", "Ingeniero", "Doctor", "Maestro", "Abogado",
               "Contador", "Comerciante", "Empleado", "Disenador",
               "Programador", "Enfermero", "Arquitecto", "Mecanico",
               "Chef", "Electricista", "Administrador"]

NIVELES = ["Primaria", "Secundaria", "Preparatoria", "Licenciatura",
           "Maestria", "Doctorado"]


def fecha_aleatoria(inicio, fin):
    """Devuelve una fecha aleatoria ISO entre dos fechas."""
    dias = (fin - inicio).days
    return (inicio + timedelta(days=random.randint(0, dias))).isoformat()


def generar_persona(id_persona):
    genero = random.choice(["M", "F"])
    nombre = random.choice(NOMBRES_F if genero == "F" else NOMBRES_M)
    ciudad, estado = random.choice(CIUDAD_ESTADO)
    return {
        "id_persona": id_persona,
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
        "fecha_registro": fecha_aleatoria(date(2024, 1, 1), date(2026, 6, 1)),
    }


# orden fijo de columnas para CSV / SQL
COLUMNAS = ["id_persona", "nombre", "apellido", "edad", "genero", "ciudad",
            "estado", "ocupacion", "nivel_estudios", "ingreso_mensual",
            "activo", "fecha_registro"]


def escribir_json(personas, ruta):
    """JSON Lines: un objeto por linea (lo que lee spark.read.json)."""
    with open(ruta, "w", encoding="utf-8") as f:
        for p in personas:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")


def escribir_csv(personas, ruta):
    with open(ruta, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNAS)
        w.writeheader()
        for p in personas:
            fila = dict(p)
            # Spark infiere booleanos en minuscula (true/false), no True/False
            fila["activo"] = "true" if p["activo"] else "false"
            w.writerow(fila)


def _val_sql(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def escribir_sql(personas, ruta, lote=500):
    with open(ruta, "w", encoding="utf-8") as f:
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
        for i in range(0, len(personas), lote):
            grupo = personas[i:i + lote]
            f.write("INSERT INTO personas (" + ", ".join(COLUMNAS) + ") VALUES\n")
            filas = []
            for p in grupo:
                vals = ", ".join(_val_sql(p[c]) for c in COLUMNAS)
                filas.append("    (" + vals + ")")
            f.write(",\n".join(filas) + ";\n\n")


def main():
    ap = argparse.ArgumentParser(description="Generador de datos de personas")
    ap.add_argument("--count", type=int, default=100000,
                    help="numero de registros (default 100000)")
    ap.add_argument("--outdir", default=os.path.join("..", "data"),
                    help="carpeta de salida (default ../data)")
    ap.add_argument("--seed", type=int, default=42,
                    help="semilla aleatoria para reproducibilidad")
    args = ap.parse_args()

    random.seed(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    j = os.path.join(args.outdir, "personas.json")
    c = os.path.join(args.outdir, "personas.csv")
    s = os.path.join(args.outdir, "personas.sql")

    print("Generando " + format(args.count, ",") + " personas (streaming, poca RAM)...")

    lote_sql = 500
    buffer_sql = []
    with open(j, "w", encoding="utf-8") as fj, \
         open(c, "w", encoding="utf-8", newline="") as fc, \
         open(s, "w", encoding="utf-8") as fs:

        # encabezado CSV
        fc.write(",".join(COLUMNAS) + "\n")
        # encabezado SQL (CREATE TABLE)
        fs.write("CREATE TABLE IF NOT EXISTS personas (\n")
        fs.write("    id_persona      INT PRIMARY KEY,\n")
        fs.write("    nombre          VARCHAR(50),\n")
        fs.write("    apellido        VARCHAR(50),\n")
        fs.write("    edad            INT,\n")
        fs.write("    genero          CHAR(1),\n")
        fs.write("    ciudad          VARCHAR(50),\n")
        fs.write("    estado          VARCHAR(50),\n")
        fs.write("    ocupacion       VARCHAR(50),\n")
        fs.write("    nivel_estudios  VARCHAR(30),\n")
        fs.write("    ingreso_mensual DECIMAL(10,2),\n")
        fs.write("    activo          BOOLEAN,\n")
        fs.write("    fecha_registro  DATE\n")
        fs.write(");\n\n")

        for i in range(1, args.count + 1):
            p = generar_persona(i)

            # JSON Lines
            fj.write(json.dumps(p, ensure_ascii=False) + "\n")

            # CSV (booleano en minuscula para Spark)
            fila = [str(p["id_persona"]), p["nombre"], p["apellido"], str(p["edad"]),
                    p["genero"], p["ciudad"], p["estado"], p["ocupacion"],
                    p["nivel_estudios"], str(p["ingreso_mensual"]),
                    "true" if p["activo"] else "false", p["fecha_registro"]]
            fc.write(",".join(fila) + "\n")

            # SQL por lotes
            vals = ", ".join(_val_sql(p[col]) for col in COLUMNAS)
            buffer_sql.append("    (" + vals + ")")
            if len(buffer_sql) >= lote_sql:
                fs.write("INSERT INTO personas (" + ", ".join(COLUMNAS) + ") VALUES\n")
                fs.write(",\n".join(buffer_sql) + ";\n\n")
                buffer_sql = []

            if i % 500000 == 0:
                print("  ... " + format(i, ",") + " registros")

        if buffer_sql:
            fs.write("INSERT INTO personas (" + ", ".join(COLUMNAS) + ") VALUES\n")
            fs.write(",\n".join(buffer_sql) + ";\n\n")

    mb = lambda ruta: os.path.getsize(ruta) / 1048576.0
    print("\nListo. Archivos creados en:", os.path.abspath(args.outdir))
    print("  - personas.json  ({:.1f} MB)".format(mb(j)))
    print("  - personas.csv   ({:.1f} MB)".format(mb(c)))
    print("  - personas.sql   ({:.1f} MB)".format(mb(s)))


if __name__ == "__main__":
    main()

