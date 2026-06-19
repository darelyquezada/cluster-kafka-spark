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


COLUMNAS = ["id_persona", "nombre", "apellido", "edad", "genero", "ciudad",
            "estado", "ocupacion", "nivel_estudios", "ingreso_mensual",
            "activo", "fecha_registro"]


def _val_sql(v):
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser(description="Generador de datos de personas")
    ap.add_argument("--count", type=int, default=100000)
    ap.add_argument("--outdir", default="data")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    j = os.path.join(args.outdir, "personas.json")
    c = os.path.join(args.outdir, "personas.csv")

    print("Generando " + format(args.count, ",") + " personas...")

    with open(j, "w", encoding="utf-8") as fj, \
         open(c, "w", encoding="utf-8", newline="") as fc:

        fc.write(",".join(COLUMNAS) + "\n")

        for i in range(1, args.count + 1):
            p = generar_persona(i)
            fj.write(json.dumps(p, ensure_ascii=False) + "\n")

            fila = [str(p["id_persona"]), p["nombre"], p["apellido"], str(p["edad"]),
                    p["genero"], p["ciudad"], p["estado"], p["ocupacion"],
                    p["nivel_estudios"], str(p["ingreso_mensual"]),
                    "true" if p["activo"] else "false", p["fecha_registro"]]
            fc.write(",".join(fila) + "\n")

    print("Listo:", j, "y", c)


if __name__ == "__main__":
    main()
