#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor de Kafka para el Worker.
Muestra en tiempo real los registros que llegan a los topics de Kafka.

Uso desde la carpeta worker2/:
    docker exec -it kafka python3 -c "
        import subprocess; subprocess.run(['pip', 'install', 'kafka-python-ng', '-q'])
    " 2>/dev/null
    docker exec -it spark-worker-2 bash -c \
        "pip install kafka-python-ng -q && python3 /data/ver_kafka.py"

O simplemente ejecuta:
    ver_kafka.bat
"""
import json, os, sys, signal
from kafka import KafkaConsumer

KAFKA = os.environ.get("KAFKA", "100.83.126.17:9092,100.116.140.81:9092,100.126.67.90:9092")
TOPICS = ["personas", "empleos", "actividad"]
MAX_MSGS = int(os.environ.get("MAX", "50"))   # default: 50 mensajes para no saturar
GROUP_ID = os.environ.get("GROUP_ID", "monitor-worker-" + os.environ.get("HOSTNAME", "worker"))
DELAY    = float(os.environ.get("DELAY", "0"))  # pausa entre mensajes en segundos

# Colores ANSI para la terminal
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
RED    = "\033[91m"

COLORES_TOPIC = {
    "personas":  "\033[96m",  # cyan
    "empleos":   "\033[93m",  # amarillo
    "actividad": "\033[92m",  # verde
}

print(BOLD + "=" * 65 + RESET)
print(BOLD + "  MONITOR KAFKA - VER REGISTROS EN TIEMPO REAL" + RESET)
print(BOLD + "=" * 65 + RESET)
print(f"  Brokers: {KAFKA}")
print(f"  Topics:  {', '.join(TOPICS)}")
print(f"  Grupo:   {GROUP_ID}")
print(f"  Limite:  {'sin limite' if MAX_MSGS == 0 else str(MAX_MSGS) + ' mensajes'}")
print(f"  Modo:    desde el PRINCIPIO (earliest) - muestra mensajes ya guardados")
print(BOLD + "=" * 65 + RESET)
print(f"  {CYAN}[personas]{RESET}   {YELLOW}[empleos]{RESET}   {GREEN}[actividad]{RESET}")
print(BOLD + "-" * 65 + RESET)
print()

try:
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA,
        auto_offset_reset="earliest",     # desde el principio: muestra todo lo que hay en Kafka
        enable_auto_commit=False,         # no marcar como leído (no interfiere con otros consumidores)
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10000,        # si no hay mensajes en 10s, termina
    )
except Exception as e:
    print(f"{RED}ERROR al conectar a Kafka: {e}{RESET}")
    print("Verifica que los brokers estén activos y accesibles.")
    sys.exit(1)

total = 0
running = True

def salir(sig, frame):
    global running
    running = False
    print(f"\n{BOLD}--- Monitor detenido. Total mensajes vistos: {total:,} ---{RESET}")
    sys.exit(0)

signal.signal(signal.SIGINT, salir)
signal.signal(signal.SIGTERM, salir)

print(f"  Leyendo mensajes desde el inicio... (Ctrl+C para salir)\n")
import time

for msg in consumer:
    topic  = msg.topic
    valor  = msg.value
    particion = msg.partition
    offset    = msg.offset
    color  = COLORES_TOPIC.get(topic, RESET)

    # Formatear el mensaje de forma compacta y legible
    id_p = valor.get("id_persona", "?")

    if topic == "personas":
        nombre   = valor.get("nombre", "")
        apellido = valor.get("apellido", "")
        ciudad   = valor.get("ciudad", "")
        estado   = valor.get("estado", "")
        edad     = valor.get("edad", "")
        linea = f"id={id_p:<6} | {nombre} {apellido:<12} | {edad} años | {ciudad}, {estado}"

    elif topic == "empleos":
        ocup     = valor.get("ocupacion", "")
        nivel    = valor.get("nivel_estudios", "")
        ingreso  = valor.get("ingreso_mensual", 0)
        linea = f"id={id_p:<6} | {ocup:<14} | {nivel:<12} | ${ingreso:,.2f}"

    elif topic == "actividad":
        activo = "✔ Activo  " if valor.get("activo") else "✘ Inactivo"
        fecha  = valor.get("fecha_registro", "")
        linea = f"id={id_p:<6} | {activo} | Registro: {fecha}"

    else:
        linea = json.dumps(valor, ensure_ascii=False)

    print(f"  {color}[{topic:<9}]{RESET} p{particion}/o{offset:<6} {linea}")

    total += 1
    if DELAY > 0:
        time.sleep(DELAY)
    if MAX_MSGS > 0 and total >= MAX_MSGS:
        print(f"\n{BOLD}Límite de {MAX_MSGS} mensajes alcanzado. Usa MAX=0 para ver todos.{RESET}")
        break

consumer.close()
print(f"\n{BOLD}Total mensajes vistos: {total:,}{RESET}")
