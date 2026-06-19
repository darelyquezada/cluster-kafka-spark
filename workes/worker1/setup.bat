@echo off
REM ===========================================================
REM  Setup rapido del Worker en la maquina remota.
REM  Ejecuta este script DESPUES de copiar la carpeta worker/
REM  y de haber generado los datos.
REM ===========================================================
echo.
echo === Spark Worker - Setup ===
echo.

REM 1. Verificar que Docker esta instalado
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker no esta instalado. Instalalo primero.
    exit /b 1
)

REM 2. Verificar que existen los datos
if not exist "data\personas.json" (
    echo.
    echo Los datos no existen. Generandolos ahora...
    echo Necesitas Python 3 instalado.
    echo.
    mkdir data 2>nul
    python generar_personas.py --outdir data
    if errorlevel 1 (
        echo ERROR: No se pudieron generar los datos.
        echo Copia manualmente la carpeta data/ del master, o instala Python 3.
        exit /b 1
    )
)

echo.
echo Datos OK: data/personas.json encontrado.
echo.

REM 3. Levantar el worker
echo Levantando el worker...
docker compose up -d

echo.
echo === Worker levantado ===
echo Revisa en el master (http://localhost:8080) que el worker aparezca.
echo.
pause
