const fs = require('fs');

const TOTAL_RECORDS = 100000;

// Abrimos los tres streams de escritura para no saturar la RAM
const jsonStream = fs.createWriteStream('./data_masiva.json');
const csvStream = fs.createWriteStream('./data_masiva.csv');
const sqlStream = fs.createWriteStream('./data_masiva.sql');

// Arrays de datos para generar combinaciones únicas
const nombres = ["Ana", "Carlos", "Luis", "Maria", "Jose", "Elena", "Erick", "Darely", "Eduardo"];
const apellidos = ["Lopez", "Garcia", "Martinez", "Hernandez", "Gomez", "Perez", "Alvarez", "Cruz"];
const ciudades = ["Aguascalientes", "CDMX", "Guadalajara", "Monterrey", "Merida"];
const ocupaciones = ["Estudiante", "Ingeniero", "Abogado", "Medico", "Diseñador"];
const niveles = ["Preparatoria", "Licenciatura", "Maestria", "Doctorado"];

// 1. Preparar Cabeceras
// CSV Header (12 columnas)
csvStream.write('id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,activo,fecha_registro\n');

// JSON inicio de arreglo
jsonStream.write('[\n');

// SQL creación de tabla (opcional pero le da presentación al proyecto)
sqlStream.write(`CREATE TABLE IF NOT EXISTS personas (
    id_persona INT PRIMARY KEY,
    nombre VARCHAR(50),
    apellido VARCHAR(50),
    edad INT,
    genero CHAR(1),
    ciudad VARCHAR(50),
    estado VARCHAR(50),
    ocupacion VARCHAR(50),
    nivel_estudios VARCHAR(50),
    ingreso_mensual DECIMAL(10,2),
    activo BOOLEAN,
    fecha_registro DATE
);\n\n`);

console.log('Generando 100,000 registros en JSON, CSV y SQL. Esto puede tardar unos segundos...');

// 2. Generar y escribir datos
for (let i = 1; i <= TOTAL_RECORDS; i++) {
    // Generación de datos aleatorios
    const nombre = nombres[Math.floor(Math.random() * nombres.length)];
    const apellido = apellidos[Math.floor(Math.random() * apellidos.length)];
    const edad = Math.floor(Math.random() * (60 - 18 + 1)) + 18;
    const genero = Math.random() > 0.5 ? "M" : "F";
    const ciudad = ciudades[Math.floor(Math.random() * ciudades.length)];
    const estado = "Aguascalientes";
    const ocupacion = ocupaciones[Math.floor(Math.random() * ocupaciones.length)];
    const nivel = niveles[Math.floor(Math.random() * niveles.length)];
    const ingreso = parseFloat((Math.random() * 20000 + 3000).toFixed(2));
    const activo = Math.random() > 0.2;
    // Generar una fecha aleatoria reciente
    const fecha = new Date(Date.now() - Math.floor(Math.random() * 10000000000)).toISOString().split('T')[0];

    // Formato JSON
    const registroObj = {
        id_persona: i, nombre, apellido, edad, genero, ciudad, estado, 
        ocupacion, nivel_estudios: nivel, ingreso_mensual: ingreso, activo, fecha_registro: fecha
    };
    const isLast = i === TOTAL_RECORDS;
    jsonStream.write(`  ${JSON.stringify(registroObj)}${isLast ? '\n' : ',\n'}`);

    // Formato CSV
    csvStream.write(`${i},${nombre},${apellido},${edad},${genero},${ciudad},${estado},${ocupacion},${nivel},${ingreso},${activo},${fecha}\n`);

    // Formato SQL (INSERT statement)
    sqlStream.write(`INSERT INTO personas VALUES (${i}, '${nombre}', '${apellido}', ${edad}, '${genero}', '${ciudad}', '${estado}', '${ocupacion}', '${nivel}', ${ingreso}, ${activo}, '${fecha}');\n`);
}

// 3. Cerrar flujos
jsonStream.write(']\n');
jsonStream.end();
csvStream.end();
sqlStream.end();

let terminados = 0;
const checarTermino = () => {
    terminados++;
    if (terminados === 3) {
        console.log('¡Éxito! Archivos data_masiva.json, data_masiva.csv y data_masiva.sql generados correctamente.');
    }
};

jsonStream.on('finish', checarTermino);
csvStream.on('finish', checarTermino);
sqlStream.on('finish', checarTermino);