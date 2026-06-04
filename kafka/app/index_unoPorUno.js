const fs = require('fs');
const { Kafka, Partitioners } = require('kafkajs');

// Configuración del clúster con las IPs de los 3 equipos físicos
const kafka = new Kafka({
  clientId: 'proyecto-distribuido',
  // Actualiza con las IPs reales
  brokers: ['10.13.140.92:9092', '10.13.140.189:9092'],
  retry: {
    initialRetryTime: 1000,
    retries: 10
  }
});
const admin = kafka.admin();
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'grupo-consumidores-1' });

const topicos = ['ventas', 'usuarios', 'logs', 'transacciones', 'alertas'];

async function run() {
  try {
    // ==========================================
    // 1. CREACIÓN DE TÓPICOS Y PARTICIONES
    // ==========================================
    console.log('Conectando Admin para verificar/crear tópicos...');
    await admin.connect();
    
    const topicsToCreate = topicos.map(t => ({
      topic: t,
      numPartitions: 3,     // 3 particiones para distribuir la carga
      replicationFactor: 2  // Factor 3 para replicar en tu nodo, el de Eduardo y el de Darely
    }));

    // 1. Quitamos la directiva 'waitForLeaders: true'
    await admin.createTopics({ topics: topicsToCreate });
    console.log('Comando de creación enviado a Kafka...');
    
    // 2. Agregamos una pausa manual de 5 segundos para que la red sincronice las particiones
    console.log('Esperando 5 segundos para que los nodos asignen líderes...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    console.log('Tópicos listos con sus particiones y replicación.');
    await admin.disconnect();

    // ==========================================
    // 2. LEVANTAR EL CONSUMIDOR
    // ==========================================
    await consumer.connect();
    
    // Suscribimos el consumidor a los 5 tópicos
    for (const t of topicos) {
      await consumer.subscribe({ topic: t, fromBeginning: false });
    }

    // El consumidor se queda escuchando en segundo plano
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        // Imprimimos solo una fracción de los mensajes para no saturar tu terminal
        if (Math.random() < 0.0001) { 
            console.log(`[CONSUMIDOR] Recibido en ${topic} (Part. ${partition}): ${message.value.toString().substring(0, 60)}...`);
        }
      },
    });

    // ==========================================
    // 3. LEER EL ARCHIVO JSON Y PRODUCIR MENSAJES
    // ==========================================
    await producer.connect();
    
    console.log('Leyendo el archivo data_masiva.json de 100,000 registros...');
    const rawData = fs.readFileSync('./data_masiva.json', 'utf-8');
    const datosMasivos = JSON.parse(rawData);

    console.log(`Iniciando el envío distribuido de ${datosMasivos.length} registros...`);
    
    for (let i = 0; i < datosMasivos.length; i++) {
      const registro = datosMasivos[i];
      
      // Elegimos un tópico aleatorio para distribuir los datos
      const topicDestino = topicos[Math.floor(Math.random() * topicos.length)];

      await producer.send({
        topic: topicDestino,
        messages: [
          { 
            // Usamos el id_persona como llave para asegurar el orden en la partición
            key: `key-${registro.id_persona}`, 
            value: JSON.stringify(registro) 
          }
        ],
      });

      // Log de progreso cada 10,000 mensajes
      if ((i + 1) % 10000 === 0) {
        console.log(`[PRODUCTOR] Se han enviado ${i + 1} mensajes al clúster...`);
      }
    }

    console.log('¡Lectura y envío finalizado! Todos los datos de prueba están en el clúster.');

  } catch (error) {
    console.error('Error crítico en el clúster:', error);
  }
}

run();