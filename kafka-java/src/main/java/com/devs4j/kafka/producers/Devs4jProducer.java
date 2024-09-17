package com.devs4j.kafka.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Devs4jProducer {
	
	// Habilitamos el logger en esta clase
	private static Logger log = LoggerFactory.getLogger(Devs4jProducer.class);
	
	public static void main(String[] args) {
		// Creamos esta variable para medir el tiempo de ejecución total en milisegundos
		long startTime = System.currentTimeMillis();
		// Propiedades para configurar el productor
		Properties props = new Properties();
		// Broker de Kafka al que nos vamos a conectar para enviar mensajes
		props.put("bootstrap.servers","localhost:9092");
		// Si el valor es "0", indica que no necesitamos recibir ningún ack
		// Si el valor es "1", indica que un broker del cluster de Kafka nos tiene que responder con un ack
		// Si el valor es "all", todos los brokers del cluster de Kafka nos tieene que responder con un ack
		props.put("acks","1");
		// Indicamos que la clave, o key, del mensaje se serializa y se envía como un String
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		// Indicamos que el valor del mensaje se serializa y se envía como un String
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		// Podemos establecer las propiedades "linger.ms", "batch.size" y "buffer.memory" para mejorar el rendimiento a la hora de enviar mensajes a Kafka
		
		
		// Creamos un productor usando las propiedades anteriores
		// Indicamos el tipo del dato de la clave, o key, de los mensajes y el tipo de dato del valor de esos mensaje para la creación del productor
		// Creamos el productor dentro de un try para que automáticamente invoque al método "close" de dicho productor cuando termine de ejecutarse el código indicado dentro de ese try
		// "try" automáticamente ejecuta el método "close" del productor porque "KafkaProducer" implementa la interfaz "Producer" que extiende de "Closeable", es decir, si una implementación extiende de "Closeable", podemos usar "try" de esta manera para que de forma automática ejecute el método "close" 
		// Si no usamos este "try", tenemos que invocar manualmente el método "close" del productor al finalizar
		try (Producer<String, String> producer = new KafkaProducer<String, String>(props)){
			// Enviamos 10000 mensajes al tópico "devs4j-topic" del broker del cluster de Kafka
			for(int i = 0;i < 100000; i++) {
				// Envía el mensaje "devs4j-value" con clave, o key, el índice del bucle "i" al tópico "devs4j-topic" del broker del cluster de Kafka
				// El tipo de dato "Future" que devuelve el método "send" indica que el envío es asíncrono
				producer.send(new ProducerRecord<String,String>("devs4j-topic",String.valueOf(i),"devs4j-value"));
			
				// Nota: El rendimiento es mucho mejor si se usa envíos asíncronos que síncronos ya que los envíos síncronos tardan mucho más tiempo
				// Usando el método "get" conseguimos que el envío sea síncrono
				//producer.send(new ProducerRecord<String,String>("devs4j-topic",String.valueOf(i),"devs4j-value")).get();
			}
			producer.flush();
		}
		// Sólo es necesario para envíos síncronos
		/*catch(InterruptedException | ExecutionException e) {
		 	// Escribimos esta traza en el log a modo de error con los datos de la excepción
			log.error("Message producer interrupted", e);
		}*/
		
		// Escribimos una traza en log a modo de información con el tiempo total de ejecución de esté metodo main en milisegundos
		log.info("Processing time - {} ms", (System.currentTimeMillis() - startTime));
	}

}
