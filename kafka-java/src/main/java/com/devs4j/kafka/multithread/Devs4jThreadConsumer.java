package com.devs4j.kafka.multithread;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Clase para crear un consumidor de Kafka multihilo

public class Devs4jThreadConsumer extends Thread{
	
	// Habilitamos el logger en esta clase
	private static Logger log = LoggerFactory.getLogger(Devs4jThreadConsumer.class);
	
	private final KafkaConsumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false); // Propiedad que indica si el consumidor está activo o apagado
	
	public Devs4jThreadConsumer(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void run() {
		// El consumidor se suscribe a los tópicos cuyos mensajes quiere consumir. En este array se especifican los nombres de los tópicos
		// En nuestro caso, nuestro consumidor sólo quiere consumir mensajes del tópico "devs4j-topic"
		consumer.subscribe(Arrays.asList("devs4j-topic"));
		
		try {
			// Bucle infinito para que el consumidor se quede consumiendo los mensajes que vayan llegando hasta que se apague
			ConsumerRecords<String, String> consumerRecords = null;
			while(!closed.get()) {
				// El consumidor va a consumir mensajes cada 100 ms y esos mensajes consumidos cada 100 ms se almacenan en la variable "consumerRecords"
				consumerRecords = consumer.poll(Duration.ofMillis(100));
				// Por cada mensaje de los mensajes recibidos cada 100 ms, escribimos en el log a modo de información el offset, la partición, la clave y el valor del mensaje
				for(ConsumerRecord<String, String> consumerRecord: consumerRecords) {
					// Como el log en modo info escribe las trazas en consola, este proceso es lento si este consumidor recibe una gran cantidad de mensajes
					// Por lo tanto, para hacer este consumidor más ágil, en modo información sólo escribimos en el log el offset, la partición, la clave y el valor de aquellos mensajes cuyas claves, o keys, sean módulo 10000(valor aleatorio seleccionado para un productor que envía 100000 mensajes al tópico del broker de Kafka)
					// En modo debug, como no escribe en consola, en este modo podemos escribir en el log el offset, la partición, la clave y el valor de todos los mensajes que reciba este consumidor
					log.debug("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
					if(Integer.parseInt(consumerRecord.key()) % 10000 == 0)
						// Escribimos en el log a modo de información el offset, la partición, la clave y el valor del mensaje
						log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
			
				}
			}
		}
		catch(WakeupException e) {
			// Propagamos la excepción en caso de que el consumidor esté activo
			if(!closed.get())
				throw e;
		}
		finally {
			// Cerramos el consumidor
			consumer.close();
		}
			
	}
	
	// Método para apagar el consumidor
	public void shutdown() {
		// Establecemos la propiedad "closed" a true para indicar que el consumidor está apagado
		closed.set(true);
		// Este método es thread-safe y es útil para abortar un poll largo, es decir, si el consumidor se encuentra consumiendo mensajes, este método nos va a servir para apagarlo o interrumpirlo
		// Si hay poll en ejecución y se invoca a este método, se produce una excepción de tipo WakeupException que es tratada en el método "run"
		consumer.wakeup();
	}

}
