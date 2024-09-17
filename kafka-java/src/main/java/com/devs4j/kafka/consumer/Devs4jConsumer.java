package com.devs4j.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Devs4jConsumer {
	
	// Habilitamos el logger en esta clase
	private static Logger log = LoggerFactory.getLogger(Devs4jConsumer.class);
	
	public static void main(String[] args) {
		
		// Propiedades para configurar el consumidor
		Properties props=new Properties();
		// Broker de Kafka al que nos vamos a conectar para consumir mensajes
		props.setProperty("bootstrap.servers","localhost:9092");
		// Identificador de nuestro grupo de consumo o consumer group
		props.setProperty("group.id","devs4j-group");
		// Para que de forma automática se haga commit a los registros que se van leyendo
		props.setProperty("enable.auto.commit","true");
		// Tiempo en milisegundos para realizar cada commit
		props.setProperty("auto.commit.interval.ms","1000");
		// Indicamos que la clave, o key, del mensaje se deserializa y se consume como un String
		props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		// Indicamos que el valor del mensaje se deserializa y se consume como un String
		props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
	
		// Creamos un consumidor usando las propiedades anteriores
		// Indicamos el tipo del dato de la clave, o key, de los mensajes y el tipo de dato del valor de esos mensajes para la creación del consumidor
		// Creamos el consumidor dentro de un try para que automáticamente invoque al método "close" de dicho productor cuando termine de ejecutarse el código indicado dentro de ese try
		// "try" automáticamente ejecuta el método "close" del consumidor porque "KafkaConsumer" implementa la interfaz "Consumer" que extiende de "Closeable", es decir, si una implementación extiende de "Closeable", podemos usar "try" de esta manera para que de forma automática ejecute el método "close" 
		// Si no usamos este "try", tenemos que invocar manualmente el método "close" del consumidor al finalizar
		try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)){
			// El consumidor se suscribe a los tópicos cuyos mensajes quiere consumir. En este array se especifican los nombres de los tópicos
			// En nuestro caso, nuestro consumidor sólo quiere consumir mensajes del tópico "devs4j-topic"
			consumer.subscribe(Arrays.asList("devs4j-topic"));
			// Bucle infinito para que el consumidor se quede consumiendo los mensajes que vayan llegando a los tópicos que se ha suscrito
			ConsumerRecords<String, String> consumerRecords = null;
			while(true) {
				// El consumidor va a consumir mensajes cada 100 ms y esos mensajes consumidos cada 100 ms se almacenan en la variable "consumerRecords"
				consumerRecords = consumer.poll(Duration.ofMillis(100));
				// Por cada mensaje de los mensajes recibidos cada 100 ms, hacemos lo siguiente: Escribimos en el log a modo de información el offset, la partición, la clave y el valor del mensaje
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
	}
}
