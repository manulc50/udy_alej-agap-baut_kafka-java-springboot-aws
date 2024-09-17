package com.devs4j.kafka.multithread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Devs4jMultithreadConsumer {
	
	private final static int NUM_THREADS = 5;
	
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
	
		// Creamos un pool de NUM_THREADS hilos
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
		
		// Bucle para crear NUM_THREADS de hilos que ejecuten consumidores de Kafka
		for(int i = 0; i < NUM_THREADS; i++) {
			// Creamos un consumidor de Kafka a partir de las propiedades anteriores
			// La clase "Devs4jThreadConsumer" es un consumidor que extiende de Thread para que pueda ejecutarse en un hilo
			Devs4jThreadConsumer consumer = new Devs4jThreadConsumer(new KafkaConsumer<String, String>(props));
			// Ejecutamos un hilo con el consumidor
			executor.execute(consumer);
		}
		
		while(!executor.isTerminated());
	}

}
