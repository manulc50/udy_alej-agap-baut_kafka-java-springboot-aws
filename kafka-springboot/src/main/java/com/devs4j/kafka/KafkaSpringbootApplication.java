package com.devs4j.kafka;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.concurrent.ListenableFuture;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;


@SpringBootApplication
public class KafkaSpringbootApplication /*implements CommandLineRunner*/{
	
	// Habilitamos el logger en esta clase
	private static Logger log = LoggerFactory.getLogger(KafkaSpringbootApplication.class);
	
	// Inyectamos del contenedor, o memoria, de Spring el bean de tipo "KafkaTemplate" que se corresponde con una plantilla de Kafka que nos permite producir, o enviar, mensajes a tópicos de brokers de Kafka 
	@Autowired
	private KafkaTemplate<String, String > kafkaTemplate;
	
	// Inyectamos del contenedor, o memoria, de Spring el bean de tipo "KafkaListenerEndpointRegistry" que nos va a permitir iniciar y/o finalizar el consumo de mensajes de forma manual desde cualquier parte de esta clase
	/*@Autowired
	private KafkaListenerEndpointRegistry registry;*/
	
	// Inyectamos del contenedor, o memorio, de Spring el bean con la implementación de MeterRegistry. En este caso, la implementación la lleva a cabo PrometheusMeterRegistry 
	@Autowired
	private MeterRegistry meterRegistry;

	
	// Método para leer, o consumir, mensajes de Kafka
	// @KafkaListener es una anotación necesaria para leer, o consumir, mensajes de Kafka
	// Esta anotación recibe 2 parámetros o atributos obligatorios; el nombre del tópico o tópicos de Kafka que tiene que escuchar para leer o recibir los mensajes, y el identificar del grupo de consumo o consumer group
	// El atributo "autoStartup" de esta anotación puesto a false(Por defecto, es true) hace que no se consuma mensajes de manera automática. Esto es útil cuando se desea iniciar o finalizar el consumo de mensajes de manera manual en cualquier parte del código
	// El atributo "id" es el identificador del consumidor y nos permite poder obtenerlo y manejarlo(Por ejemplo, para iniciar o finalizar el consumo de mensajes) desde cualquier parte de esta clase a través de la propiedad "registry" de tipo KafkaListenerEndpointRegistry
	// Cuando en la clase de configuraciones de Kafka, se crea un Listener Container Factory que permite Batch Listener(poder leer más de un registro al mismo tiempo), tenemos que indicar en esta anotación quién es ese Listener Container Factory en el atributo "containerFactory" 
	// Cuando el Listener Container Factory tiene habilitado el Batch Listener, podemos establecer las siguientes propiedades:
	// - max.poll.interval.ms: Define el tiempo entre una ejecución y otra para el método pool
	// - max.poll.records: Define el máximo número de registros a devolver por el método pool
	// Si no habilitamos Batch Listener en el Listener Container Factory, este método sólo recibe un mensaje al mismo tiempo(String message)
	// Si habilitamos Batch Listener en el Listener Container Factory, este método recibe más de un mensaje al mismo tiempo(List<String> messages)
	// Si deseamos mostrar más información sobre los mensajes, como por ejemplo, su offset, su clave, o key, etc...), y no sólo su texto, tenemos que pasar a este método una lista de tipo ConsumerRecord<String, String> en lugar de una lista de tipo String
	@KafkaListener(id ="devs4j", autoStartup ="true", topics = "devs4j-topic", containerFactory = "listenerContainerFactory", groupId = "devs4j-group", properties = {"max.poll.interval.ms:4000", "max.poll.records:50"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		// Muestra en el log a modo de información(escribe en consola) cada mensaje recibido en este método
		// Sin Batch Listener
		//log.info("Message received {}", message);
		// Con Batch Listener
		/*log.info("Star reading messages");
		for(ConsumerRecord<String, String> message: messages)
			log.info("Partition = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(), message.value());
		log.info("Batch complete");*/
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringbootApplication.class, args);
	}
	
	// Con esta anotación, programamos la ejecución de este método cada 2000 ms(2 seg) de tiempo comenzando la primera ejecución después de pasar 100 ms desde el arranque de la aplicación Spring Boot
	@Scheduled(fixedRate = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		// Para probar el Listener Container Factory con Batch Listener habilitado, enviamos 200 mensajes a Kafka que el consumidor recibirá en bloques de 50 en 50("max.poll.records:50") cada 4 seg("max.poll.interval.ms:4000")
		for(int i = 0; i < 200; i++)
			// String.valueOf(i) se corresponde con la clave, o key, de cada mensaje
			kafkaTemplate.send("devs4j-topic",String.valueOf(i),String.format("Sample message %d", i));
	}
	
	// Con esta anotación, programamos la ejecución de este método cada 2000 ms(2 seg) de tiempo comenzando la primera ejecución después de pasar 500 ms desde el arranque de la aplicación Spring Boot
	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void messageCountMetric() {
		/*
		// PARA OBTENER UNA lISTA CON LAS MÉTRICAS DISPONIBLES
		List<Meter> meters = meterRegistry.getMeters();
		// Mostramos los nombres de esas métricas en el log a modo de información
		for(Meter meter: meters) {
			log.info("Metric {} ",meter.getId().getName());
		}*/

		// Obtenemos la métrica "kafka.producer.record.send.total" para saber el número total de mensajes que el productor ha enviado a Kafka en intervalos de 2 seg
		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {} ",count);
	}

	/*@Override
	public void run(String... args) throws Exception {*/
		/*
		// EJEMPLO DE ENVÍOS ASÍNCRONOS CON Y SIN CALLBACKS
		// Envía de forma asíncrona el mensaje de ejemplo "Sample message" al tópico "devs4j-topic" de los brokers de Kafka
		// Para trabajar sin establecer callbacks asíncronos cuando el productor envía un mensaje
		//kafkaTemplate.send("devs4j-topic","Sample message");
		// Para trabajar con callbacks asíncronos cuando el productor envía un mensaje
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("devs4j-topic","Sample message");
		// En este caso, establecemos un Callback de tipo KafkaSendCallback e implementamos sus métodos ya que es una interfaz
		future.addCallback(new KafkaSendCallback<String, String>() {

			// Si el envío se realizó correctamente, se ejecuta este método
			// Muestra por el log a modo de información un mensaje de éxito junto con el offset del mensaje enviado
			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message sent " + result.getRecordMetadata().offset());
				
			}

			// Si el envío no se realizó correctamente y se produjo una excepción de tipo Throwable, se ejecuta este método
			// Muestra por el log a modo de error un mensaje de error junto con los datos de la excepción ocurrida
			@Override
			public void onFailure(Throwable ex) {
				log.error("Error sending message", ex);
				
			}

			// Si el envío no se realizó correctamente y se produjo una excepción de tipo KafkaProducerException, se ejecuta este método
			// Muestra por el log a modo de error un mensaje de error junto con los datos de la excepción ocurrida
			@Override
			public void onFailure(KafkaProducerException ex) {
				log.error("Error sending message", ex);
				
			}
		});*/
		
		/*
		// EJEMPLO DE ENVÍO SÍNCRONO
		// Usando el método "get" del método "send", el envío se realiza de manera síncrona
		// De manera opcional, podemos pasar al método "get" un tiempo(En este caso 100 ms) que indica el tiempo máximo disponible para realizar el envío del mensaje
		// Si el envío no se produce durante ese tiempo establecido, se produce una excepción
		//kafkaTemplate.send("devs4j-topic","Sample message").get(100, TimeUnit.MILLISECONDS);
	
		// Para probar el Listener Container Factory con Batch Listener habilitado, enviamos 100 mensajes a Kafka que el consumir recibirá en bloques de 10 en 10("max.poll.records:10") cada 4 seg("max.poll.interval.ms:4000")
		for(int i = 0; i < 100; i++)
			// String.valueOf(i) se corresponde con la clave, o key, de cada mensaje
			kafkaTemplate.send("devs4j-topic",String.valueOf(i),String.format("Sample message %d", i));
		*/
		/*
		// EJEMPLO DE TRATAMIENTO MANUAL DEL INICIO Y FINALIZACIÓN DEL CONSUMO DE MENSAJES
		log.info("Waiting to start");
		// Interrumpimos la ejecución de la aplicación durante 5 seg justo después de enviar 100 mensajes a Kafka
		Thread.sleep(5000);
		log.info("Starting");
		// Iniciamos el consumo de mensajes en el consumidor con identificador "devs4j"
		registry.getListenerContainer("devs4j").start();
		// Volvemos a interrumpir la ejecución de la aplicación durante 5 seg para, a continuación, finalizar el consumo de mensajes
		Thread.sleep(5000);
		// Finalizamos el consumo de mensajes en el consumidor con identificador "devs4j"
		registry.getListenerContainer("devs4j").stop();*/
	//}

}
