package com.devs4j.kafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

// Clase con todas las configuraciones de Kafka

@Configuration // Anotamos esta clase de configuración como una clase de configuración de Spring
@EnableScheduling // Anotación necesaria para poder usar la anotación @Scheduled en esta aplicación Spring Boot, que es una anotación que nos permite programar tareas que se ejecuten cada cierto tiempo de forma automática
public class KafkaConfiguration {
	
	// Método que crea y devuelve un HashMap con las configuraciones del consumidor de Kafka
	private Map<String, Object> consumerProperties(){
		Map<String, Object> props = new HashMap<>();
		// Broker de Kafka al que nos vamos a conectar para consumir mensajes
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		// Identificador de nuestro grupo de consumo o consumer group
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"devs4j-group");
		// Indicamos que la clave, o key, del mensaje se deserializa y se consume como un String
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
		// Indicamos que el valor del mensaje se deserializa y se consume como un String
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
		return props;
	}
	
	// Método que crea y devuelve un HashMap con las configuraciones del productor de Kafka
	private Map<String, Object> producerProperties(){
		Map<String, Object> props = new HashMap<>();
		// Broker de Kafka al que nos vamos a conectar para enviar mensajes
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		// Indicamos que la clave, o key, del mensaje se serializa y se envía como un String
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
		// Indicamos que el valor del mensaje se serializa y se envía como un String
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
		return props;
	}
	
	// Método que crea y devuelve un KafkaTemplate, para el envío de mensajes a Kafka, a partir de un DefaultKafkaProducerFactory, como un bean de Spring
	// Tanto para la creación del KafkaTemplate como para la creación del DefaultKafkaProducerFactory, hay que indicar el tipo de la clave, o key, de los mensajes y el tipo de valor de los mensajes que se van a enviar a Kafka. En este caso, tanto la clave, o key, como el valor de los mensajes son de tipo String
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(){
		DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<String, String>(producerProperties());
		// Integramos nuestro Meter Registry, de tipo PrometheusMeterRegistry, a nuestro producer de Kafka mediante un listener que estará dando seguimiento a las métricas
		producerFactory.addListener(new MicrometerProducerListener<String, String>(meterRegistry()));
		KafkaTemplate<String, String> template = new KafkaTemplate<String, String>(producerFactory);
		return template;
	}
	
	// Método que devuelve la implementación de un ConsumerFactory como un bean de Spring
	// En este caso, la implementación del ConsumerFactory es de tipo DefaultKafkaConsumerFactory que se crea a partir de las configuraciones del condumidor de Kafka establecidas en el método anterior "consumerProperties"
	// Para la creación de un ConsumerFactory, es necesario indicar el tipo de la clave, o key, de los mensajes y el tipo del valor de los mensajes que se van a consumir. En este caso, tanto la clave, o key, como el valor de los mensaje son de tipo String
	@Bean
	public ConsumerFactory<String, String> consumerFactory(){
		return new DefaultKafkaConsumerFactory<String, String>(consumerProperties());
	}
	
	// Método que crea y devuelve un listener, para el consumo de mensajes desde Kafka, de tipo ConcurrentKafkaListenerContainerFactory como un bean de Spring
	// Para la creación de este listener, establecemos el ConsumerFactory creado en el método anterior "consumerFactory"
	// Para la creación de este listener, es necesario indicar el tipo de la clave, o key, de los mensajes y el tipo del valor de los mensajes que se van a consumir. En este caso, tanto la clave, o key, como el valor de los mensaje son de tipo String
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory(){
		ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		listenerContainerFactory.setConsumerFactory(consumerFactory());
		listenerContainerFactory.setBatchListener(true); // Para permitir leer más de un registro al mismo tiempo
		listenerContainerFactory.setConcurrency(3); // Consumo concurrente con 3 hilos
		return listenerContainerFactory;
	}
	
	// Método que crea y devuelve un objeto de tipo PrometheusMeterRegistry(extiende de la clase abstracta MeterRegistry) como un bean de Spring
	// Para la creación de este objeto, usamos la configuración por defecto de Prometheus
	@Bean
	public MeterRegistry meterRegistry() {
		PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
		return meterRegistry;
	}

}
