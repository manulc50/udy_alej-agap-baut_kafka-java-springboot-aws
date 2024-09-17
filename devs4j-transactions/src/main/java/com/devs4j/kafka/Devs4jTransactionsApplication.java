package com.devs4j.kafka;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.devs4j.kafka.models.Devs4jTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

@SpringBootApplication
@EnableScheduling // Anotación necesaria para poder usar la anotación @Scheduled en esta aplicación Spring Boot, que es una anotación que nos permite programar tareas que se ejecuten cada cierto tiempo de forma automática
public class Devs4jTransactionsApplication{ //implements CommandLineRunner{ // PARA LA PRUEBA DEL CLIENTE DE ELASTICSEARCH
	
	
	// Habilitamos el logger en esta clase
	private static Logger log = LoggerFactory.getLogger(Devs4jTransactionsApplication.class);
	
	// Inyectamos del contenedor, o memoria, de Spring el bean de tipo "KafkaTemplate" que se corresponde con una plantilla de Kafka que nos permite producir, o enviar, mensajes a tópicos de brokers de Kafka 
	@Autowired
	private KafkaTemplate<String, String > kafkaTemplate;
	
	// Inyectamos del contenedor, o memoria, de Spring este bean para poder realizar mapeos de información entre objetos
	@Autowired
	private ObjectMapper mapper;
	
	// Inyectamos del contenedor, o memoria, de Spring el bean de tipo "RestHighLevelClient" que se corresponde con el cliente de ElasticSearch 
	@Autowired
	private RestHighLevelClient client;
	
	@KafkaListener(topics = "devs4j-transactions", groupId = "devs4j-group", containerFactory = "listenerContainerFactory")
	public void listen(List<ConsumerRecord<String, String>> messages) throws JsonMappingException, JsonProcessingException {
		for(ConsumerRecord<String, String> message: messages) {
			// Comentamos la escritura en el log de abajo porque en modo información se escribe en consola y es una operación lenta cuando estamos enviando a Kafka una gran cantidad de mensajes
			//log.info("Partition: {}, Offset: {}, Key: {}, Message: {}", message.partition(), message.offset(), message.key(), message.value());
			// Crea una petición de ElasticSearch para su indexado
			// Usamos una combinación de la partición, la key, o clave, y el offset del mensaje para el identificador de la petición de ElasticSearch 
			IndexRequest request = buildIndexRequest(String.format("%s-%s-%s", message.partition(), message.key(), message.offset()), message.value());
			// Realiza el indexado de la petición anterior en ElasticSearch usando el cliente de forma asíncrona
			client.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
				
				// Método que se ejecutará en caso de indexación satisfactoria
				@Override
				public void onResponse(IndexResponse response) {
					log.debug("Succesfull rquest");
					
				}
				
				// Método que se ejecutará en caso de indexación errónea
				@Override
				public void onFailure(Exception e) {
					log.error("Error storing the message {}", e);
					
				}
			});
		}
	}
	
	// Método que crea un petición de indexado para ElasticSearch a partir de una clave, o key, que se usará como identificador de la petición, y a partir del contenido que se quiere indexar
	private IndexRequest buildIndexRequest(String key, String value) {
		// Creamos una petición de indexado para ElasticSearch índicando el nombre del índice de ElasticSearch a usar(tiene que existir previamente)
		IndexRequest request= new IndexRequest("devs4j-transactions");
		// Establecemos un identificador a la petición de indexado a partir de la clave, o key, que se recibe como argumento de entrada
		request.id(key);
		// Establecemos el contenido a indexar a partir del parámetro de entrada "value" que es un JSON
		// "XContentType.JSON" indica que el tipo de dato o contenido a indexar en ElasticSearch es un JSON
		request.source(value, XContentType.JSON);
		return request;
	}
	
	// Con esta anotación, programamos la ejecución de este método cada 15000 ms(15 seg) de tiempo comenzando la primera ejecución justo después de arrancar la aplicación Spring Boot
	@Scheduled(fixedRate = 15000)
	public void sendMessages() throws JsonProcessingException {
		// Instancia para generar datos de ejemplo
		Faker faker = new Faker();
		// Enviamos 10000 mensajes a Kafka que se corresponden con instancias de tipo Devs4jTransaction
		for(int i = 0; i < 10000; i++) {
			// Creamos una instancia de tipo Devs4jTransaction y le asignamos a sus propiedades datos de ejemplo generados por Faker
			Devs4jTransaction transaction = new Devs4jTransaction();
			transaction.setUsername(faker.name().username());
			transaction.setNombre(faker.name().firstName());
			transaction.setApellido(faker.name().lastName());
			transaction.setMonto(faker.number().randomDouble(4, 0, 50000)); // Genera aleatoriamente un double entre 0 y 50000 usando 4 dígitos como máximo en la parte decimal
			
			// Enviamos al tópico "devs4j-transactions" el objeto "transaction" de tipo Devs4jTransaction serializado como un String
			// Usamos, como clave, o key, el valor de la propiedad "username" de ese objeto
			kafkaTemplate.send("devs4j-transactions",transaction.getUsername(),mapper.writeValueAsString(transaction));
	
		}
	}
	
	public static void main(String[] args) {
		SpringApplication.run(Devs4jTransactionsApplication.class, args);
	}
	
	/*
	// CÓDIGO PARA PROBAR EL CLIENTE DE ELASTICSEARCH INDEXANDO UN JSON
	@Override
	public void run(String... args) throws Exception {
		// Creamos una petición de indexado para ElasticSearch índicando el nombre del índice de ElasticSearch a usar(tiene que existir previamente)
		IndexRequest request= new IndexRequest("devs4j-transactions");
		// Establecemos un identificador a la petición de indexado
		request.id("44");
		// Establecemos un JSON con los datos que queremos indexar a ElasticSearch
		request.source("{\"nombre\":\"Sammie\"," + "\"apellido\":\"Goldner\"," + "\"username\":\"hugh.vonrueden\"," + "\"monto\":9622235.2009}", XContentType.JSON);
		
		// Realiza el indexado en ElasticSearch a través del cliente de forma síncrona
		// Para el rendimiento, es mejor usar la forma asíncrona con el método "indexAsync"
		// "XContentType.JSON" indica que el tipo de dato o contenido a indexar en ElasticSearch es un JSON
		IndexResponse response = client.index(request, RequestOptions.DEFAULT);
		
		log.info("Response id= {}", response.getId() );
	}*/

}
