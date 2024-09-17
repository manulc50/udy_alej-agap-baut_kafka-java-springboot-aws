package com.devs4j.kafka.configs;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Clase con todas las configuraciones de ElasticSearch

@Configuration // Anotamos esta clase de configuración como una clase de configuración de Spring
public class ElasticSearchConfig {
	
	// Método que crea y devuelve un cliente de ElasticSearch como un bean de Spring
	@Bean(destroyMethod = "close") // Para liberar recursos invocaremos el método "close" de este cliente, cuando lo dejemos de utilizar
	public RestHighLevelClient createClient() {
		// Sólo si ElasticSearch requiere credenciales por autenticación básica
		/*final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials("user","password"));*/

		RestHighLevelClient client = new RestHighLevelClient(
				// Aquí indicamos el host, el puerto y el esquema donde está ejecutándose ElasticSearch
				RestClient.builder(new HttpHost("localhost", 9200,"http"))
				// Sólo si ElasticSearch requiere credenciales por autenticación básica	
				//.setHttpClientConfigCallback(config -> config.setDefaultCredentialsProvider(credentialsProvider))
		);
		return client;
	}

}
