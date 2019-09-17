package com.github.marcelmfa.twitterkafka.components.elasticsearch;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.config.ElasticSearchConfiguration;

@Component
public class ElasticSearchProducer {
	
	private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchProducer.class);
	
	private ElasticSearchConfiguration config;

	private RestHighLevelClient client;

	public ElasticSearchProducer(ElasticSearchConfiguration config) {
		super();
		this.config = config;
	}

	@PostConstruct
	public void postConstruct() {

		if (config.isSecure()) {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));

			RestClientBuilder builder = RestClient.builder(new HttpHost(config.getHostname(), 443, "https"))
					.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
						@Override
						public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
							return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
						}
					});
			client = new RestHighLevelClient(builder);
		} else {
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(config.getHostname(), 80, "http")));
		}
	}
	
	public String produce(String jsonData) throws IOException {

		IndexRequest request = new IndexRequest(config.getIndex(), config.getType());
		
		try {
			IndexResponse response = client.index(request, RequestOptions.DEFAULT);
			
			if (config.isDebug()) {
				LOG.info("Data: " + jsonData + " has been sent to ElasticSearch sucessfully. ID: " + response.getId());
			}
			
			return response.getId();
		} catch (IOException e) {
			LOG.error("Failed send data " + jsonData, e);
			throw e;
		}
	}
}
