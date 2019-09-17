package com.github.marcelmfa.twitterkafka.components.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.components.elasticsearch.ElasticSearchProducer;
import com.github.marcelmfa.twitterkafka.config.KafkaConfiguration;

@Component
public class KafkaConsumerComponent {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerComponent.class);
	
	/**
	 * Kafka consumer default values.
	 */
	private static final String GROUP_ID = "MyLabApp";
	private static final String DEFAULT_OFFSET_CONFIG = "earliest";
	
	private KafkaConsumer<String, String> kafkaConsumer;
	
	private KafkaConfiguration config;
	
	private ElasticSearchProducer elasticSearchProducer;

	public KafkaConsumerComponent(KafkaConfiguration kafkaConfiguration, ElasticSearchProducer elasticSearchProducer) {
		super();
		this.config = kafkaConfiguration;
		this.elasticSearchProducer = elasticSearchProducer;
	}

	@PostConstruct
	public void postConstruct() {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBoostrapServers());
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_OFFSET_CONFIG);
		
		kafkaConsumer = new KafkaConsumer<>(props);
		kafkaConsumer.subscribe(Arrays.asList(config.getTopic()));
	}
	
	@EventListener(ApplicationReadyEvent.class)
	public void runAfterApplicationReady() throws IOException {
		
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer
					.poll(Duration.ofSeconds(config.getPollingSeconds()));
			
			for (ConsumerRecord<String, String> record: records) {
				String jsonString = record.value();
				if (config.isDebug()) {
					LOG.info("Read data: " + jsonString + " from Kafka");
				}
				try {
					elasticSearchProducer.produce(jsonString);
				} catch (IOException e) {
					LOG.error("Failed send data " + jsonString + " to ElasticSearch");
					throw e;
				}
			}
		}
	}
	
	@PreDestroy
	public void destroy() {
		LOG.info("----------------------- SHUTTING DOWN KAFKA CONSUMER -----------------------");
		kafkaConsumer.close();
	}
}