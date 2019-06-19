package com.github.marcelmfa.twitterkafka.components.kafka;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.config.KafkaConfiguration;

@Component
public class KafkaProducerWrapper {
	
	private Logger LOG = LoggerFactory.getLogger(KafkaProducerWrapper.class);
	
	private KafkaProducer<String, String> kafkaProducer;
	
	private KafkaConfiguration config;
	
	public KafkaProducerWrapper(KafkaConfiguration kafkaConfiguration) {
		super();
		this.config = kafkaConfiguration;
	}

	@PostConstruct
	public void postConstruct() {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBoostrapServers());
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		if (config.isSafeProducer()) {
			addSafeProducerProperties(props);
		}
		
		kafkaProducer = new KafkaProducer<>(props);
	}
	
	private void addSafeProducerProperties(Properties props) {
		
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
	}
	
	public void digest(String topic, String msg) {
		kafkaProducer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					LOG.error("Failed send message", exception);
				}
			}
		});
	}
	
	@PreDestroy
	public void destroy() {
		LOG.info("shutting down kafka producer");
		kafkaProducer.close();
	}
}
