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

@Component
public class KafkaProducerWrapper {
	
	private Logger LOG = LoggerFactory.getLogger(KafkaProducerWrapper.class);
	
	private KafkaProducer<String, String> kafkaProducer;
	
	@PostConstruct
	public void postConstruct() {
		String boostrapServers = "127.0.0.1:9092";
		
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		kafkaProducer = new KafkaProducer<>(props);
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
