package com.github.marcelmfa.twitterkafka.components.kafka;

import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.config.KafkaConfiguration;

@Component
public class KafkaConsumerWrapper {

	private KafkaConsumer<String, String> kafkaConsumer;
	
	private KafkaConfiguration config;

	public KafkaConsumerWrapper(KafkaConfiguration kafkaConfiguration) {
		super();
		this.config = kafkaConfiguration;
	}

	@PostConstruct
	public void postConstruct() {
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBoostrapServers());
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	}
	
	@EventListener(ApplicationReadyEvent.class)
	public void runAfterApplicationReady() {
		
	}
}