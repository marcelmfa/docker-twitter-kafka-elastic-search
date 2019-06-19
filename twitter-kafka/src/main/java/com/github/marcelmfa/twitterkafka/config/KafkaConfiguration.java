package com.github.marcelmfa.twitterkafka.config;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class KafkaConfiguration {
	
	private static final String SAFE_PRODUCER_KEY = "kafka.safe_producer";

	@Value("${kafka.boostrap_servers}")
	private String boostrapServers;
	
	private boolean safeProducer = false;
	
	private Environment env;
	
	public KafkaConfiguration(Environment env) {
		super();
		this.env = env;
	}

	@PostConstruct
	public void postConstruct() {
		if (env.containsProperty(SAFE_PRODUCER_KEY)) {
			safeProducer = Boolean.getBoolean(env.getProperty(SAFE_PRODUCER_KEY));
		}
	}
	
	public String getBoostrapServers() {
		return boostrapServers;
	}
	
	public boolean isSafeProducer() {
		return safeProducer;
	}
}
