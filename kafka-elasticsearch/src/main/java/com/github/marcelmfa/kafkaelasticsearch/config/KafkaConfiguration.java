package com.github.marcelmfa.kafkaelasticsearch.config;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

@Configuration
public class KafkaConfiguration {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConfiguration.class);
	
	private static final String POLLING_SECONDS_KEY = "kafka.polling_seconds";
	
	private static final String SAFE_PRODUCER_KEY = "kafka.safe_producer";
	
	private static final String DEBUG_KEY = "kafka.debug";

	@Value("${kafka.boostrap_servers}")
	private String boostrapServers;
	
	@Value("${kafka.topic}")
	private String topic;
	
	private Integer pollingSeconds = 5;
	
	private boolean safeProducer = false;
	
	private boolean debug = false;
	
	private Environment env;
	
	public KafkaConfiguration(Environment env) {
		super();
		this.env = env;
	}

	@PostConstruct
	public void postConstruct() {
		Assert.hasText(boostrapServers, "'boostrapServers' must not be empty");
		Assert.hasText(topic, "'topic' must not be empty");
		
		if (env.containsProperty(POLLING_SECONDS_KEY)) {
			pollingSeconds = env.getProperty(POLLING_SECONDS_KEY, Integer.class);
		}
		
		if (env.containsProperty(SAFE_PRODUCER_KEY)) {
			safeProducer = env.getProperty(SAFE_PRODUCER_KEY, Boolean.class);
		}
		
		if (env.containsProperty(DEBUG_KEY)) {
			debug = env.getProperty(DEBUG_KEY, Boolean.class);
			LOG.info("KAFKA DEBUG ENABLED");
		}
	}
	
	public String getBoostrapServers() {
		return boostrapServers;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public Integer getPollingSeconds() {
		return pollingSeconds;
	}
	
	public boolean isSafeProducer() {
		return safeProducer;
	}
	
	public boolean isDebug() {
		return debug;
	}
}
