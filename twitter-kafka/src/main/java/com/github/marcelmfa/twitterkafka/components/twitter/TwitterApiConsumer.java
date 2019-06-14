package com.github.marcelmfa.twitterkafka.components.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.components.kafka.KafkaProducerWrapper;
import com.github.marcelmfa.twitterkafka.config.TwitterApiConfiguration;
import com.twitter.hbc.core.Client;

@Component
public class TwitterApiConsumer extends AbstractTwitterApi<Void>{
	
	private Logger LOG = LoggerFactory.getLogger(TwitterApiConsumer.class);

	private static final String TOPIC = "kafka-tweets";
	
	private KafkaProducerWrapper kafkaProducerWrapper;
	
	private BlockingQueue<String> msgQueue;
	
	private Client client;

	public TwitterApiConsumer(TwitterApiConfiguration config, KafkaProducerWrapper kafkaProducerWrapper) {
		super(config);
		this.kafkaProducerWrapper = kafkaProducerWrapper;
	}
	
	@PostConstruct
	public void postConstruct() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		msgQueue = new LinkedBlockingQueue<String>(config.getMaxMessages().intValue());

		client = createTwitterClient(msgQueue);
		client.connect();
	}

	public Void run( ) {
		String msg = null;
		while (!client.isDone()) {
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("Error polling twitter messages", e);
				client.stop();
				break;
			}
			
			if (msg != null) {
				kafkaProducerWrapper.digest(TOPIC, msg);
			}
		}
		
		return null;
	}
	
	@PreDestroy
	public void destroy() {
		LOG.info("shutting down twitter api consumer");
		msgQueue.clear();
		msgQueue = null;
		client.stop();
	}
}
