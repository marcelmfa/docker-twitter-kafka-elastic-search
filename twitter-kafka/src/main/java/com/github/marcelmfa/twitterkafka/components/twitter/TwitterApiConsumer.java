package com.github.marcelmfa.twitterkafka.components.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.components.kafka.KafkaProducerWrapper;
import com.github.marcelmfa.twitterkafka.config.TwitterApiConfiguration;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;

@Component
public class TwitterApiConsumer {
	
	private Logger LOG = LoggerFactory.getLogger(TwitterApiConsumer.class);

	private static final String TOPIC = "kafka-tweets";
	
	private TwitterApiConfiguration config;
	
	private KafkaProducerWrapper kafkaProducerWrapper;
	
	private BlockingQueue<String> msgQueue;
	
	private Client client;

	public TwitterApiConsumer(TwitterApiConfiguration config, KafkaProducerWrapper kafkaProducerWrapper) {
		super();
		this.config = config;
		this.kafkaProducerWrapper = kafkaProducerWrapper;
	}
	
	private Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint()
				.trackTerms(Lists.newArrayList("kafka"));

		ClientBuilder builder = new ClientBuilder()
				.name(config.getClientName()) // optional: mainly for the logs
				.hosts(HttpHosts.STREAM_HOST)
				.authentication(config.toOAuth1())
				.endpoint(hosebirdEndpoint)
				.retries(3)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
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
	
	public void run( ) {
		String msg = null;
		while (!client.isDone()) {
			try {
				msg = msgQueue.poll(config.getPollingSeconds(), TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				LOG.error("Error polling twitter messages", e);
				client.stop();
				break;
			}
			
			if (msg != null) {
				kafkaProducerWrapper.digest(TOPIC, msg);
			}
		}
	}
	
	@EventListener(ApplicationReadyEvent.class)
	public void runAfterApplicationReady() {
		run();
	}
	
	@PreDestroy
	public void destroy() {
		LOG.info("shutting down twitter api consumer");
		msgQueue.clear();
		msgQueue = null;
		client.stop();
	}
}
