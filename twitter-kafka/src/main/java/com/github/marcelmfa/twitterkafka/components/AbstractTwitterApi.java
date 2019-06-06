package com.github.marcelmfa.twitterkafka.components;

import java.util.concurrent.BlockingQueue;

import com.github.marcelmfa.twitterkafka.config.TwitterApiConfiguration;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;

public abstract class AbstractTwitterApi<T> {

	protected TwitterApiConfiguration config;
	
	public AbstractTwitterApi(TwitterApiConfiguration config) {
		super();
		this.config = config;
	}
	
	protected Client createTwitterClient(BlockingQueue<String> msgQueue) {
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
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}
	
	public abstract T run();
}
