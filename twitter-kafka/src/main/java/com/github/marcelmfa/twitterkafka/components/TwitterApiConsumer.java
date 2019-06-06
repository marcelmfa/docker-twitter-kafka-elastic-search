package com.github.marcelmfa.twitterkafka.components;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.marcelmfa.twitterkafka.config.TwitterApiConfiguration;
import com.twitter.hbc.core.Client;

@Component
public class TwitterApiConsumer extends AbstractTwitterApi<Void>{
	
	private Logger LOG = LoggerFactory.getLogger(TwitterApiConsumer.class);

	public TwitterApiConsumer(TwitterApiConfiguration config) {
		super(config);
	}

	public Void run( ) {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(config.getMaxMessages().intValue());

		Client client = createTwitterClient(msgQueue);
		client.connect();
		
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
				LOG.info(msg);
			}
		}
		
		return null;
	}
}
