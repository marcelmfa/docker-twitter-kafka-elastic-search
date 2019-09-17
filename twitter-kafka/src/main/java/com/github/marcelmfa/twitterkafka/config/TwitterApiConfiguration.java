package com.github.marcelmfa.twitterkafka.config;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

import com.twitter.hbc.httpclient.auth.OAuth1;

@Configuration
public class TwitterApiConfiguration {
	
	private static final Logger LOG = LoggerFactory.getLogger(TwitterApiConfiguration.class);

	private static final String CLIENT_NAME_KEY = "twitter.client_name";
	
	private static final String POLLING_SECONDS_KEY = "twitter.polling_seconds";
	
	private static final String DEBUG_KEY = "twitter.debug";

	@Value("${twitter.api_key}")
	private String apiKey;
	
	@Value("${twitter.api_secret_key}")
	private String apiSecretKey;
	
	@Value("${twitter.access_token}")
	private String accessToken;
	
	@Value("${twitter.access_token_secret}")
	private String accessTokenSecret;
	
	@Value("${twitter.max_messages}")
	private Long maxMessages = 100000L;
	
	@Value("${twitter.terms}")
	private String[] terms;
	
	private String clientName = "Hosebird-Client";
	
	private Integer pollingSeconds = 5;
	
	private boolean debug = false;
	
	private Environment env;
	
	public TwitterApiConfiguration(Environment env) {
		this.env = env;
	}

	@PostConstruct
	public void postContruct() {
		Assert.hasText(apiKey, "'apiKey' must not be empty");
		Assert.hasText(apiSecretKey, "'apiSecretKey' must not be empty");
		Assert.hasText(accessToken, "'accessToken' must not be empty");
		Assert.hasText(accessTokenSecret, "'accessTokenSecret' must not be empty");

		if (env.containsProperty(CLIENT_NAME_KEY)) {
			clientName = env.getProperty(CLIENT_NAME_KEY);
		}
		
		if (env.containsProperty(POLLING_SECONDS_KEY)) {
			pollingSeconds = env.getProperty(POLLING_SECONDS_KEY, Integer.class);
		}
		
		if (env.containsProperty(DEBUG_KEY)) {
			debug = env.getProperty(DEBUG_KEY, Boolean.class);
			LOG.info("TWITTER DEBUG ENABLED");
		}
	}

	public OAuth1 toOAuth1() {
		return new OAuth1(apiKey, apiSecretKey, accessToken, accessTokenSecret);
	}
	
	public Long getMaxMessages() {
		return maxMessages;
	}
	
	public String getClientName() {
		return clientName;
	}
	
	public Integer getPollingSeconds() {
		return pollingSeconds;
	}
	
	public boolean isDebug() {
		return debug;
	}
	
	public String[] getTerms() {
		return terms;
	}
}
