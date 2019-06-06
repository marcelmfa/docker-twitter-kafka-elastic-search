package com.github.marcelmfa.twitterkafka.config;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import com.twitter.hbc.httpclient.auth.OAuth1;

@Configuration
public class TwitterApiConfiguration {

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
	
	@Value("${twitter.client_name}")
	private String clientName = "Hosebird-Client";
	
	@PostConstruct
	public void postContruct() {
		Assert.hasText(apiKey, "'apiKey' must not be empty");
		Assert.hasText(apiSecretKey, "'apiSecretKey' must not be empty");
		Assert.hasText(accessToken, "'accessToken' must not be empty");
		Assert.hasText(accessTokenSecret, "'accessTokenSecret' must not be empty");
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
}
