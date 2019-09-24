package com.github.marcelmfa.kafkaelasticsearch.config;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;

@Configuration
public class ElasticSearchConfiguration {
	
	private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchConfiguration.class);

	private static final String SECURE_KEY = "elasticsearch.secure";
	
	private static final String DEBUG_KEY = "elasticsearch.debug";

	@Value("${elasticsearch.hostname}")
	private String hostname;

	@Value("${elasticsearch.username}")
	private String username;

	@Value("${elasticsearch.password}")
	private String password;

	@Value("${elasticsearch.index}")
	private String index;
	
	@Value("${elasticsearch.type}")
	private String type;
	
	private boolean secure = true;
	
	private Environment env;
	
	private boolean debug = false; 
	
	public ElasticSearchConfiguration(Environment env) {
		super();
		this.env = env;
	}

	@PostConstruct
	public void postContruct() {
		Assert.hasText(hostname, "'hostname' must not be empty");
		Assert.hasText(index, "'index' must not be empty");
		Assert.hasText(type, "'type' must not be empty");
		
		if (env.containsProperty(SECURE_KEY)) {
			secure = env.getProperty(SECURE_KEY, Boolean.class);
		}
		
		if (secure) {
			Assert.hasText(username, "'username' must not be empty");
			Assert.hasText(password, "'password' must not be empty");
		}
		
		if (env.containsProperty(DEBUG_KEY)) {
			debug = env.getProperty(DEBUG_KEY, Boolean.class);
			LOG.info("ELASTICSEARCH DEBUG ENABLED");
		}
	}

	public String getHostname() {
		return hostname;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getIndex() {
		return index;
	}
	
	public String getType() {
		return type;
	}

	public boolean isSecure() {
		return secure;
	}
	
	public boolean isDebug() {
		return debug;
	}
}
