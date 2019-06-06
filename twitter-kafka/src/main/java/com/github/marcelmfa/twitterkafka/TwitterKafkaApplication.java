package com.github.marcelmfa.twitterkafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.github.marcelmfa.twitterkafka.components.TwitterApiConsumer;

@SpringBootApplication
public class TwitterKafkaApplication {
	
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(TwitterKafkaApplication.class, args);
		
		TwitterApiConsumer consumer = context.getBean(TwitterApiConsumer.class);
		consumer.run();
	}
}
