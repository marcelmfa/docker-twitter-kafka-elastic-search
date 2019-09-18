package com.github.marcelmfa.twitterkafka;

import java.io.IOException;

import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TwitterKafkaApplication {
	
	public static void main(String[] args) throws BeansException, IOException {
		SpringApplication.run(TwitterKafkaApplication.class, args);
	}
}
