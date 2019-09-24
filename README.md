# docker-twitter-kafka-elastic-search

A dockerized Kafka that receives data from *twitter-kafka* Spring Boot application and serves to *kafka-elasticsearch* Spring Boot application.

## Setup for applications

Most of setup of applications consists of **enviroment_variables**.

### Kafka

It's dockerized in docker-compose and consists of a single Kafka broker and a single Zookeeper. Change it if you wants another behavior or configure Spring Boot application to use another Kafka in yaml files.

### Elastic Search + Kibana

I have used [Bonsai](http://bonsai.io) that provides a free elasticsearch cluster for free tests. It also provides a Kibana app.

### Twitter-Kafka

In this app, you have to set these following environment variables:

- TWITTER_API_KEY
- TWITTER_API_SECRET_KEY
- TWITTER_ACCESS_TOKEN
- TWITTER_ACCESS_TOKEN_SECRET

These values you get after create a Twitter developer account and a app at their [website](https://developer.twitter.com/).

> This application have a Dockerfile if you want to generate a image with this app.

### Kafka-Elasticsearch

In this app, you have to set these following environment variables:

- ES_HOSTNAME
- ES_USERNAME
- ES_PASSWORD

These values you get in Bonsai or use your own ElasticSearch cluster.

> *secure* variable means that communication will be established by HTTPS, if be set to **false** so username and password will not be required and HTTP will be used.

## How to run

1. Run docker-compose
2. Run twitter-kafka Spring boot application
3. Run kafka-elasticsearch Spring boot application
4. Check tweets in Kibana provided by Bonsai

## FAQ

- Its failing on writing data on Elasticsearch. What i do now?
Create a index in Bonsai website. There are a tool that allows you interate with elasticsearch. So you can create indexes and a lot of other things.
