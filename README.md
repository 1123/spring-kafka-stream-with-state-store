# spring-kafka-stream-with-state-store

This is a sample Spring Boot application for a stateful transformation with Kafka-Streams. It reads messages from one topic, and outputs pairs of these messages to a new topic. 

Prerequisites:

* mvn 
* java 8
* a running kafka cluster

How to run: 
* adjust kafka connection details in src/main/resources/application.yaml
* start the app using mvn spring-boot:run
* watch the logs to see the original and transformed messages. 


