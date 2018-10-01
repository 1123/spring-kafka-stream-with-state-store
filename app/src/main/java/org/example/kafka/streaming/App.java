package org.example.kafka.streaming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableKafkaStreams
@EnableScheduling
@EnableKafka
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class);
    }

}

