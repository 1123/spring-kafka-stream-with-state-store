package org.example.kafka.streaming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Component
@Slf4j
public class SampleMessageProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${local.stream.input}")
    private String inputTopic;

    @Autowired
    public SampleMessageProducer(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    private static final List<String> sources = Arrays.asList("s1", "s2", "s3");
    private static final List<String> keys = Arrays.asList("key1", "key2", "key3", "key4", "key5");
    private static Random r = new Random();
    private static long startTime = 0L;

    @Scheduled(fixedDelay = 1000)
    public void send() {
        log.info("================ Sending sample message ====================");
        int key = r.nextInt(5);
        kafkaTemplate.send(inputTopic, key, randomMessage(key).toString());
    }

    private static Message randomMessage(int key) {
        int timestep = r.nextInt(100);
        startTime += timestep;
        return Message.builder()
                .key(key)
                .lat(r.nextFloat())
                .lng(r.nextFloat())
                .source(sources.get(r.nextInt(3)))
                .timestamp(startTime)
                .build();
    }

}


