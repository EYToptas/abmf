package com.abmf.kafkaconsumer.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class UsageEventConsumer {

   // @KafkaListener(topics = "abmf-usage", groupId = "abmf-consumer-group")
    public void consume(String message) {
        System.out.println("Kafka'dan gelen mesaj: " + message);
    }

}
