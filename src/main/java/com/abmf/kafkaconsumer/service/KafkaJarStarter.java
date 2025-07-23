package com.abmf.kafkaconsumer.service;

import java.io.IOException;

public class KafkaJarStarter {

    public static void startKafkaConsumer() {
        try {
            ProcessBuilder builder = new ProcessBuilder(
                    "java",
                    "-jar",
                    "lib/KafkaApp-1.0-SNAPSHOT.jar",
                    "--topic=chf-to-abmf"
            );
            builder.redirectOutput(ProcessBuilder.Redirect.appendTo(new java.io.File("logs/kafka.log")));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(new java.io.File("logs/kafka-error.log")));

            builder.start();
            System.out.println("\n\n\n\n\n\n\nKafka JAR başlatıldı.\n\n\n\n\n");
        } catch (IOException e) {
            System.err.println("Kafka JAR başlatılamadı: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
