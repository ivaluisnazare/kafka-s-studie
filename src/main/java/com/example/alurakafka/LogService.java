package com.example.alurakafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.printf("Found" + records.count() + "Record not found");
                for (var record : records) {
                    System.out.printf("<-------------------------------------------------------------------------->");
                    System.out.printf("Processing log OK |-|" + record.topic());
                    System.out.printf(record.key());
                    System.out.printf(record.value());
                    System.out.printf(record.topic());
                    System.out.printf(String.valueOf(record.offset()));
                    System.out.printf(String.valueOf(record.partition()));
                }
            }
        }
}

        private static Properties properties () {
            var properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());  // Optional, not influence in process.
            return properties;
        }
    }
