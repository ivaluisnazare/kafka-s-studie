package com.example.alurakafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Map;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var logService = new LogService();

        try (var service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"), logService::toGoPass,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }
    private void toGoPass(ConsumerRecord<String, String> record) {
            System.out.printf("<-------------------------------------------------------------------------->");
            System.out.printf("Processing log OK |-|" + record.topic());
            System.out.printf(record.key());
            System.out.printf(record.value());
            System.out.printf(record.topic());
            System.out.printf(String.valueOf(record.offset()));
            System.out.printf(String.valueOf(record.partition()));
                }
    }
