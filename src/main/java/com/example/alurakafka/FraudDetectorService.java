package com.example.alurakafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudDetectorService::toGoPass);
        service.run();
    }
        private void toGoPass(ConsumerRecord<String, String> record){

            System.out.printf("<-------------------------------------------------------------------------->");
            System.out.printf("Fraud Detector ok, ok |-|");
            System.out.printf(record.key());
            System.out.printf(record.value());
            System.out.printf(record.topic());
            System.out.printf(String.valueOf(record.offset()));
            System.out.printf(String.valueOf(record.partition()));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.printf("Email send with success");
        }
}
