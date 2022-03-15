package com.example.alurakafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.HashMap;

public class FraudDetectorService {
    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try(
                var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                        "ECOMMERCE_NEW_ORDER", fraudDetectorService::toGoPass,
                        Order.class,
                        new HashMap<>() {
                        });
            ){
            service.run();}
    }
        private void toGoPass(ConsumerRecord<String, Order> record){

            System.out.printf("<-------------------------------------------------------------------------->");
            System.out.printf("Fraud Detector ok, ok |-|");
            System.out.printf(record.key());
            System.out.printf(String.valueOf(record.value()));
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
