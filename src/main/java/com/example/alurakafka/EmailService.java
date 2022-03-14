package com.example.alurakafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(), "ECOMMERCE_SAND_EMAIL", emailService::toGoPass)){
        service.run();}
    }

    private void toGoPass(ConsumerRecord<String, String> record){

        System.out.printf("<-------------------------------------------------------------------------->");
        System.out.printf("Send email, ok |-|");
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
