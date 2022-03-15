package com.example.alurakafka;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {
                for (int i = 0; i <= 100; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var order = new Order(userId, orderId, amount);
                    var email = "Dear, welcome to my ecommerce, look your product Sir.";
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                    emailDispatcher.send("ECOMMERCE_SAND_EMAIL", userId, email);
                }
            }
        }

    }}