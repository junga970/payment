package com.example.paymentsync.payment;

public class DuplicatePaymentException extends RuntimeException {

    public DuplicatePaymentException(String orderId) {
        super("Payment already exists for orderId=" + orderId);
    }
}
