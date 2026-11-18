package com.example.paymentsync.payment;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(String orderId) {
        super("Payment not found: orderId=" + orderId);
    }
}
