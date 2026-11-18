package com.example.paymentsync.payment;

public class InvalidCallbackException extends RuntimeException {

    public InvalidCallbackException(String message) {
        super(message);
    }
}
