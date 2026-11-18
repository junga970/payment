package com.example.paymentsync.pg;

public class PgClientException extends RuntimeException {

    public PgClientException(String message) {
        super(message);
    }
}
