package com.example.paymentsync.payment;

import java.time.LocalDateTime;

public record PaymentResponse(
        Long id,
        String orderId,
        String pgTransactionKey,
        long amount,
        PaymentStatus status,
        LocalDateTime createdAt) {

    public static PaymentResponse from(Payment p) {
        return new PaymentResponse(p.getId(), p.getOrderId(), p.getPgTransactionKey(),
                p.getAmount(), p.getStatus(), p.getCreatedAt());
    }
}
