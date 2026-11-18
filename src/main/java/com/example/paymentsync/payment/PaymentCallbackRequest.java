package com.example.paymentsync.payment;

import com.example.paymentsync.pg.PgPaymentStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record PaymentCallbackRequest(
        @NotBlank String orderId,
        @NotBlank String pgTransactionKey,
        @NotNull PgPaymentStatus status) {
}
