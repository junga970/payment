package com.example.paymentsync.payment;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record PaymentRegisterRequest(
        @NotBlank String orderId,
        @NotBlank String pgTransactionKey,
        @Positive long amount) {
}
