package com.example.paymentsync.payment;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> register(@Valid @RequestBody PaymentRegisterRequest request) {
        Payment payment = paymentService.registerPayment(
                request.orderId(), request.pgTransactionKey(), request.amount());
        return ResponseEntity.status(HttpStatus.CREATED).body(PaymentResponse.from(payment));
    }

    /** PG 콜백. 이미 확정된 건이 중복 수신돼도 200 OK (멱등). */
    @PostMapping("/callback")
    public ResponseEntity<PaymentResponse> callback(@Valid @RequestBody PaymentCallbackRequest request) {
        Payment payment = paymentService.handleCallback(
                request.orderId(), request.pgTransactionKey(), request.status());
        return ResponseEntity.ok(PaymentResponse.from(payment));
    }

    @ExceptionHandler(PaymentNotFoundException.class)
    public ResponseEntity<String> handleNotFound(PaymentNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
    }

    @ExceptionHandler(InvalidCallbackException.class)
    public ResponseEntity<String> handleInvalidCallback(InvalidCallbackException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }

    @ExceptionHandler(DuplicatePaymentException.class)
    public ResponseEntity<String> handleDuplicate(DuplicatePaymentException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
    }
}
