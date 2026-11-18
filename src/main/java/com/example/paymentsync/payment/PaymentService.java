package com.example.paymentsync.payment;

import com.example.paymentsync.outbox.OutboxEvent;
import com.example.paymentsync.outbox.OutboxEventRepository;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    static final String AGGREGATE_TYPE = "PAYMENT";
    static final String EVENT_TYPE_SYNC_REQUESTED = "PAYMENT_SYNC_REQUESTED";

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository paymentRepository,
                          OutboxEventRepository outboxEventRepository,
                          ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Payment(PENDING)와 OutboxEvent를 하나의 트랜잭션으로 저장한다.
     * 동일 orderId가 이미 있으면 DuplicatePaymentException (멱등성).
     * 동시 요청이 조회를 모두 통과해도 order_id 유니크 제약이 최종 방어선이 된다.
     */
    @Transactional
    public Payment registerPayment(String orderId, String pgTransactionKey, long amount) {
        if (paymentRepository.findByOrderId(orderId).isPresent()) {
            throw new DuplicatePaymentException(orderId);
        }

        Payment payment;
        try {
            payment = paymentRepository.saveAndFlush(new Payment(orderId, pgTransactionKey, amount));
        } catch (DataIntegrityViolationException e) {
            throw new DuplicatePaymentException(orderId);
        }

        outboxEventRepository.save(new OutboxEvent(
                AGGREGATE_TYPE, orderId, EVENT_TYPE_SYNC_REQUESTED, toPayload(payment)));
        return payment;
    }

    /**
     * PG 콜백을 Payment 상태에 반영한다. 이미 확정(COMPLETED/FAILED)된 건이면 아무것도 하지 않는다(멱등).
     * pgTransactionKey가 저장된 값과 다르면 거부한다.
     */
    @Transactional
    public Payment handleCallback(String orderId, String pgTransactionKey, PgPaymentStatus pgStatus) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));
        if (!payment.getPgTransactionKey().equals(pgTransactionKey)) {
            throw new InvalidCallbackException("pgTransactionKey mismatch: orderId=" + orderId);
        }
        applyIfPending(payment, pgStatus);
        return payment;
    }

    /** 컨슈머가 PG 조회 결과를 반영한다. 이미 확정된 건(콜백 선도착 등)이면 무시한다. */
    @Transactional
    public void applyPgResult(String orderId, PgPaymentStatus pgStatus) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new PaymentNotFoundException(orderId));
        applyIfPending(payment, pgStatus);
    }

    private void applyIfPending(Payment payment, PgPaymentStatus pgStatus) {
        if (payment.getStatus() != PaymentStatus.PENDING) {
            log.info("Payment already finalized, ignoring: orderId={}, current={}, incoming={}",
                    payment.getOrderId(), payment.getStatus(), pgStatus);
            return;
        }
        switch (pgStatus) {
            case COMPLETED -> payment.complete();
            case FAILED -> payment.fail();
        }
    }

    private String toPayload(Payment payment) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("paymentId", payment.getId());
        body.put("orderId", payment.getOrderId());
        body.put("pgTransactionKey", payment.getPgTransactionKey());
        body.put("amount", payment.getAmount());
        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
