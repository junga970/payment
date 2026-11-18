package com.example.paymentsync.payment;

import com.example.paymentsync.outbox.OutboxEvent;
import com.example.paymentsync.outbox.OutboxEventRepository;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

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

    /** PG 조회 결과를 Payment 상태에 반영한다. 이미 확정된 건이면 도메인 검증에서 예외가 발생한다. */
    @Transactional
    public void applyPgResult(String orderId, PgPaymentStatus pgStatus) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException("Payment not found: orderId=" + orderId));
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
