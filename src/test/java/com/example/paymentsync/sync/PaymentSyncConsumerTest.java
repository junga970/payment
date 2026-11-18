package com.example.paymentsync.sync;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.paymentsync.payment.Payment;
import com.example.paymentsync.payment.PaymentRepository;
import com.example.paymentsync.payment.PaymentStatus;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

// 실제 MySQL/Redis 필요 (docker compose up -d). 테스트 전용 스트림 키 사용.
@SpringBootTest(properties = "app.outbox.stream-key=payment:sync:stream:consumer-test")
class PaymentSyncConsumerTest {

    private static final String STREAM_KEY = "payment:sync:stream:consumer-test";
    private static final String GROUP = "payment-sync-group";

    @Autowired
    PaymentSyncConsumer consumer;
    @Autowired
    PaymentRepository paymentRepository;
    @Autowired
    StringRedisTemplate redisTemplate;

    private final String orderId = "test-" + UUID.randomUUID();

    @BeforeEach
    void setUp() {
        redisTemplate.delete(STREAM_KEY);
        consumer.initGroup();
    }

    @AfterEach
    void cleanUp() {
        redisTemplate.delete(STREAM_KEY);
        paymentRepository.findByOrderId(orderId).ifPresent(paymentRepository::delete);
    }

    private void givenPaymentAndMessage(String pgKey) {
        paymentRepository.saveAndFlush(new Payment(orderId, pgKey, 10000));
        String payload = "{\"orderId\":\"" + orderId + "\",\"pgTransactionKey\":\"" + pgKey + "\"}";
        redisTemplate.opsForStream().add(STREAM_KEY, Map.of(
                "eventId", "1", "aggregateType", "PAYMENT", "aggregateId", orderId,
                "eventType", "PAYMENT_SYNC_REQUESTED", "payload", payload));
    }

    private long pendingCount() {
        return redisTemplate.opsForStream().pending(STREAM_KEY, GROUP).getTotalPendingMessages();
    }

    private PaymentStatus statusOf() {
        return paymentRepository.findByOrderId(orderId).orElseThrow().getStatus();
    }

    @Test
    void PG가_승인완료면_Payment가_COMPLETED가_되고_XACK된다() {
        givenPaymentAndMessage("pg-ok-" + UUID.randomUUID());

        consumer.consume();

        assertThat(statusOf()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
    }

    @Test
    void PG가_실패면_Payment가_FAILED가_되고_XACK된다() {
        givenPaymentAndMessage("pg-fail-" + UUID.randomUUID());

        consumer.consume();

        assertThat(statusOf()).isEqualTo(PaymentStatus.FAILED);
        assertThat(pendingCount()).isZero();
    }

    @Test
    void PG_조회_예외시_상태는_PENDING_유지되고_XACK되지_않는다() {
        givenPaymentAndMessage("pg-error-" + UUID.randomUUID());

        consumer.consume();

        assertThat(statusOf()).isEqualTo(PaymentStatus.PENDING);
        assertThat(pendingCount()).isEqualTo(1);
    }

    @Test
    void 그룹_초기화는_여러번_호출해도_안전하다() {
        consumer.initGroup();
        consumer.initGroup();
        assertThat(redisTemplate.opsForStream().groups(STREAM_KEY).stream()
                .filter(g -> g.groupName().equals(GROUP))).hasSize(1);
    }
}
