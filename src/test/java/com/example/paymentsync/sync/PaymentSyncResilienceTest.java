package com.example.paymentsync.sync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.example.paymentsync.payment.Payment;
import com.example.paymentsync.payment.PaymentRepository;
import com.example.paymentsync.payment.PaymentStatus;
import com.example.paymentsync.pg.PgClient;
import com.example.paymentsync.pg.PgClientException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// 실제 MySQL/Redis 필요 (docker compose up -d). 전용 스트림/DLQ 키 사용, min-idle 0으로 즉시 회수 가능.
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "app.outbox.stream-key=payment:sync:stream:resilience-test",
        "app.sync.dlq-key=payment:sync:dlq:resilience-test",
        "app.sync.recovery.min-idle-ms=0",
        "app.sync.recovery.max-delivery-count=3"})
class PaymentSyncResilienceTest {

    private static final String STREAM_KEY = "payment:sync:stream:resilience-test";
    private static final String DLQ_KEY = "payment:sync:dlq:resilience-test";
    private static final String GROUP = "payment-sync-group";

    @Autowired
    PaymentSyncConsumer consumer;
    @Autowired
    PaymentRepository paymentRepository;
    @Autowired
    StringRedisTemplate redisTemplate;
    @Autowired
    MockMvc mockMvc;
    @MockitoSpyBean
    PgClient pgClient;

    private final String orderId = "test-" + UUID.randomUUID();

    @BeforeEach
    void setUp() {
        redisTemplate.delete(List.of(STREAM_KEY, DLQ_KEY));
        consumer.initGroup();
    }

    @AfterEach
    void cleanUp() {
        redisTemplate.delete(List.of(STREAM_KEY, DLQ_KEY));
        paymentRepository.findByOrderId(orderId).ifPresent(paymentRepository::delete);
    }

    private Payment givenPayment(String pgKey) {
        return paymentRepository.saveAndFlush(new Payment(orderId, pgKey, 10000));
    }

    private void addMessage(String pgKey) {
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
    void 콜백이_먼저_도착해_확정된_결제의_스트림_메시지는_PG_호출없이_XACK된다() throws Exception {
        String pgKey = "pg-ok-" + UUID.randomUUID();
        givenPayment(pgKey);
        mockMvc.perform(post("/api/payments/callback").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"" + orderId + "\",\"pgTransactionKey\":\"" + pgKey
                                + "\",\"status\":\"COMPLETED\"}"))
                .andExpect(status().isOk());
        assertThat(statusOf()).isEqualTo(PaymentStatus.COMPLETED);

        addMessage(pgKey);
        consumer.consume();

        verify(pgClient, never()).getPaymentStatus(any());
        assertThat(statusOf()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
    }

    @Test
    void PG_일시_오류로_Pending에_남은_메시지가_복구로직으로_COMPLETED_처리된다() {
        String pgKey = "pg-ok-" + UUID.randomUUID();
        givenPayment(pgKey);
        addMessage(pgKey);
        // 첫 호출만 PG 일시 오류, 이후는 정상 응답
        doThrow(new PgClientException("temporary")).doCallRealMethod().when(pgClient).getPaymentStatus(any());

        consumer.consume();
        assertThat(statusOf()).isEqualTo(PaymentStatus.PENDING);
        assertThat(pendingCount()).isEqualTo(1);

        consumer.recoverPendingMessages();

        assertThat(statusOf()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
        assertThat(redisTemplate.hasKey(DLQ_KEY)).isFalse();
    }

    @Test
    void PG_오류가_3회_반복되면_DLQ로_이동하고_원본_Pending은_0건이다() {
        String pgKey = "pg-error-" + UUID.randomUUID();
        givenPayment(pgKey);
        addMessage(pgKey);

        consumer.consume();            // 전달 1회차
        consumer.recoverPendingMessages(); // 2회차
        assertThat(pendingCount()).isEqualTo(1);
        consumer.recoverPendingMessages(); // 3회차 -> 한도 도달, DLQ 이관

        verify(pgClient, times(3)).getPaymentStatus(any());
        assertThat(pendingCount()).isZero();
        assertThat(statusOf()).isEqualTo(PaymentStatus.PENDING);

        List<MapRecord<String, Object, Object>> dlq = redisTemplate.opsForStream().range(DLQ_KEY, Range.unbounded());
        assertThat(dlq).hasSize(1);
        Map<Object, Object> fields = dlq.get(0).getValue();
        assertThat(fields.get("aggregateId")).isEqualTo(orderId);
        assertThat(String.valueOf(fields.get("payload"))).contains(orderId);
        assertThat(fields.get("originalStreamKey")).isEqualTo(STREAM_KEY);
        assertThat(fields.get("deliveryCount")).isEqualTo("3");
        assertThat(String.valueOf(fields.get("failureReason"))).contains("PG lookup failed");

        // DLQ로 이동한 뒤에는 더 이상 재시도하지 않는다
        consumer.recoverPendingMessages();
        verify(pgClient, times(3)).getPaymentStatus(any());
    }
}
