package com.example.paymentsync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.paymentsync.outbox.OutboxEvent;
import com.example.paymentsync.outbox.OutboxEventRepository;
import com.example.paymentsync.outbox.OutboxPublisher;
import com.example.paymentsync.outbox.OutboxStatus;
import com.example.paymentsync.payment.PaymentRepository;
import com.example.paymentsync.payment.PaymentService;
import com.example.paymentsync.payment.PaymentStatus;
import com.example.paymentsync.pg.PgClient;
import com.example.paymentsync.pg.PgClientException;
import com.example.paymentsync.sync.PaymentSyncConsumer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;

/**
 * 결제 등록 -> Outbox 발행 -> Redis Streams -> 컨슈머 -> PG 조회/콜백 전체 파이프라인 시나리오 테스트.
 * 실제 MySQL/Redis 필요 (docker compose up -d). 스케줄러는 꺼 두고 각 단계를 직접 호출해 순서를 통제한다.
 * 전용 스트림/DLQ 키를 쓰며, 테스트가 만든 데이터는 종료 시 삭제하고 남은 것이 없는지 검증한다.
 */
@AutoConfigureMockMvc
@SpringBootTest(properties = {
        "app.outbox.stream-key=payment:sync:stream:e2e-test",
        "app.sync.dlq-key=payment:sync:dlq:e2e-test",
        "app.sync.recovery.min-idle-ms=0",
        "app.sync.recovery.max-delivery-count=3"})
class EndToEndPaymentSyncTest {

    private static final String STREAM_KEY = "payment:sync:stream:e2e-test";
    private static final String DLQ_KEY = "payment:sync:dlq:e2e-test";
    private static final String GROUP = "payment-sync-group";

    @Autowired
    PaymentService paymentService;
    @Autowired
    OutboxPublisher outboxPublisher;
    @Autowired
    PaymentSyncConsumer consumer;
    @Autowired
    PaymentRepository paymentRepository;
    @Autowired
    OutboxEventRepository outboxEventRepository;
    @Autowired
    StringRedisTemplate redisTemplate;
    @Autowired
    MockMvc mockMvc;
    @MockitoSpyBean
    PgClient pgClient;

    private final String orderId = "e2e-" + UUID.randomUUID();

    @BeforeEach
    void setUp() {
        redisTemplate.delete(List.of(STREAM_KEY, DLQ_KEY));
        consumer.initGroup();
    }

    @AfterEach
    void cleanUpAndVerifyNothingLeft() {
        redisTemplate.delete(List.of(STREAM_KEY, DLQ_KEY));
        paymentRepository.findByOrderId(orderId).ifPresent(paymentRepository::delete);
        outboxEventRepository.deleteAll(outboxEventsOf());

        assertThat(paymentRepository.findByOrderId(orderId)).isEmpty();
        assertThat(outboxEventsOf()).isEmpty();
        assertThat(redisTemplate.hasKey(STREAM_KEY)).isFalse();
        assertThat(redisTemplate.hasKey(DLQ_KEY)).isFalse();
    }

    private List<OutboxEvent> outboxEventsOf() {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(orderId)).toList();
    }

    private PaymentStatus paymentStatus() {
        return paymentRepository.findByOrderId(orderId).orElseThrow().getStatus();
    }

    private long pendingCount() {
        return redisTemplate.opsForStream().pending(STREAM_KEY, GROUP).getTotalPendingMessages();
    }

    private long streamSize() {
        Long size = redisTemplate.opsForStream().size(STREAM_KEY);
        return size == null ? 0 : size;
    }

    private void sendCallback(String pgKey, String callbackStatus) throws Exception {
        mockMvc.perform(post("/api/payments/callback").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"" + orderId + "\",\"pgTransactionKey\":\"" + pgKey
                                + "\",\"status\":\"" + callbackStatus + "\"}"))
                .andExpect(status().isOk());
    }

    @Test
    void 시나리오1_콜백이_먼저_도착하면_컨슈머는_PG_호출없이_스킵하고_XACK한다() throws Exception {
        String pgKey = "pg-ok-" + UUID.randomUUID();
        paymentService.registerPayment(orderId, pgKey, 10000);
        outboxPublisher.publishPendingEvents();
        assertThat(streamSize()).isEqualTo(1);
        assertThat(outboxEventsOf()).extracting(OutboxEvent::getStatus).containsExactly(OutboxStatus.PUBLISHED);

        sendCallback(pgKey, "COMPLETED");
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.COMPLETED);

        consumer.consume();

        verify(pgClient, never()).getPaymentStatus(any());
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
    }

    @Test
    void 시나리오2_콜백이_유실돼도_컨슈머의_PG_조회로_COMPLETED로_복구된다() {
        String pgKey = "pg-ok-" + UUID.randomUUID();
        paymentService.registerPayment(orderId, pgKey, 10000);
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.PENDING); // 콜백 미수신

        outboxPublisher.publishPendingEvents();
        consumer.consume();

        verify(pgClient, times(1)).getPaymentStatus(pgKey);
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
    }

    @Test
    void 시나리오3_PG_일시_오류는_PEL에_남았다가_복구_재시도로_COMPLETED된다() {
        String pgKey = "pg-ok-" + UUID.randomUUID();
        // 1회 실패 후 2회째부터 정상 응답
        doThrow(new PgClientException("temporary")).doCallRealMethod().when(pgClient).getPaymentStatus(any());
        paymentService.registerPayment(orderId, pgKey, 10000);
        outboxPublisher.publishPendingEvents();

        consumer.consume();
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(pendingCount()).isEqualTo(1);

        consumer.recoverPendingMessages();

        verify(pgClient, times(2)).getPaymentStatus(pgKey);
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.COMPLETED);
        assertThat(pendingCount()).isZero();
        assertThat(redisTemplate.hasKey(DLQ_KEY)).isFalse();
    }

    @Test
    void 시나리오4_PG_장애가_3회_지속되면_DLQ로_격리되고_원본은_XACK된다() {
        String pgKey = "pg-error-" + UUID.randomUUID(); // Mock PG가 항상 예외
        paymentService.registerPayment(orderId, pgKey, 10000);
        outboxPublisher.publishPendingEvents();

        consumer.consume();                 // 1회차
        consumer.recoverPendingMessages();  // 2회차
        assertThat(pendingCount()).isEqualTo(1);
        consumer.recoverPendingMessages();  // 3회차 -> 한도 도달, DLQ 이관

        verify(pgClient, times(3)).getPaymentStatus(pgKey);
        assertThat(pendingCount()).isZero();
        assertThat(paymentStatus()).isEqualTo(PaymentStatus.PENDING);

        List<MapRecord<String, Object, Object>> dlq = redisTemplate.opsForStream().range(DLQ_KEY, Range.unbounded());
        assertThat(dlq).hasSize(1);
        Map<Object, Object> fields = dlq.get(0).getValue();
        assertThat(fields.get("aggregateId")).isEqualTo(orderId);
        assertThat(fields.get("originalStreamKey")).isEqualTo(STREAM_KEY);
        assertThat(fields.get("deliveryCount")).isEqualTo("3");
        assertThat(String.valueOf(fields.get("failureReason"))).contains("PG lookup failed");
    }
}
