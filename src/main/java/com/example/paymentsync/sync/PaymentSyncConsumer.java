package com.example.paymentsync.sync;

import com.example.paymentsync.payment.Payment;
import com.example.paymentsync.payment.PaymentRepository;
import com.example.paymentsync.payment.PaymentService;
import com.example.paymentsync.pg.PgClient;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Redis Stream 컨슈머 그룹으로 결제 동기화 메시지를 읽어 PG 상태를 Payment에 반영한다.
 * DB 반영이 끝난 뒤에만 XACK 하며, 처리 실패 메시지는 ACK 없이 PEL(Pending)에 남긴다.
 * (재시도/DLQ/중복 방어는 Step 5)
 */
@Component
public class PaymentSyncConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentSyncConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final PaymentRepository paymentRepository;
    private final PaymentService paymentService;
    private final PgClient pgClient;
    private final ObjectMapper objectMapper;
    private final String streamKey;
    private final String group;
    private final String consumerName;
    private final int batchSize;
    private final boolean enabled;

    private volatile boolean groupReady = false;

    public PaymentSyncConsumer(StringRedisTemplate redisTemplate,
                               PaymentRepository paymentRepository,
                               PaymentService paymentService,
                               PgClient pgClient,
                               ObjectMapper objectMapper,
                               @Value("${app.outbox.stream-key}") String streamKey,
                               @Value("${app.sync.consumer.group}") String group,
                               @Value("${app.sync.consumer.name}") String consumerName,
                               @Value("${app.sync.consumer.batch-size}") int batchSize,
                               @Value("${app.sync.consumer.enabled}") boolean enabled) {
        this.redisTemplate = redisTemplate;
        this.paymentRepository = paymentRepository;
        this.paymentService = paymentService;
        this.pgClient = pgClient;
        this.objectMapper = objectMapper;
        this.streamKey = streamKey;
        this.group = group;
        this.consumerName = consumerName;
        this.batchSize = batchSize;
        this.enabled = enabled;
    }

    /** 앱 시작 시 그룹 생성. Redis가 아직 안 떠 있어도 기동은 막지 않고, 폴링 시점에 다시 시도한다. */
    @EventListener(ApplicationReadyEvent.class)
    void initOnStartup() {
        if (enabled) {
            try {
                initGroup();
            } catch (Exception e) {
                log.warn("Consumer group init failed, will retry on next poll", e);
            }
        }
    }

    /** 스트림과 컨슈머 그룹이 없으면 생성(XGROUP CREATE ... MKSTREAM). 이미 있으면 무시. */
    public void initGroup() {
        try {
            redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), group);
        } catch (RedisSystemException e) {
            if (!String.valueOf(e.getMostSpecificCause().getMessage()).contains("BUSYGROUP")) {
                throw e;
            }
        }
        groupReady = true;
    }

    @Scheduled(fixedDelayString = "${app.sync.consumer.poll-delay-ms}")
    void scheduledConsume() {
        if (!enabled) {
            return;
        }
        try {
            if (!groupReady) {
                initGroup();
            }
            consume();
        } catch (Exception e) {
            log.warn("Payment sync consume failed", e);
        }
    }

    /** 새 메시지를 최대 batchSize건 읽어 처리한다. */
    public void consume() {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(group, consumerName),
                StreamReadOptions.empty().count(batchSize),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()));
        if (records == null) {
            return;
        }
        for (MapRecord<String, Object, Object> record : records) {
            try {
                process(record);
                redisTemplate.opsForStream().acknowledge(streamKey, group, record.getId());
            } catch (Exception e) {
                log.warn("Payment sync failed, leaving unacked: recordId={}", record.getId(), e);
            }
        }
    }

    private void process(MapRecord<String, Object, Object> record) throws Exception {
        String orderId = objectMapper.readTree(String.valueOf(record.getValue().get("payload")))
                .path("orderId").asText();
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException("Payment not found: orderId=" + orderId));

        // PG 호출은 DB 트랜잭션 밖에서 수행한다.
        PgPaymentStatus pgStatus = pgClient.getPaymentStatus(payment.getPgTransactionKey());
        paymentService.applyPgResult(orderId, pgStatus);
    }
}
