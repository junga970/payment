package com.example.paymentsync.sync;

import com.example.paymentsync.payment.Payment;
import com.example.paymentsync.payment.PaymentRepository;
import com.example.paymentsync.payment.PaymentService;
import com.example.paymentsync.payment.PaymentStatus;
import com.example.paymentsync.pg.PgClient;
import com.example.paymentsync.pg.PgPaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Redis Stream 컨슈머 그룹으로 결제 동기화 메시지를 읽어 PG 상태를 Payment에 반영한다.
 * - DB 반영이 끝난 뒤에만 XACK 한다.
 * - 이미 확정(COMPLETED/FAILED)된 결제의 중복 메시지는 PG 호출 없이 스킵하고 XACK 한다.
 * - 처리 실패 메시지는 ACK 없이 PEL에 남기고, recoverPendingMessages()가 XCLAIM으로 회수해 재처리한다.
 * - 전달 횟수가 한도에 도달하면 DLQ 스트림으로 옮기고 원본은 XACK 한다.
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
    private final String dlqKey;
    private final boolean recoveryEnabled;
    private final Duration recoveryMinIdle;
    private final int maxDeliveryCount;

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
                               @Value("${app.sync.consumer.enabled}") boolean enabled,
                               @Value("${app.sync.dlq-key}") String dlqKey,
                               @Value("${app.sync.recovery.enabled}") boolean recoveryEnabled,
                               @Value("${app.sync.recovery.min-idle-ms}") long recoveryMinIdleMs,
                               @Value("${app.sync.recovery.max-delivery-count}") int maxDeliveryCount) {
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
        this.dlqKey = dlqKey;
        this.recoveryEnabled = recoveryEnabled;
        this.recoveryMinIdle = Duration.ofMillis(recoveryMinIdleMs);
        this.maxDeliveryCount = maxDeliveryCount;
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

    @Scheduled(fixedDelayString = "${app.sync.recovery.poll-delay-ms}")
    void scheduledRecover() {
        if (!recoveryEnabled) {
            return;
        }
        try {
            if (!groupReady) {
                initGroup();
            }
            recoverPendingMessages();
        } catch (Exception e) {
            log.warn("Payment sync recovery failed", e);
        }
    }

    /**
     * PEL에서 minIdle 이상 머문 메시지를 회수(XCLAIM)해 재처리한다.
     * 전달 횟수가 maxDeliveryCount에 도달한 메시지는 재시도하지 않고 DLQ로 보낸다.
     */
    public void recoverPendingMessages() {
        PendingMessages pending = redisTemplate.opsForStream().pending(streamKey, group, Range.unbounded(), batchSize);
        if (pending == null) {
            return;
        }
        for (PendingMessage message : pending) {
            if (message.getElapsedTimeSinceLastDelivery().compareTo(recoveryMinIdle) < 0) {
                continue;
            }
            RecordId id = message.getId();
            long deliveryCount = message.getTotalDeliveryCount();
            try {
                if (deliveryCount >= maxDeliveryCount) {
                    List<MapRecord<String, Object, Object>> found =
                            redisTemplate.opsForStream().range(streamKey, Range.closed(id.getValue(), id.getValue()));
                    if (found != null && !found.isEmpty()) {
                        moveToDlq(found.get(0), "max delivery count exceeded (" + deliveryCount + ")", deliveryCount);
                    } else {
                        // 이미 스트림에서 삭제된 메시지: PEL만 정리
                        redisTemplate.opsForStream().acknowledge(streamKey, group, id);
                    }
                    continue;
                }
                List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream()
                        .claim(streamKey, group, consumerName, recoveryMinIdle, id);
                if (claimed == null || claimed.isEmpty()) {
                    continue; // 다른 컨슈머가 먼저 회수했거나 이미 ACK 됨
                }
                MapRecord<String, Object, Object> record = claimed.get(0);
                try {
                    process(record);
                    redisTemplate.opsForStream().acknowledge(streamKey, group, id);
                } catch (Exception e) {
                    log.warn("Payment sync retry failed: recordId={}, deliveryCount={}", id, deliveryCount + 1, e);
                    if (deliveryCount + 1 >= maxDeliveryCount) {
                        moveToDlq(record, e.toString(), deliveryCount + 1);
                    }
                }
            } catch (Exception e) {
                log.warn("Recovery of pending message failed: recordId={}", id, e);
            }
        }
    }

    /** DLQ 스트림에 원본 데이터와 실패 사유를 기록한 뒤 원본 스트림에서 XACK 한다. */
    private void moveToDlq(MapRecord<String, Object, Object> record, String reason, long deliveryCount) {
        Map<String, String> fields = new LinkedHashMap<>();
        record.getValue().forEach((k, v) -> fields.put(String.valueOf(k), String.valueOf(v)));
        fields.put("originalStreamKey", streamKey);
        fields.put("originalRecordId", record.getId().getValue());
        fields.put("deliveryCount", String.valueOf(deliveryCount));
        fields.put("failureReason", reason);
        redisTemplate.opsForStream().add(StreamRecords.string(fields).withStreamKey(dlqKey));
        redisTemplate.opsForStream().acknowledge(streamKey, group, record.getId());
        log.error("Moved to DLQ: recordId={}, reason={}", record.getId(), reason);
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

        // 멱등성: 이미 확정된 결제는 PG 재호출 없이 스킵 (호출 측에서 XACK)
        if (payment.getStatus() != PaymentStatus.PENDING) {
            log.info("Already processed, skipping: orderId={}, status={}", orderId, payment.getStatus());
            return;
        }

        // PG 호출은 DB 트랜잭션 밖에서 수행한다.
        PgPaymentStatus pgStatus = pgClient.getPaymentStatus(payment.getPgTransactionKey());
        paymentService.applyPgResult(orderId, pgStatus);
    }
}
