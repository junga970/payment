package com.example.paymentsync.outbox;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * INIT 상태의 Outbox 이벤트를 오래된 순으로 읽어 Redis Stream(XADD)으로 발행한다.
 * 이벤트마다 개별로 DB에 반영하므로 한 건이 실패해도 나머지는 계속 처리한다.
 *
 * 전달 보장은 at-least-once: XADD 성공 후 DB 반영이 실패하면 다음 주기에 다시 발행될 수 있다.
 * 컨슈머는 메시지의 eventId(또는 orderId)로 멱등하게 처리해야 한다.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final StringRedisTemplate redisTemplate;
    private final String streamKey;
    private final int maxRetryCount;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           StringRedisTemplate redisTemplate,
                           @Value("${app.outbox.stream-key}") String streamKey,
                           @Value("${app.outbox.max-retry-count}") int maxRetryCount) {
        this.outboxEventRepository = outboxEventRepository;
        this.redisTemplate = redisTemplate;
        this.streamKey = streamKey;
        this.maxRetryCount = maxRetryCount;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-delay-ms}")
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus.INIT);
        for (OutboxEvent event : events) {
            try {
                redisTemplate.opsForStream().add(StreamRecords.string(toFields(event)).withStreamKey(streamKey));
                event.markPublished();
            } catch (Exception e) {
                event.recordFailure(maxRetryCount);
                log.warn("Outbox publish failed: id={}, retryCount={}, status={}",
                        event.getId(), event.getRetryCount(), event.getStatus(), e);
            }
            outboxEventRepository.save(event);
        }
    }

    private Map<String, String> toFields(OutboxEvent event) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("eventId", String.valueOf(event.getId()));
        fields.put("aggregateType", event.getAggregateType());
        fields.put("aggregateId", event.getAggregateId());
        fields.put("eventType", event.getEventType());
        fields.put("payload", event.getPayload());
        return fields;
    }
}
