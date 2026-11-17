package com.example.paymentsync.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// 실제 MySQL/Redis 필요 (docker compose up -d). 테스트 전용 스트림 키를 사용해 개발용 스트림과 분리한다.
@SpringBootTest(properties = "app.outbox.stream-key=payment:sync:stream:test")
class OutboxPublisherTest {

    private static final String STREAM_KEY = "payment:sync:stream:test";

    @Autowired
    OutboxPublisher outboxPublisher;
    @Autowired
    OutboxEventRepository outboxEventRepository;
    @MockitoSpyBean
    StringRedisTemplate redisTemplate;

    private final String aggregateId = "test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        redisTemplate.delete(STREAM_KEY);
        outboxEventRepository.deleteAll(eventsOfTest());
    }

    private List<OutboxEvent> eventsOfTest() {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().startsWith(aggregateId))
                .toList();
    }

    private OutboxEvent saveEvent(String suffix) {
        return outboxEventRepository.saveAndFlush(new OutboxEvent(
                "PAYMENT", aggregateId + suffix, "PAYMENT_SYNC_REQUESTED", "{\"orderId\":\"" + aggregateId + suffix + "\"}"));
    }

    private OutboxEvent reload(OutboxEvent event) {
        return outboxEventRepository.findById(event.getId()).orElseThrow();
    }

    @Test
    void INIT_이벤트가_Redis_Stream에_발행되고_PUBLISHED로_변경된다() {
        OutboxEvent event = saveEvent("");

        outboxPublisher.publishPendingEvents();

        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(STREAM_KEY, Range.unbounded());
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getValue())
                .containsEntry("eventId", String.valueOf(event.getId()))
                .containsEntry("aggregateType", "PAYMENT")
                .containsEntry("aggregateId", aggregateId)
                .containsEntry("eventType", "PAYMENT_SYNC_REQUESTED")
                .containsEntry("payload", event.getPayload());

        OutboxEvent published = reload(event);
        assertThat(published.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(published.getPublishedAt()).isNotNull();

        // 이미 발행된 건은 다시 발행되지 않는다
        outboxPublisher.publishPendingEvents();
        assertThat(redisTemplate.opsForStream().size(STREAM_KEY)).isEqualTo(1);
    }

    @Test
    void 발행_실패시_INIT_유지_retryCount_증가_3회째_FAILED() {
        OutboxEvent event = saveEvent("");
        doThrow(new RedisConnectionFailureException("redis down")).when(redisTemplate).opsForStream();

        outboxPublisher.publishPendingEvents();
        assertThat(reload(event).getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(reload(event).getRetryCount()).isEqualTo(1);

        outboxPublisher.publishPendingEvents();
        assertThat(reload(event).getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(reload(event).getRetryCount()).isEqualTo(2);

        outboxPublisher.publishPendingEvents();
        assertThat(reload(event).getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(reload(event).getRetryCount()).isEqualTo(3);
        assertThat(reload(event).getPublishedAt()).isNull();

        // FAILED 건은 더 이상 조회/발행되지 않는다
        outboxPublisher.publishPendingEvents();
        assertThat(reload(event).getRetryCount()).isEqualTo(3);
    }

    @Test
    void 한_건이_실패해도_다른_건의_발행은_계속된다() {
        OutboxEvent first = saveEvent("-1");
        OutboxEvent second = saveEvent("-2");
        // 첫 번째 XADD만 실패, 이후는 정상 동작
        doThrow(new RedisConnectionFailureException("temporary")).doCallRealMethod()
                .when(redisTemplate).opsForStream();

        outboxPublisher.publishPendingEvents();

        assertThat(reload(first).getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(reload(first).getRetryCount()).isEqualTo(1);
        assertThat(reload(second).getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(redisTemplate.opsForStream().size(STREAM_KEY)).isEqualTo(1);
    }
}
