package com.example.paymentsync.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class OutboxEventTest {

    @Autowired
    OutboxEventRepository outboxEventRepository;

    private OutboxEvent newEvent() {
        return new OutboxEvent("PAYMENT", "order-1", "PAYMENT_SYNC_REQUESTED", "{\"orderId\":\"order-1\"}");
    }

    @Test
    void 저장하면_INIT_상태_retryCount_0() {
        OutboxEvent saved = outboxEventRepository.saveAndFlush(newEvent());

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(saved.getRetryCount()).isZero();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getPublishedAt()).isNull();
    }

    @Test
    void markPublished는_PUBLISHED와_발행시각을_기록하고_중복호출은_예외() {
        OutboxEvent event = newEvent();
        event.markPublished();

        assertThat(event.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        assertThat(event.getPublishedAt()).isNotNull();
        assertThatThrownBy(event::markPublished).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void recordFailure는_최대횟수_전까지_INIT_유지_도달하면_FAILED() {
        OutboxEvent event = newEvent();

        event.recordFailure(3);
        event.recordFailure(3);
        assertThat(event.getRetryCount()).isEqualTo(2);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.INIT);

        event.recordFailure(3);
        assertThat(event.getRetryCount()).isEqualTo(3);
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThatThrownBy(() -> event.recordFailure(3)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void INIT_이벤트만_생성순으로_조회된다() throws Exception {
        OutboxEvent first = outboxEventRepository.saveAndFlush(newEvent());
        Thread.sleep(5);
        OutboxEvent published = outboxEventRepository.saveAndFlush(newEvent());
        published.markPublished();
        Thread.sleep(5);
        OutboxEvent second = outboxEventRepository.saveAndFlush(newEvent());
        outboxEventRepository.flush();

        List<OutboxEvent> result = outboxEventRepository.findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus.INIT);

        assertThat(result).extracting(OutboxEvent::getId).containsSubsequence(first.getId(), second.getId());
        assertThat(result).extracting(OutboxEvent::getId).doesNotContain(published.getId());
    }
}
