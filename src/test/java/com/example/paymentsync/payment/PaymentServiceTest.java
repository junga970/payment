package com.example.paymentsync.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import com.example.paymentsync.outbox.OutboxEvent;
import com.example.paymentsync.outbox.OutboxEventRepository;
import com.example.paymentsync.outbox.OutboxStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

// 트랜잭션 커밋/롤백을 실제로 검증해야 하므로 @Transactional을 붙이지 않는다 (실제 MySQL 필요).
@SpringBootTest
class PaymentServiceTest {

    @Autowired
    PaymentService paymentService;
    @Autowired
    PaymentRepository paymentRepository;
    @MockitoSpyBean
    OutboxEventRepository outboxEventRepository;

    private final String orderId = "test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        paymentRepository.findByOrderId(orderId).ifPresent(paymentRepository::delete);
        outboxEventRepository.deleteAll(outboxEventsOf(orderId));
    }

    private List<OutboxEvent> outboxEventsOf(String aggregateId) {
        return outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(aggregateId))
                .toList();
    }

    @Test
    void 결제_등록시_Payment와_OutboxEvent가_각각_1건씩_저장된다() {
        Payment payment = paymentService.registerPayment(orderId, "pg-key-1", 15000);

        assertThat(paymentRepository.findByOrderId(orderId)).isPresent();
        assertThat(paymentRepository.findByOrderId(orderId).get().getStatus()).isEqualTo(PaymentStatus.PENDING);

        List<OutboxEvent> events = outboxEventsOf(orderId);
        assertThat(events).hasSize(1);
        OutboxEvent event = events.get(0);
        assertThat(event.getAggregateType()).isEqualTo("PAYMENT");
        assertThat(event.getEventType()).isEqualTo("PAYMENT_SYNC_REQUESTED");
        assertThat(event.getStatus()).isEqualTo(OutboxStatus.INIT);
        assertThat(event.getPayload())
                .isEqualTo("{\"paymentId\":" + payment.getId() + ",\"orderId\":\"" + orderId
                        + "\",\"pgTransactionKey\":\"pg-key-1\",\"amount\":15000}");
    }

    @Test
    void Outbox_저장에서_예외가_발생하면_Payment도_함께_롤백된다() {
        doThrow(new RuntimeException("outbox save failed"))
                .when(outboxEventRepository).save(any(OutboxEvent.class));

        assertThatThrownBy(() -> paymentService.registerPayment(orderId, "pg-key-1", 15000))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("outbox save failed");

        assertThat(paymentRepository.findByOrderId(orderId)).isEmpty();
        assertThat(outboxEventsOf(orderId)).isEmpty();
    }

    @Test
    void 동일_orderId로_재요청하면_예외가_발생하고_추가_저장되지_않는다() {
        paymentService.registerPayment(orderId, "pg-key-1", 15000);

        assertThatThrownBy(() -> paymentService.registerPayment(orderId, "pg-key-2", 99999))
                .isInstanceOf(DuplicatePaymentException.class);

        assertThat(paymentRepository.findByOrderId(orderId).get().getPgTransactionKey()).isEqualTo("pg-key-1");
        assertThat(outboxEventsOf(orderId)).hasSize(1);
    }
}
