package com.example.paymentsync.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;

// 실제 MySQL 사용 (docker compose up -d 필요). 각 테스트는 트랜잭션 롤백으로 데이터가 남지 않는다.
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class PaymentTest {

    @Autowired
    PaymentRepository paymentRepository;

    @Test
    void 저장하면_PENDING_상태와_생성시각이_채워진다() {
        Payment saved = paymentRepository.saveAndFlush(new Payment("order-1", "pg-key-1", 1000));

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();
        assertThat(paymentRepository.findByOrderId("order-1")).isPresent();
    }

    @Test
    void orderId가_중복되면_유니크_제약조건_위반() {
        paymentRepository.saveAndFlush(new Payment("order-dup", "pg-key-1", 1000));

        assertThatThrownBy(() -> paymentRepository.saveAndFlush(new Payment("order-dup", "pg-key-2", 2000)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void complete는_PENDING에서_COMPLETED로_전이된다() {
        Payment payment = new Payment("order-2", "pg-key", 1000);
        payment.complete();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void 이미_COMPLETED면_complete_fail_모두_예외() {
        Payment payment = new Payment("order-3", "pg-key", 1000);
        payment.complete();

        assertThatThrownBy(payment::complete).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(payment::fail).isInstanceOf(IllegalStateException.class);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void fail은_PENDING에서_FAILED로_전이되고_이후_변경은_예외() {
        Payment payment = new Payment("order-4", "pg-key", 1000);
        payment.fail();

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThatThrownBy(payment::complete).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void 금액이_0이하면_생성_불가() {
        assertThatThrownBy(() -> new Payment("order-5", "pg-key", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
