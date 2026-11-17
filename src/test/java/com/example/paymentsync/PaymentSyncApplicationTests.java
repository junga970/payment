package com.example.paymentsync;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// 실행 전 `docker compose up -d` 필요 (MySQL, Redis 연결 확인용 스모크 테스트)
@SpringBootTest
class PaymentSyncApplicationTests {

    @Test
    void contextLoads() {
    }
}
