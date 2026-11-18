package com.example.paymentsync.pg;

import org.springframework.stereotype.Component;

/**
 * 로컬/테스트용 Mock PG.
 * - 키에 "error" 포함: PgClientException (조회 실패 시뮬레이션)
 * - 키에 "fail" 포함: FAILED
 * - 그 외: COMPLETED
 */
@Component
public class MockPgClient implements PgClient {

    @Override
    public PgPaymentStatus getPaymentStatus(String pgTransactionKey) {
        if (pgTransactionKey.contains("error")) {
            throw new PgClientException("PG lookup failed: " + pgTransactionKey);
        }
        if (pgTransactionKey.contains("fail")) {
            return PgPaymentStatus.FAILED;
        }
        return PgPaymentStatus.COMPLETED;
    }
}
