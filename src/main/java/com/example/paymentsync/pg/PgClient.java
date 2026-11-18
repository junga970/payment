package com.example.paymentsync.pg;

/** PG사 결제 상태 조회. 실제 PG 연동 구현체로 교체될 외부 경계라서 인터페이스로 둔다. */
public interface PgClient {

    /** @throws PgClientException PG 조회 자체가 실패한 경우 (타임아웃 등) */
    PgPaymentStatus getPaymentStatus(String pgTransactionKey);
}
