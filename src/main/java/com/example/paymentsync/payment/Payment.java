package com.example.paymentsync.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.LocalDateTime;

@Entity
@Table(name = "payment",
        uniqueConstraints = @UniqueConstraint(name = "uk_payment_order_id", columnNames = "order_id"))
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, length = 64)
    private String orderId;

    @Column(name = "pg_transaction_key", nullable = false, length = 128)
    private String pgTransactionKey;

    @Column(nullable = false)
    private long amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    protected Payment() {
    }

    public Payment(String orderId, String pgTransactionKey, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        this.orderId = orderId;
        this.pgTransactionKey = pgTransactionKey;
        this.amount = amount;
        this.status = PaymentStatus.PENDING;
    }

    /** PENDING 상태에서만 완료 처리할 수 있다. 이미 COMPLETED/FAILED면 예외. */
    public void complete() {
        requirePending(PaymentStatus.COMPLETED);
        this.status = PaymentStatus.COMPLETED;
    }

    /** PENDING 상태에서만 실패 처리할 수 있다. 이미 COMPLETED/FAILED면 예외. */
    public void fail() {
        requirePending(PaymentStatus.FAILED);
        this.status = PaymentStatus.FAILED;
    }

    private void requirePending(PaymentStatus target) {
        if (status != PaymentStatus.PENDING) {
            throw new IllegalStateException(
                    "Cannot change payment status " + status + " -> " + target + " (orderId=" + orderId + ")");
        }
    }

    @PrePersist
    void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getPgTransactionKey() { return pgTransactionKey; }
    public long getAmount() { return amount; }
    public PaymentStatus getStatus() { return status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
}
