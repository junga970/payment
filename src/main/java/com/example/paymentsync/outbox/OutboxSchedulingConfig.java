package com.example.paymentsync.outbox;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** app.outbox.poller.enabled=false 이면 @Scheduled 가 동작하지 않는다. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "app.outbox.poller.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxSchedulingConfig {
}
