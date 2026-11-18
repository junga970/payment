package com.example.paymentsync;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 개별 스케줄러(Outbox Poller, Consumer)의 활성화 여부는 각 컴포넌트의 enabled 프로퍼티로 제어한다. */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
