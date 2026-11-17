package com.example.paymentsync.payment;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class PaymentControllerTest {

    @Autowired
    MockMvc mockMvc;
    @Autowired
    PaymentRepository paymentRepository;
    @Autowired
    com.example.paymentsync.outbox.OutboxEventRepository outboxEventRepository;

    private final String orderId = "test-" + UUID.randomUUID();

    @AfterEach
    void cleanUp() {
        paymentRepository.findByOrderId(orderId).ifPresent(paymentRepository::delete);
        outboxEventRepository.deleteAll(outboxEventRepository.findAll().stream()
                .filter(e -> e.getAggregateId().equals(orderId)).toList());
    }

    private String body(String orderId, long amount) {
        return "{\"orderId\":\"" + orderId + "\",\"pgTransactionKey\":\"pg-key\",\"amount\":" + amount + "}";
    }

    @Test
    void 등록하면_201과_PENDING_응답() throws Exception {
        mockMvc.perform(post("/api/payments").contentType(MediaType.APPLICATION_JSON).content(body(orderId, 1000)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value(orderId))
                .andExpect(jsonPath("$.status").value("PENDING"));
    }

    @Test
    void 중복_orderId는_409() throws Exception {
        mockMvc.perform(post("/api/payments").contentType(MediaType.APPLICATION_JSON).content(body(orderId, 1000)))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/api/payments").contentType(MediaType.APPLICATION_JSON).content(body(orderId, 1000)))
                .andExpect(status().isConflict());
    }

    @Test
    void 금액이_0이하면_400() throws Exception {
        mockMvc.perform(post("/api/payments").contentType(MediaType.APPLICATION_JSON).content(body(orderId, 0)))
                .andExpect(status().isBadRequest());
    }
}
