package com.example.eventapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@WithMockUser(roles = "USER")
class StatsControllerIntegrationTest {

    private static final Instant FIXED_NOW = Instant.parse("2026-01-15T12:00:00Z");

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16").withReuse(true);

    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(FIXED_NOW, ZoneOffset.UTC);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventRepository repository;

    @Autowired
    private ObjectMapper mapper;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        org.mockito.Mockito.when(kafkaTemplate.send(org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void summary_emptyDatabase_returnsAllZerosAndEmptyCollections() throws Exception {
        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount").value(0))
                .andExpect(jsonPath("$.countByType").isMap())
                .andExpect(jsonPath("$.countByType.*").isEmpty())
                .andExpect(jsonPath("$.countLast24Hours").value(0))
                .andExpect(jsonPath("$.top5TypesLast7Days").isArray())
                .andExpect(jsonPath("$.top5TypesLast7Days.length()").value(0));
    }

    @Test
    void summary_groupsByTypeAndCountsTotal() throws Exception {
        insert("a", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        insert("a", FIXED_NOW.minus(2, ChronoUnit.HOURS));
        insert("b", FIXED_NOW.minus(3, ChronoUnit.HOURS));

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount").value(3))
                .andExpect(jsonPath("$.countByType.a").value(2))
                .andExpect(jsonPath("$.countByType.b").value(1));
    }

    @Test
    void summary_24hWindowExcludesOlderEvents() throws Exception {
        insert("a", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        insert("a", FIXED_NOW.minus(23, ChronoUnit.HOURS));
        // exactly 24h ago is included by >= boundary
        insert("a", FIXED_NOW.minus(24, ChronoUnit.HOURS));
        // strictly older is excluded
        insert("a", FIXED_NOW.minus(25, ChronoUnit.HOURS));
        insert("a", FIXED_NOW.minus(48, ChronoUnit.HOURS));

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount").value(5))
                .andExpect(jsonPath("$.countLast24Hours").value(3));
    }

    @Test
    void summary_top5_returnsAtMostFiveOrderedByCount() throws Exception {
        // 6 distinct types, varying counts (within 7d window)
        for (int i = 0; i < 6; i++) {
            insert("t1", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        }
        for (int i = 0; i < 5; i++) {
            insert("t2", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        }
        for (int i = 0; i < 4; i++) {
            insert("t3", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        }
        for (int i = 0; i < 3; i++) {
            insert("t4", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        }
        for (int i = 0; i < 2; i++) {
            insert("t5", FIXED_NOW.minus(1, ChronoUnit.HOURS));
        }
        insert("t6", FIXED_NOW.minus(1, ChronoUnit.HOURS));

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.top5TypesLast7Days.length()").value(5))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].type").value("t1"))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].count").value(6))
                .andExpect(jsonPath("$.top5TypesLast7Days[4].type").value("t5"))
                .andExpect(jsonPath("$.top5TypesLast7Days[4].count").value(2));
    }

    @Test
    void summary_top5_ignoresEventsOlderThan7Days() throws Exception {
        // type "old" has many events but all older than 7d -> must not appear in top5
        for (int i = 0; i < 10; i++) {
            insert("old", FIXED_NOW.minus(8, ChronoUnit.DAYS));
        }
        insert("new", FIXED_NOW.minus(1, ChronoUnit.DAYS));

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.top5TypesLast7Days.length()").value(1))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].type").value("new"));
    }

    @Test
    void summary_eventsWithNullType_areGroupedAsUnknown() throws Exception {
        insert(null, FIXED_NOW.minus(1, ChronoUnit.HOURS));
        insert(null, FIXED_NOW.minus(2, ChronoUnit.HOURS));
        insert("a", FIXED_NOW.minus(1, ChronoUnit.HOURS));

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.countByType.unknown").value(2))
                .andExpect(jsonPath("$.countByType.a").value(1))
                .andExpect(jsonPath("$.top5TypesLast7Days[?(@.type == 'unknown')].count")
                        .value(org.hamcrest.Matchers.contains(2)));
    }

    private void insert(String type, Instant createdAt) {
        Event e = new Event();
        e.setId(UUID.randomUUID());
        e.setPayload(type == null ? "{}" : "{\"type\":\"" + type + "\"}");
        e.setType(type);
        e.setStatus("RECEIVED");
        e.setCreatedAt(createdAt);
        repository.save(e);
    }
}
