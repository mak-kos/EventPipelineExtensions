package com.example.eventapi.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@WithMockUser(roles = "USER")
class EventApiIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16").withReuse(true);

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
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void postEvent_persistsAndReturnsId() throws Exception {
        String body = "{\"type\":\"user.signup\",\"value\":42}";

        mockMvc.perform(post("/events").contentType("application/json").content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.status").value("accepted"));

        assertThat(repository.count()).isEqualTo(1);
        Event saved = repository.findAll().get(0);
        assertThat(saved.getType()).isEqualTo("user.signup");
        assertThat(saved.getStatus()).isEqualTo("RECEIVED");
        assertThat(saved.getCreatedAt()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    void getById_whenExisting_returnsParsedPayload() throws Exception {
        UUID id = postAndGetId("{\"type\":\"user.signup\",\"value\":42,\"nested\":{\"a\":1}}");

        mockMvc.perform(get("/events/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.type").value("user.signup"))
                .andExpect(jsonPath("$.status").value("RECEIVED"))
                .andExpect(jsonPath("$.payload.value").value(42))
                .andExpect(jsonPath("$.payload.nested.a").value(1))
                .andExpect(jsonPath("$.createdAt").isString());
    }

    @Test
    void getById_whenMissing_returns404WithErrorBody() throws Exception {
        UUID missing = UUID.randomUUID();

        mockMvc.perform(get("/events/{id}", missing))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("Event not found: " + missing));
    }

    @Test
    void getById_whenIdNotUuid_returns400() throws Exception {
        mockMvc.perform(get("/events/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void list_default_paginatesWith20PerPage() throws Exception {
        for (int i = 0; i < 25; i++) {
            postAndGetId("{\"type\":\"t" + i + "\"}");
        }

        mockMvc.perform(get("/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(25))
                .andExpect(jsonPath("$.size").value(20))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.content.length()").value(20));
    }

    @Test
    void list_filterByType_returnsOnlyMatching() throws Exception {
        postAndGetId("{\"type\":\"a\"}");
        postAndGetId("{\"type\":\"b\"}");
        postAndGetId("{\"type\":\"a\"}");

        mockMvc.perform(get("/events").param("type", "a"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2))
                .andExpect(jsonPath("$.content[0].type").value("a"))
                .andExpect(jsonPath("$.content[1].type").value("a"));
    }

    @Test
    void list_filterByDateRange_returnsOnlyMatching() throws Exception {
        Instant baseline = Instant.now().minus(1, ChronoUnit.HOURS);
        UUID old = postAndGetId("{\"type\":\"x\"}");
        // Backdate one event directly via repository to make the range filter meaningful
        Event oldEvent = repository.findById(old).orElseThrow();
        oldEvent.setCreatedAt(baseline.minus(2, ChronoUnit.HOURS));
        repository.save(oldEvent);

        UUID recent = postAndGetId("{\"type\":\"x\"}");

        mockMvc.perform(get("/events")
                        .param("from", baseline.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.content[0].id").value(recent.toString()));
    }

    @Test
    void list_sizeOverMax_isClampedToMax() throws Exception {
        for (int i = 0; i < 3; i++) {
            postAndGetId("{\"type\":\"a\"}");
        }

        mockMvc.perform(get("/events").param("size", "9999"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(100));
    }

    @Test
    void list_invalidFromAfterTo_returns400() throws Exception {
        mockMvc.perform(get("/events")
                        .param("from", "2025-01-02T00:00:00Z")
                        .param("to", "2025-01-01T00:00:00Z"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void list_negativePage_returns400() throws Exception {
        mockMvc.perform(get("/events").param("page", "-1"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void list_zeroSize_returns400() throws Exception {
        mockMvc.perform(get("/events").param("size", "0"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void list_payloadIsParsedJsonNotString() throws Exception {
        postAndGetId("{\"type\":\"a\",\"meta\":{\"k\":\"v\"}}");

        mockMvc.perform(get("/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].payload").isMap())
                .andExpect(jsonPath("$.content[0].payload.meta.k").value("v"));
    }

    @Test
    void createdAt_isIso8601Utc() throws Exception {
        UUID id = postAndGetId("{\"type\":\"a\"}");

        String response = mockMvc.perform(get("/events/{id}", id))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode node = mapper.readTree(response);
        String createdAt = node.get("createdAt").asText();

        assertThat(createdAt).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z");
    }

    private UUID postAndGetId(String body) throws Exception {
        String response = mockMvc.perform(post("/events").contentType("application/json").content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(mapper.readTree(response).get("id").asText());
    }
}
