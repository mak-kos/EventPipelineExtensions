package com.example.eventapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.crypto.SecretKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
class SecurityIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16").withReuse(true);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private JwtTokenProvider tokens;

    @Autowired
    private EventRepository repository;

    @Value("${app.jwt.secret}")
    private String secret;

    @MockBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void getEvents_withoutToken_returns401() throws Exception {
        mockMvc.perform(get("/events"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401));
    }

    @Test
    void getStats_withoutToken_returns401() throws Exception {
        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getEvents_withMalformedToken_returns401() throws Exception {
        mockMvc.perform(get("/events").header("Authorization", "Bearer not-a-real-jwt"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getEvents_withExpiredToken_returns401() throws Exception {
        String expired = manuallyIssue("user", "USER",
                Instant.now().minus(2, ChronoUnit.HOURS),
                Instant.now().minus(1, ChronoUnit.HOURS));

        mockMvc.perform(get("/events").header("Authorization", "Bearer " + expired))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getEvents_withTokenSignedByWrongKey_returns401() throws Exception {
        SecretKey wrong = Keys.hmacShaKeyFor("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA0=".getBytes());
        String foreign = Jwts.builder()
                .subject("user")
                .claim("role", "USER")
                .expiration(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
                .signWith(wrong, Jwts.SIG.HS256)
                .compact();

        mockMvc.perform(get("/events").header("Authorization", "Bearer " + foreign))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void getEvents_withUserToken_returns200() throws Exception {
        String token = tokens.issue("user", "USER").token();

        mockMvc.perform(get("/events").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void getStats_withUserToken_returns200() throws Exception {
        String token = tokens.issue("user", "USER").token();

        mockMvc.perform(get("/stats/summary").header("Authorization", "Bearer " + token))
                .andExpect(status().isOk());
    }

    @Test
    void deleteEvent_withUserToken_returns403() throws Exception {
        String token = tokens.issue("user", "USER").token();
        UUID id = UUID.randomUUID();

        mockMvc.perform(delete("/events/{id}", id).header("Authorization", "Bearer " + token))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403));
    }

    @Test
    void deleteEvent_withAdminToken_existing_returns204() throws Exception {
        Event e = new Event();
        e.setId(UUID.randomUUID());
        e.setPayload("{\"type\":\"x\"}");
        e.setType("x");
        e.setStatus("RECEIVED");
        e.setCreatedAt(Instant.now());
        repository.save(e);

        String token = tokens.issue("admin", "ADMIN").token();

        mockMvc.perform(delete("/events/{id}", e.getId()).header("Authorization", "Bearer " + token))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteEvent_withAdminToken_missing_returns404() throws Exception {
        String token = tokens.issue("admin", "ADMIN").token();
        UUID missing = UUID.randomUUID();

        mockMvc.perform(delete("/events/{id}", missing).header("Authorization", "Bearer " + token))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void postEvent_withoutToken_remainsOpen_returns200() throws Exception {
        mockMvc.perform(post("/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"open\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("accepted"));
    }

    private String manuallyIssue(String username, String role, Instant issuedAt, Instant expiresAt) {
        SecretKey key = Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
        return Jwts.builder()
                .subject(username)
                .claim("role", role)
                .issuedAt(Date.from(issuedAt))
                .expiration(Date.from(expiresAt))
                .signWith(key, Jwts.SIG.HS256)
                .compact();
    }
}
