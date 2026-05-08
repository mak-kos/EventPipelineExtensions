package com.example.eventapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    static final int DEFAULT_PAGE_SIZE = 20;
    static final int MAX_PAGE_SIZE = 100;
    static final String STATUS_RECEIVED = "RECEIVED";
    static final String KAFKA_TOPIC = "events";

    private final EventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int maxPageSize;

    public EventService(EventRepository repository,
                        KafkaTemplate<String, String> kafkaTemplate,
                        ObjectMapper objectMapper,
                        @Value("${app.events.max-page-size:" + MAX_PAGE_SIZE + "}") int maxPageSize) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.maxPageSize = maxPageSize > 0 ? maxPageSize : MAX_PAGE_SIZE;
    }

    /**
     * Persists the event then publishes to Kafka. Persistence runs in {@code save}'s
     * own short transaction; the Kafka publish happens AFTER commit so an external-I/O
     * failure cannot leave a half-committed transaction. If Kafka publishing fails
     * the event still exists in Postgres and downstream replay (Python track) can
     * re-publish it.
     */
    public UUID createEvent(Map<String, Object> body) {
        UUID id = UUID.randomUUID();
        String serialisedPayload;
        String envelope;
        try {
            serialisedPayload = objectMapper.writeValueAsString(body == null ? Map.of() : body);

            Map<String, Object> message = new LinkedHashMap<>();
            message.put("id", id.toString());
            message.put("payload", body == null ? Map.of() : body);
            envelope = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid JSON payload", ex);
        }

        Event event = new Event();
        event.setId(id);
        event.setPayload(serialisedPayload);
        event.setType(extractType(body));
        event.setStatus(STATUS_RECEIVED);
        event.setCreatedAt(Instant.now());
        repository.save(event);

        kafkaTemplate.send(KAFKA_TOPIC, id.toString(), envelope)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        log.error("Kafka publish failed for event {} ", id, ex);
                    }
                });

        return id;
    }

    @Transactional(readOnly = true)
    public EventResponse findById(UUID id) {
        Event event = repository.findById(id).orElseThrow(() -> new EventNotFoundException(id));
        return toResponse(event);
    }

    @Transactional
    public void deleteById(UUID id) {
        if (!repository.existsById(id)) {
            throw new EventNotFoundException(id);
        }
        repository.deleteById(id);
    }

    @Transactional(readOnly = true)
    public PageResponse<EventResponse> findAll(String type, Instant from, Instant to,
                                               int page, int size) {
        if (page < 0) {
            throw new IllegalArgumentException("page must be >= 0");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("'from' must be <= 'to'");
        }

        int effectiveSize = Math.min(size, maxPageSize);
        // Stable ordering: secondary sort by id breaks ties on identical createdAt timestamps,
        // preventing duplicate/skipped rows across paginated requests.
        Pageable pageable = PageRequest.of(page, effectiveSize,
                Sort.by(Sort.Order.desc("createdAt"), Sort.Order.asc("id")));

        Specification<Event> spec = Specification.where(EventSpecifications.hasType(type))
                .and(EventSpecifications.createdAtFrom(from))
                .and(EventSpecifications.createdAtTo(to));

        Page<EventResponse> mapped = repository.findAll(spec, pageable).map(this::toResponse);
        return PageResponse.of(mapped);
    }

    private EventResponse toResponse(Event event) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(event.getPayload());
        } catch (JsonProcessingException ex) {
            log.error("Stored payload is not valid JSON for event {}", event.getId(), ex);
            throw new IllegalStateException("Corrupted payload for event " + event.getId(), ex);
        }
        return new EventResponse(
                event.getId(),
                payload,
                event.getType(),
                event.getStatus(),
                event.getCreatedAt()
        );
    }

    private static String extractType(Map<String, Object> body) {
        if (body == null) {
            return null;
        }
        Object value = body.get("type");
        if (value == null) {
            return null;
        }
        String s = value.toString();
        return s.length() > 255 ? s.substring(0, 255) : s;
    }
}
