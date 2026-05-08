package com.example.eventapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/events")
public class EventController {

    private static final String TOPIC = "events";

    private final EventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public EventController(EventRepository repository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body)
            throws JsonProcessingException {

        UUID id = UUID.randomUUID();
        String payload = objectMapper.writeValueAsString(body);

        Event event = new Event();
        event.setId(id);
        event.setPayload(payload);
        event.setStatus("RECEIVED");
        event.setCreatedAt(Instant.now());
        repository.save(event);

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("id", id.toString());
        message.put("payload", body);
        kafkaTemplate.send(TOPIC, id.toString(), objectMapper.writeValueAsString(message));

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", id.toString());
        response.put("status", "accepted");
        return ResponseEntity.ok(response);
    }
}
