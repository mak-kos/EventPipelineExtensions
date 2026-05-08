package com.example.eventapi.event;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record EventResponse(
        UUID id,
        JsonNode payload,
        String type,
        String status,
        Instant createdAt
) {
}
