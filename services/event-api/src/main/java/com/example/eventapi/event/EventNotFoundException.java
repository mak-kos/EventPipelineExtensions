package com.example.eventapi.event;

import java.util.UUID;

public class EventNotFoundException extends RuntimeException {

    private final UUID id;

    public EventNotFoundException(UUID id) {
        super("Event not found: " + id);
        this.id = id;
    }

    public UUID getId() {
        return id;
    }
}
