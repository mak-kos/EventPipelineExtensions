package com.example.eventapi.event;

import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;

/**
 * Null-safe {@link Specification} factories for {@link Event}. Each factory returns
 * a no-op specification when its input is null, so callers can compose optional
 * filters with {@code .and(...)} without explicit null checks.
 */
final class EventSpecifications {

    private EventSpecifications() {
    }

    static Specification<Event> hasType(String type) {
        if (type == null || type.isBlank()) {
            return null;
        }
        return (root, query, cb) -> cb.equal(root.get("type"), type);
    }

    static Specification<Event> createdAtFrom(Instant from) {
        if (from == null) {
            return null;
        }
        return (root, query, cb) -> cb.greaterThanOrEqualTo(root.get("createdAt"), from);
    }

    static Specification<Event> createdAtTo(Instant to) {
        if (to == null) {
            return null;
        }
        return (root, query, cb) -> cb.lessThanOrEqualTo(root.get("createdAt"), to);
    }
}
