package com.example.eventapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "app")
public record AppProperties(Jwt jwt, List<User> users) {

    /**
     * @param secret            base64-encoded HMAC-SHA-256 key, must decode to &gt;= 32 bytes.
     * @param expirationMinutes token lifetime in minutes.
     */
    public record Jwt(String secret, long expirationMinutes) {
    }

    /**
     * @param role role name without the {@code ROLE_} prefix (e.g. {@code "USER"}, {@code "ADMIN"}).
     *             Passwords stored here are seeded into an {@code InMemoryUserDetailsManager}
     *             at startup; in production these come from environment variables, never source.
     */
    public record User(String username, String password, String role) {
    }
}
