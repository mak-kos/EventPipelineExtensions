package com.example.eventapi.auth;

import java.time.Instant;

public record LoginResponse(String token, Instant expiresAt) {
}
