package com.example.eventapi;

import java.time.Instant;

public record LoginResponse(String token, Instant expiresAt) {
}
