package com.example.eventapi;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;

@Component
public class JwtTokenProvider {

    private static final Logger log = LoggerFactory.getLogger(JwtTokenProvider.class);

    static final int MIN_KEY_BYTES = 32; // HS256 requires 256-bit keys

    private final SecretKey signingKey;
    private final long expirationMs;
    private final Clock clock;

    public JwtTokenProvider(AppProperties properties, Clock clock) {
        if (properties.jwt() == null || properties.jwt().secret() == null
                || properties.jwt().secret().isBlank()) {
            throw new IllegalStateException("app.jwt.secret is required");
        }
        byte[] keyBytes = Decoders.BASE64.decode(properties.jwt().secret());
        if (keyBytes.length < MIN_KEY_BYTES) {
            throw new IllegalStateException(
                    "app.jwt.secret must decode to at least " + MIN_KEY_BYTES + " bytes (HS256)");
        }
        if (properties.jwt().expirationMinutes() <= 0) {
            throw new IllegalStateException("app.jwt.expiration-minutes must be > 0");
        }
        warnIfDevDefaultKey(keyBytes);
        this.signingKey = Keys.hmacShaKeyFor(keyBytes);
        this.expirationMs = properties.jwt().expirationMinutes() * 60_000L;
        this.clock = clock;
    }

    /**
     * Detects the all-zero placeholder key shipped as a dev default and logs a loud warning.
     * Not fail-fast — local dev still works — but operationally visible if a deployment
     * forgot to set {@code APP_JWT_SECRET}.
     */
    private static void warnIfDevDefaultKey(byte[] keyBytes) {
        for (byte b : keyBytes) {
            if (b != 0) {
                return;
            }
        }
        log.warn("JWT signing key is the all-zero placeholder. Set APP_JWT_SECRET in any "
                + "non-development environment; this key is publicly known.");
    }

    public Issued issue(String username, String role) {
        Instant now = clock.instant();
        Instant expiresAt = now.plusMillis(expirationMs);
        String token = Jwts.builder()
                .subject(username)
                .claim("role", role)
                .issuedAt(Date.from(now))
                .expiration(Date.from(expiresAt))
                .signWith(signingKey, Jwts.SIG.HS256)
                .compact();
        return new Issued(token, expiresAt);
    }

    /**
     * Verifies the signature and expiration. Throws {@link io.jsonwebtoken.JwtException}
     * (or a subclass: {@code ExpiredJwtException}, {@code MalformedJwtException}, etc.) on
     * any failure — caller treats them all as "auth rejected".
     */
    public Parsed parse(String token) {
        Claims claims = Jwts.parser()
                .verifyWith(signingKey)
                .clock(() -> Date.from(clock.instant()))
                .build()
                .parseSignedClaims(token)
                .getPayload();
        Object role = claims.get("role");
        return new Parsed(claims.getSubject(), role == null ? null : role.toString());
    }

    public record Issued(String token, Instant expiresAt) {
    }

    public record Parsed(String username, String role) {
    }
}
