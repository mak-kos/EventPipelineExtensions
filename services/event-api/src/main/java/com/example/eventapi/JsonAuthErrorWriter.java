package com.example.eventapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared helper for the security entry point and access-denied handler so 401/403 bodies
 * use the same JSON shape as {@link GlobalExceptionHandler}.
 */
final class JsonAuthErrorWriter {

    private JsonAuthErrorWriter() {
    }

    static void write(HttpServletResponse response,
                      ObjectMapper mapper,
                      HttpStatus status,
                      String error,
                      String message) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", error);
        body.put("message", message);
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(mapper.writeValueAsString(body));
    }
}
