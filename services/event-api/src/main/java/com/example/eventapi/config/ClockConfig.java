package com.example.eventapi.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ClockConfig {

    /**
     * Single injectable {@link Clock} so any class that needs "now" can depend on this rather
     * than calling {@link java.time.Instant#now()} directly. Tests substitute a fixed clock.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
