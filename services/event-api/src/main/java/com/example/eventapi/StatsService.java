package com.example.eventapi;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class StatsService {

    private static final int TOP_N = 5;
    private static final Duration LAST_24H = Duration.ofHours(24);
    private static final Duration LAST_7D = Duration.ofDays(7);

    private final EventRepository repository;
    private final Clock clock;

    public StatsService(EventRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * Aggregates over the events table using indexed columns ({@code created_at} and {@code type})
     * with four bounded queries: a total count, a group-by-type, a 24h window count, and a
     * top-N group-by within the 7d window. At the indicated scale (millions of rows) each
     * query runs in milliseconds; beyond that, materialised views or a rollup table become
     * appropriate.
     */
    @Transactional(readOnly = true)
    public StatsSummaryResponse summary() {
        Instant now = clock.instant();
        Instant since24h = now.minus(LAST_24H);
        Instant since7d = now.minus(LAST_7D);

        long total = repository.count();
        Map<String, Long> byType = toCountMap(repository.countByType());
        long last24h = repository.countByCreatedAtGreaterThanEqual(since24h);
        List<StatsSummaryResponse.TypeCount> top = repository
                .topTypesSince(since7d, PageRequest.of(0, TOP_N))
                .stream()
                .map(row -> new StatsSummaryResponse.TypeCount((String) row[0], ((Number) row[1]).longValue()))
                .toList();

        return new StatsSummaryResponse(total, byType, last24h, top);
    }

    private static Map<String, Long> toCountMap(List<Object[]> rows) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Object[] row : rows) {
            result.put((String) row[0], ((Number) row[1]).longValue());
        }
        return result;
    }
}
