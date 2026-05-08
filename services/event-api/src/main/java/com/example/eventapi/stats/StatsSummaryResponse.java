package com.example.eventapi.stats;

import java.util.List;
import java.util.Map;

public record StatsSummaryResponse(
        long totalCount,
        Map<String, Long> countByType,
        long countLast24Hours,
        List<TypeCount> top5TypesLast7Days
) {

    public record TypeCount(String type, long count) {
    }
}
