package com.example.eventapi;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface EventRepository
        extends JpaRepository<Event, UUID>, JpaSpecificationExecutor<Event> {

    long countByCreatedAtGreaterThanEqual(Instant since);

    /**
     * @return rows of [type, count] grouped by type. Events with NULL type are returned
     *         under the literal label {@code "unknown"} so the caller never has to special-case nulls.
     */
    @Query("SELECT COALESCE(e.type, 'unknown'), COUNT(e) FROM Event e GROUP BY e.type")
    List<Object[]> countByType();

    /**
     * @return rows of [type, count] for events with createdAt &gt;= since,
     *         ordered by count DESC. Apply a bounded {@link Pageable} (e.g. PageRequest.of(0, 5))
     *         to limit and avoid scanning more than necessary.
     */
    @Query("SELECT COALESCE(e.type, 'unknown'), COUNT(e) AS c " +
            "FROM Event e WHERE e.createdAt >= :since " +
            "GROUP BY e.type ORDER BY c DESC")
    List<Object[]> topTypesSince(@Param("since") Instant since, Pageable pageable);
}
