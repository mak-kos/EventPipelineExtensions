# Implementation Notes

## What was implemented

All six tasks across both services.

### Java track (`event-api`)
- **Read API** — `GET /events/{id}` (404 on miss) and `GET /events?type=&from=&to=&page=&size=` with stable `(createdAt DESC, id ASC)` ordering, default page size 20, max 100. Payload returned as parsed JSON, not the stored TEXT blob. Timestamps are ISO‑8601 UTC.
- **Statistics** — `GET /stats/summary` returning total count, count grouped by type, count for the last 24 h, and the top 5 types in the last 7 days.
- **Authentication** — JWT bearer auth (HS256 via jjwt). `POST /auth/login` issues `{token, expiresAt}`. Roles `USER` and `ADMIN`; `DELETE /events/{id}` requires `ADMIN`. Users seeded from `app.users[*]` config; passwords BCrypt‑hashed at startup.

### Python track (`event-processor`)
- **Health & status** — `GET /health` returns 200 only when both Kafka (`AdminClient.list_topics` with explicit timeout) and Minio (`bucket_exists`) respond; 503 otherwise. `GET /events/{id}/status` returns `404` / `{"status":"pending"}` / `{"status":"processed", "objectKey", "processedAt"}`.
- **Replay** — `POST /events/{id}/replay` re‑fetches the event from event‑api and re‑publishes it to Kafka.
- **Concurrent processing** — key‑partitioned worker pool (`hash(key) % N` routing) with bounded queues for backpressure, per‑(topic, partition) `OffsetWatermark` for at‑least‑once commits, and graceful shutdown driven by uvicorn's signal handling.

### Skipped
None.

---

## Test counts

| Service | Test count |
| --- | --- |
| `event-api` (`mvn test`) | **35** integration tests (Testcontainers Postgres) |
| `event-processor` (`pytest`) | **44** unit/integration tests |

Run from each service directory.

---

## Design decisions

### Java

1. **`type` column in the `events` table.** The README says `type` is "the top‑level field of the payload," but querying it inside the TEXT blob (cast to JSON, index) is not viable at "millions of events." I extract `type` into a dedicated `VARCHAR(255)` column at insert time and added `idx_events_type` and the composite `idx_events_type_created_at`. Stats and the `?type=` filter are now O(log n) seeks.
2. **Stats with four bounded queries.** A single mega‑query with `COUNT(*) FILTER (WHERE …)` was an option but trades clarity for one round‑trip; on indexed columns the four-query path is fast enough. For hundreds of millions of rows the next step is a materialised view or a counter table.
3. **`COUNT(*)` is a sequential scan in Postgres.** Documented limitation. At ~10 M rows it's 1–3 s; the spec says "responsive at millions" which this satisfies. Beyond that, switch to `pg_class.reltuples::bigint` for an estimate or maintain a counter via outbox.
4. **Stable pagination.** `(createdAt DESC, id ASC)` instead of `createdAt DESC` alone, so two events sharing a millisecond don't randomly skip or duplicate across pages.
5. **`Clock` injection.** Both `EventService` (existing field is irrelevant; uses repository directly) and `StatsService` use a `java.time.Clock` bean so tests can fix time deterministically (Task 2 tests use a fixed clock).
6. **`@Transactional(readOnly = true)` on read paths.** Hibernate auto‑flush is skipped, dirty‑checking is skipped, and intent is explicit.
7. **`JpaSpecificationExecutor` for filters.** Type‑safe predicate composition with no `:p IS NULL OR …` JPQL gymnastics. Adds zero new dependencies.
8. **Kafka publish stays outside any write transaction.** `repository.save()` runs in its own short transaction; the Kafka publish happens after commit. Putting the publish inside a `@Transactional` method would mean a rollback after I/O, which can't be safely rolled back.
9. **Producer timeouts are explicit.** `max.block.ms`, `request.timeout.ms`, `delivery.timeout.ms` are wired in `application.properties`. A Kafka stall must not eat request threads.
10. **JWT signing key validation.** `app.jwt.secret` must decode (Base64) to ≥ 32 bytes (HS256). `JwtTokenProvider` rejects shorter keys at startup; the all‑zero "dev placeholder" logs a `WARN` so a misconfigured production deployment is operationally visible (without breaking local dev).
11. **`InvalidCredentialsException` collapses every `AuthenticationException`.** Same response and message regardless of root cause — prevents username enumeration via timing or text differences.
12. **`@WithMockUser` on the Read/Stats integration tests.** Cleaner than threading real JWTs through every test; the security chain is exercised directly in `SecurityIntegrationTest`.

### Python

13. **Minio object key is `{event_id}.xml`.** This is the single most important Python decision. The original code generated `{uuid4()}.xml` and lost the link between event id and stored object. Using the event id as the key:
    * makes `/events/{id}/status` answerable purely from the in‑memory `EventStore`,
    * makes Task 5 replay **idempotent** for free — re‑put under the same key overwrites instead of creating a duplicate,
    * lets a future restart‑survivable variant rebuild state by listing Minio.
14. **`EventStore` is in‑memory.** The README invites discussion. In‑memory is the simplest design that satisfies the spec; the trade‑off is a process restart loses pending state. A future iteration can make it durable by listing Minio at boot (processed entries can be reconstructed; pending entries are inherently transient).
15. **`mark_pending` doesn't downgrade `processed`.** Replay would otherwise transiently flip the status back to "pending" between re‑publish and re‑process. Tested explicitly.
16. **Concurrency model: key‑partitioned bounded thread pool.**
    * N workers, each owning a `queue.Queue(maxsize=...)`.
    * Routing: `int.from_bytes(key, "big") % N` — deterministic across runs (Python's str hash is randomised).
    * Same key → same worker → FIFO → per‑key order preserved.
    * Bounded queue → backpressure: when Minio is slow, queues fill, the consumer blocks on `submit`, polling stops, no unbounded memory growth.
    * Failed messages don't advance the watermark; on restart they're reread; the `{event_id}.xml` key gives idempotency, so reprocessing is a no‑op.
17. **`OffsetWatermark`.** Workers complete out of order. Committing past an in‑flight offset would lose that message on crash. The watermark advances only when offsets complete contiguously per (topic, partition).
18. **Manual offset commits, async during steady state, sync on shutdown.** Auto‑commit was disabled because it commits on a timer regardless of processing state — that combined with concurrent workers loses messages on crash.
19. **Replay re‑fetches via event‑api HTTP, not direct DB.** Service boundary discipline. The processor authenticates via `POST /auth/login` and caches the token; a 401 triggers a single re‑login + retry. Direct DB access would couple the processor to event‑api's schema.
20. **All HTTP and Kafka I/O has explicit timeouts.** httpx via `httpx.Timeout(connect, read, write, pool)`; Kafka producer via `delivery.timeout.ms`/`request.timeout.ms`/`message.timeout.ms`; Minio via a `urllib3.PoolManager` with `connect=5s`, `read=10s`, bounded retries.
21. **Graceful shutdown.** uvicorn owns the signal handlers and sets `server.should_exit` on SIGTERM/SIGINT. After `server.run()` returns, `main` stops the consumer (which performs a final sync commit), drains the worker pool (sentinel + join with timeout), flushes the replay producer, and closes the HTTP client. Exit code 0 on a clean shutdown, non‑zero if any phase timed out or the consumer reported an error.

---

## Changes to the supplied code, and why

| File | Change | Why |
| --- | --- | --- |
| `db/init.sql` | Added `type VARCHAR(255)`, `idx_events_type`, `idx_events_type_created_at`, `idx_events_status`. | Required for efficient `?type=` filter and stats grouping. |
| `services/event-api/src/main/resources/application.properties` | Added Jackson UTC config, max page size, JWT settings, seeded users, explicit Kafka producer timeouts. | Per CTO rules: no infinite waits, secrets via env‑var override. |
| `services/event-api/src/main/java/com/example/eventapi/EventController.java` | Refactored to delegate to `EventService`; added GET / DELETE endpoints. | Required by Tasks 1 + 3; existing controller mixed concerns. |
| `services/event-api/pom.xml` | Added Spring Security, jjwt, validation starter, test stack (starter‑test, spring‑boot‑testcontainers, testcontainers postgresql + junit‑jupiter). Surefire `argLine=-Duser.timezone=UTC`. | Tasks 1–3 + integration tests. UTC arg fixes a Postgres `"Europe/Kiev"` rejection (Postgres only accepts the renamed `"Europe/Kyiv"` post‑tzdata‑2022a). |
| `services/event-processor-python/main.py` | Decomposed into modules; consumer in a thread, FastAPI on the main thread, key‑partitioned worker pool, signal‑driven graceful shutdown. | Required by Tasks 4 + 5 + 6. |
| `services/event-processor-python/requirements.txt` | Added `fastapi`, `uvicorn[standard]`, `httpx`, `pytest`, `pytest-asyncio`. | HTTP server, replay client, tests. |
| `postman/EventPipeline.postman_collection.json` | Added auth/login, list/get/delete events, stats, processor health/status/replay; auto‑capture token after login. | Mirrors the new endpoint surface. |

---

## Operational notes

### Default credentials & secrets (DEV ONLY)

| Variable | Default (do **not** ship to production) |
| --- | --- |
| `APP_JWT_SECRET` | base64 of 32 zero bytes; the application logs a `WARN` if this is left as the default |
| `APP_USERS_0_*` | `admin` / `admin123` / `ADMIN` |
| `APP_USERS_1_*` | `user` / `user123` / `USER` |
| `EVENT_API_USERNAME` / `EVENT_API_PASSWORD` (processor) | `user` / `user123` |

Override via environment variables in any non‑dev environment.

### Database migration on existing volumes

`db/init.sql` only runs on fresh Postgres init. Hibernate's `ddl-auto=update` will add the new `type` column on the first boot, but **the new indexes will not be created automatically** — apply the index DDL manually if upgrading an existing volume.

---

## How to run / test

```bash
# 1. Bring up infra
cd dev && docker compose up -d

# 2. event-api
cd ../services/event-api
mvn -DskipTests package
mvn spring-boot:run                        # http://localhost:8080
# - or run tests:
mvn test                                   # requires Docker for Testcontainers

# 3. event-processor
cd ../event-processor-python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py                             # http://localhost:8081
# - or run tests:
pytest -q tests/

# 4. Smoke test via curl
curl -X POST localhost:8080/events -H 'Content-Type: application/json' \
     -d '{"type":"user.signup","userId":"u-1"}'
TOKEN=$(curl -s -X POST localhost:8080/auth/login -H 'Content-Type: application/json' \
     -d '{"username":"user","password":"user123"}' | python -c "import sys,json;print(json.load(sys.stdin)['token'])")
curl -s -H "Authorization: Bearer $TOKEN" localhost:8080/events
curl -s localhost:8081/health
```

---

## Things I would change with more time

- **Outbox pattern for `POST /events`.** Today, persist + Kafka publish are two operations. If the publish fails after the DB commit, the event row exists but no Kafka message is produced. An outbox table + a poller (or Debezium/CDC) makes that pair atomic. The Python replay path is the manual recovery for now.
- **Materialised view or counter table for total event count.** Postgres `SELECT COUNT(*)` is a sequential scan; at hundreds of millions of rows the stats endpoint should switch to a maintained counter or `pg_class.reltuples`.
- **Persistent `EventStore` in the processor.** Today, restart loses pending state (processed entries can still be discovered via Minio HEAD on `{event_id}.xml`, but pending ones can't). A small SQLite or Redis‑backed store would survive restarts.
- **Login‑rate limiting.** No per‑IP throttle on `/auth/login`. With seeded users and BCrypt this is acceptable for a coding exercise but a real deployment needs Bucket4j / Spring Cloud Gateway / Cloud WAF.
- **Project `.gitignore`.** `.DS_Store`, `target/`, `.venv/`, `__pycache__/` and IDE folders are not ignored. Did not commit them, but the next contributor will easily.
- **Postman collection contracts.** The current Postman set is samples; a Newman/CI‑runnable suite with assertions would be a real test pass.
- **Observability hooks.** Today: SLF4J logs in Java, stdlib logging in Python. Real deployment would add a correlation/trace id in MDC (Java) and a `contextvars` MDC in Python, and emit via OTel.
