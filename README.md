# Backend Test Task: Event Pipeline Extensions

## Overview

You're given a small event-pipeline system with two services:

- **`event-api`** (Java / Spring Boot) — accepts JSON, persists to Postgres,
  publishes to Kafka.
- **`event-processor`** (Python) — consumes the Kafka topic, converts JSON
  to XML, stores the result in Minio.

Your task is to **extend** these services. Focus is on application-layer
code: API design, persistence, auth, integration patterns, error handling,
testing.

`dev/docker-compose.yml` brings up the full local infrastructure (Postgres,
Kafka, Minio). See `dev/README.md` for setup.

**Time budget:** ~3 days

> **On the supplied code.** Treat it as a starting point, not gospel. Some
> parts are minimal, others may be wrong or inconsistent — if something
> blocks you, fix it and note what you changed and why. Improving a shaky
> foundation is a positive signal.

## Architecture

```
                                    POST /events
              ┌────────────┐  (JSON body)  ┌──────────────┐
              │   Client   │ ─────────────▶│  event-api   │
              └────────────┘               │ (Spring Boot)│
                                           └──────┬───────┘
                                                  │
                                       writes ┌───┴────┐ publishes
                                              ▼        ▼
                                       ┌──────────┐ ┌────────┐
                                       │ Postgres │ │ Kafka  │
                                       └──────────┘ └───┬────┘
                                                        │
                                                consumes│
                                                        ▼
                                              ┌──────────────────┐
                                              │ event-processor  │
                                              │     (Python)     │
                                              └────────┬─────────┘
                                                       │ writes XML
                                                       ▼
                                                 ┌──────────┐
                                                 │  Minio   │
                                                 └──────────┘
```

### `event-api` (Java 17 / Spring Boot 3)

- Exposes `POST /events`, accepts an arbitrary JSON body.
- Persists the event to Postgres (`events` table).
- Publishes the event to Kafka topic `events`, keyed by event id.

### `event-processor` (Python 3.10+)

- Consumes Kafka topic `events`.
- Transforms the JSON payload to XML.
- Stores the XML in Minio bucket `events`.

## Scope

This task covers **both** languages — work on Java (`event-api`) **and**
Python (`event-processor`). We want to see something from each.

You don't need to complete every task. Quality beats quantity — aim for at
least one Java task and one Python task. Strong submissions typically cover
3–4 tasks across both sides.

---

## Java track (extend `event-api`)

### Read API

Add the following endpoints to `event-api`:

- `GET /events/{id}` — return a single event, or `404` if not found.
- `GET /events?type=<string>&from=<iso-datetime>&to=<iso-datetime>&page=<int>&size=<int>`
  — list events with optional filters by type and creation timestamp,
  paginated. Default page size: 20. Max page size: your call (justify in
  README).
- The response must include the original payload (parsed back to JSON, not
  the stored TEXT blob), creation timestamp in ISO-8601 (UTC), and status.

Write at least a few integration tests.

### Statistics

Add a `GET /stats/summary` endpoint that returns:

- Total event count.
- Count grouped by `type` (top-level field of the payload).
- Count of events created in the last 24 hours.
- Top 5 most frequent event types in the last 7 days.

The endpoint should remain responsive on a database with millions of events.
Document any assumptions in your README.

### Authentication

Protect both `GET /events*` and `GET /stats/*` with JWT bearer-token
authentication.

- `POST /auth/login` — accepts `{"username":..., "password":...}`, returns
  `{"token": "...", "expiresAt": "..."}`.
- Two roles: `USER` (read access to events and stats) and `ADMIN`
  (everything `USER` can do, plus `DELETE /events/{id}`).
- `POST /events` (existing) remains open for now — flag this in your README
  if you think it shouldn't be.
- User accounts are seeded on startup from configuration; no registration
  flow needed.

Provide at least:

- Tests for happy path, expired token, wrong role, missing token.
- Documentation in the README on how a client obtains and uses a token.

---

## Python track (extend `event-processor`)

### Control plane: health & status

Add an HTTP API to `event-processor` exposing:

- `GET /health` — returns `200` only when the service can actually reach
  both Kafka and Minio. A "the process is alive" check is not enough.
- `GET /events/{id}/status` — returns one of:
  - `404` if no record of this event id has been seen by the processor;
  - `{"status":"processed", "objectKey":"...", "processedAt":"..."}` if the
    event has been transformed and stored;
  - `{"status":"pending"}` for an event seen but not yet stored.

Think carefully about how the processor remembers what it has done — the
current implementation does not preserve any link between an event id and
the stored Minio object. Document the design choice in your README.

### Replay

Add `POST /events/{id}/replay`:

- Re-fetch the event from `event-api` (`GET /events/{id}` from the Java
  track, or use the existing Postgres table directly — your call, justify
  it).
- Re-publish it to the `events` Kafka topic so it gets re-processed.
- Return `202 Accepted` with a body indicating what was scheduled.
- Re-playing an event that has already been processed must not produce a
  duplicate Minio object.

### Concurrent processing with ordering & graceful shutdown

The current consumer processes messages strictly serially. Make it scale:

- Process multiple messages concurrently for higher throughput.
- Preserve ordering **per Kafka message key**. Two messages with the same
  key must be processed in the order they were produced.
- Implement clean shutdown on `SIGTERM`/`SIGINT`: stop accepting new work,
  finish in-flight messages, commit offsets, exit non-zero only on actual
  error.
- The work queue must apply backpressure — if downstream (Minio) slows
  down, the consumer must not buffer unboundedly.

Document the concurrency model and trade-offs in your README. Tests are
expected.

---

## What we look at

A working solution is the baseline; how it is built matters.

- **Correctness:** does it do what was asked, including edge cases.
- **Operational maturity:** error handling, timeouts, retries, idempotency,
  logging, observability hooks.
- **API design:** consistent shapes, sensible HTTP codes, useful errors,
  validated input, sensible defaults.
- **Persistence and integration:** efficient queries, no N+1, indexes where
  needed, transactional boundaries that make sense.
- **Security:** secrets handling, input validation, no obvious injection
  vectors, correct CORS posture.
- **Testing:** at least integration coverage on the new code paths.
- **Documentation:** README that explains decisions, trade-offs, and how to
  run / test the result.
- **Engagement with the supplied code:** noticing and improving what's
  fragile or wrong — not just adding to it — is a strong positive signal.

## Submission

Send a Git repo link (public or private — for private, share access with
the reviewer). Commit history is part of what we review.

The repo should contain:

- Your modified service code.
- Instructions on how to run and test against `dev/docker-compose.yml`.
- A short `NOTES.md` (or expanded `README.md`) covering:
  - What you implemented and what you skipped.
  - Design decisions and the reasoning behind them.
  - Anything you changed in the supplied code, and why.
  - Anything you would change with more time.

## Questions?

Reasonable clarifying questions are welcome and will not count against you.
Reach out to contact us before assuming.
