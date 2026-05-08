CREATE TABLE IF NOT EXISTS events (
    id          UUID PRIMARY KEY,
    payload     TEXT         NOT NULL,
    type        VARCHAR(255),
    status      VARCHAR(50)  NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_created_at      ON events (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_type            ON events (type);
CREATE INDEX IF NOT EXISTS idx_events_type_created_at ON events (type, created_at DESC);
