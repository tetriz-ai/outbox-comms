-- outbox-comms schema, version 1. Seven tables. Templates are code, not rows, and
-- the service holds no scheduling state: every message dispatches immediately.
-- Every org-owned table carries organization_id, and every query filters on it.

-- comms_requests — the producer's submission: the durable handle + dedup anchor
-- + shared params + the producer-resolved entries batch.
CREATE TABLE IF NOT EXISTS comms_requests (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id    UUID        NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    usecase            TEXT        NOT NULL,
    params             JSONB       NOT NULL DEFAULT '{}',
    content            JSONB,  -- request-level block content (MessageContent: shared body + per-channel overrides); nullable → dispatch falls back to the per-usecase renderer.
    entries            JSONB       NOT NULL DEFAULT '{}',
    delivery_guarantee TEXT        NOT NULL,
    status             TEXT        NOT NULL DEFAULT 'REQUEST_STATUS_ACCEPTED',
    analytics_id       TEXT,
    created_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_comms_requests_idem UNIQUE (organization_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_comms_requests_pending ON comms_requests (updated_at)
    WHERE status IN ('REQUEST_STATUS_ACCEPTED','REQUEST_STATUS_FANNING_OUT');


-- comms_messages — one per channel×recipient, materialized by the fan-out.
CREATE TABLE IF NOT EXISTS comms_messages (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id     UUID        NOT NULL,
    request_id          UUID        NOT NULL REFERENCES comms_requests(id),
    channel             TEXT        NOT NULL,
    vendor              TEXT        NOT NULL,
    recipient           TEXT        NOT NULL,
    slack_delivery      TEXT        NOT NULL DEFAULT 'SLACK_DELIVERY_UNSPECIFIED',  -- LINK | FILE for a SLACK row; UNSPECIFIED for email. Not part of the dedup key.
    recipient_context   JSONB       NOT NULL DEFAULT '{}',
    content             JSONB,  -- per-channel RenderableContent resolved by fan-out; nullable → dispatch falls back to the per-usecase renderer.
    status              TEXT        NOT NULL DEFAULT 'MESSAGE_STATUS_QUEUED',
    skip_reason         TEXT,
    dead_reason         TEXT,
    unconfirmed_reason  TEXT,
    attempts            INT         NOT NULL DEFAULT 0,
    verification_level  TEXT        NOT NULL,
    delivery_guarantee  TEXT        NOT NULL,
    next_retry_at       TIMESTAMP WITH TIME ZONE,
    rendered_content    JSONB,
    provider_message_id TEXT,
    claim_id            UUID,  -- set by each claim (QUEUED/FAILED → SENDING); a move out of SENDING must name it.
    last_error          TEXT,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_comms_messages_dedup UNIQUE (request_id, channel, recipient)
);
-- reconcile partial indexes — only the small non-terminal set.
CREATE INDEX IF NOT EXISTS idx_comms_messages_retry   ON comms_messages (next_retry_at) WHERE status = 'MESSAGE_STATUS_FAILED';
CREATE INDEX IF NOT EXISTS idx_comms_messages_queued  ON comms_messages (updated_at)    WHERE status = 'MESSAGE_STATUS_QUEUED';
CREATE INDEX IF NOT EXISTS idx_comms_messages_sending ON comms_messages (updated_at)
    WHERE status = 'MESSAGE_STATUS_SENDING' OR (status = 'MESSAGE_STATUS_SENT' AND verification_level = 'VERIFICATION_LEVEL_CONFIRMED');
-- history keyset (the tiebreaker direction matches the DESC scan).
CREATE INDEX IF NOT EXISTS idx_comms_messages_org_created ON comms_messages (organization_id, created_at DESC, id DESC);
-- feedback lookup by provider message id.
CREATE INDEX IF NOT EXISTS idx_comms_messages_provider ON comms_messages (provider_message_id) WHERE provider_message_id IS NOT NULL;


-- comms_delivery_events — append-only provider events.
CREATE TABLE IF NOT EXISTS comms_delivery_events (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id   UUID        NOT NULL,
    message_id        UUID        NOT NULL REFERENCES comms_messages(id),
    event_type        TEXT        NOT NULL,
    provider_event_id TEXT,
    provider_payload  JSONB,
    occurred_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_comms_delivery_events_msg ON comms_delivery_events (message_id, occurred_at);
-- Feedback is delivered at least once: dedup a redelivered event on the provider's event id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_comms_delivery_events_provider ON comms_delivery_events (provider_event_id)
    WHERE provider_event_id IS NOT NULL;


-- comms_suppressions — global sender-reputation list, keyed by address with no
-- organization: an address that hard-bounced bounces for every sender.
CREATE TABLE IF NOT EXISTS comms_suppressions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    deleted_at      TIMESTAMP WITH TIME ZONE
);
-- Addresses are matched case-insensitively; the service stores them lowercased.
CREATE UNIQUE INDEX IF NOT EXISTS uq_comms_suppressions_email ON comms_suppressions (lower(email)) WHERE deleted_at IS NULL;

-- comms_purge_tombstones — what PurgeOrgComms keeps of each purged email message
-- that reached the provider: its provider message id and lowercased address, with
-- no organization, like suppressions. A bounce or complaint that arrives after the
-- purge still suppresses the address. The prune job deletes a tombstone once
-- OUTBOX_COMMS_PURGE_TOMBSTONE_TTL has passed.
CREATE TABLE IF NOT EXISTS comms_purge_tombstones (
    provider_message_id TEXT PRIMARY KEY,
    recipient           TEXT        NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_comms_purge_tombstones_created ON comms_purge_tombstones (created_at);

-- comms_preferences — the preference GATE ("whether to send"). PERSON beats ORG.
CREATE TABLE IF NOT EXISTS comms_preferences (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID        NOT NULL,
    entity_type     TEXT        NOT NULL,
    entity_id       UUID        NOT NULL,
    category        TEXT        NOT NULL,
    channel         TEXT        NOT NULL,
    enabled         BOOLEAN     NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_comms_pref UNIQUE (organization_id, entity_type, entity_id, category, channel)
);


-- comms_channel_destinations — the routing DESTINATION ("where to send").
-- Chat-only; one row per scope (organization, entity, usecase, channel), so
-- setting a destination replaces the scope's ref, even under concurrent calls.
-- An entry resolves the scope it names (ORG or PERSON), with no fallback.
CREATE TABLE IF NOT EXISTS comms_channel_destinations (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id  UUID        NOT NULL,
    entity_type      TEXT        NOT NULL,
    entity_id        UUID        NOT NULL,
    usecase          TEXT        NOT NULL,
    channel          TEXT        NOT NULL,
    destination_kind TEXT        NOT NULL DEFAULT 'DESTINATION_KIND_CHANNEL',
    destination_ref  TEXT        NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_comms_dest UNIQUE (organization_id, entity_type, entity_id, usecase, channel)
);

