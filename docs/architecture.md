# Architecture

outbox-comms is one Go binary that serves the gRPC API, runs the queue workers, and runs the
scheduled jobs. Postgres holds every request, message and delivery event. Redis carries the
queue, the delayed retries and the scheduler's lock.

## The path of a message

```
SubmitComms ──► comms_requests row ──► request trigger ──► fan-out
                (ACCEPTED)                                   │
                                                             ▼
                                       comms_messages rows (QUEUED or SKIPPED)
                                                             │ one dispatch trigger each
                                                             ▼
                                         dispatcher ──► SMTP / SES / Slack
                                                             │
                                                             ▼
                                         SENT, DELIVERED, FAILED or DEAD
                                                             │ SES feedback (optional)
                                                             ▼
                                         DELIVERED, BOUNCED, suppression
```

1. **Submit.** The API validates the request and inserts the request row under the
   organization's idempotency key, and only the insert that created the row publishes the
   request trigger. A second submit with the same key returns the same handle, or
   `ALREADY_EXISTS` when the stored request differs from the new one.
2. **Fan-out.** A worker turns each entry into one message row, resolving a configured
   destination to a channel and marking the row `SKIPPED` with a reason when the preference
   gate says no. It then publishes one dispatch trigger per `QUEUED` row. Rows are unique per
   request, channel and recipient, so a fan-out that runs twice adds nothing.
3. **Dispatch.** A worker claims the row by moving it from `QUEUED`, or from `FAILED` past its
   retry time, to `SENDING` in one conditional update, so a duplicated trigger never sends
   twice. It checks the suppression list, renders the content, calls the sender, and records
   the outcome. An error before the sender is called puts the row back to `QUEUED`.
4. **Feedback.** When SES feedback is configured, a consumer reads SES's delivery, bounce and
   complaint events from SQS and matches each to its message by the provider's message id. A
   hard bounce or a complaint adds the address to the suppression list.

## State changes

Every status change is a conditional update that names the status it expects to move from.
When the row has moved on, the update changes nothing, and the caller treats the stale
transition as already done. The dispatcher appends a delivery event for each send outcome, the
feedback consumer one for each provider event, and reconcile changes the row only.

The event is written after the status change, not in the same transaction. When that write
fails, the status stands and the attempt returns the error, so a message can reach a terminal
status without its event. The message status is the record, and the event log is best-effort.

The outcome is written after the provider call even when the call's context has ended, so a
shutdown in the middle of a send still records what the provider answered. The sender's
outcome decides the next status:

| Outcome | Next status |
|---|---|
| Success | `SENT` for email, `DELIVERED` for Slack |
| Retryable | `FAILED` with `next_retry_at`, or `DEAD` once the attempts run out |
| Permanent, or a revoked Slack token | `DEAD` with the reason |
| Uncertain | `FAILED` for a `GUARANTEED` message, `UNCONFIRMED` for a `BEST_EFFORT` one |

A `FAILED` row gets one delayed retry task on Redis at its `next_retry_at`, with a task id
built from the message id and the attempt number, so the same retry is never scheduled twice.
The delays come from a fixed ladder, by default 1m, 5m, 30m, 2h and 6h, with no random spread,
and the last step repeats when the attempt count allows more tries. When Slack asks for a wait
with Retry-After on a 429, the retry waits the later of that and the ladder step.

A usecase's contract can override the ladder and the attempt count, each field on its own,
over the global setting.

## Recovery

Triggers can be lost, because a worker can die mid-send and a queue message can vanish. The
reconcile job finds the rows a lost trigger leaves behind and drives them on. It runs every
10 minutes by default, reads at most 1000 rows per query, and locks each batch with
`FOR UPDATE SKIP LOCKED` so two runs never take the same row.

| Rows it looks for | What it does |
|---|---|
| A request still `ACCEPTED` or `FANNING_OUT` after 5 minutes | Publishes the request trigger again |
| A `GUARANTEED` message still `SENDING` after 5 minutes | `QUEUED` and a new trigger, with the crash counted as an attempt, or `DEAD` with `crash_exhausted` once attempts reach the cap |
| A `BEST_EFFORT` message still `SENDING` after 5 minutes | `UNCONFIRMED` |
| A `SENT` message that needs a delivery event and has none after 30 minutes | `UNCONFIRMED` |
| A `QUEUED` message older than 5 minutes | Publishes the trigger again |
| A `FAILED` message past its retry time | Publishes the trigger again |

Each row of that table is one query, and a run tries every one even when an earlier one
fails. A failed query, or a failed gauge update, is counted in
`outbox_comms_reconcile_failures_total` by its `arm` label, and the run returns all the errors
together.

The same run sets the overdue-retry gauge and the 24-hour bounce and complaint rates. Both
rates read 0 until the window holds 100 email sends, and a separate gauge shows the sample. A
daily prune job strips stored provider payloads older than 90 days, keeps the rows, and
deletes expired purge tombstones.

## Scheduled jobs and leader election

Every replica runs queue workers, and only one replica schedules the periodic jobs. Replicas
elect that leader with a lock on the single Redis, a `SET` with `NX` and an expiry that holds a
random token. A leader that stops renewing loses the lock when it expires, and another replica
takes over.

The leader extends the lock before it expires and releases it on shutdown. Both are Lua
scripts that check the token first, so a replica never extends or releases a lock it has lost.

Each tick of the reconcile and prune schedules enqueues a task with a fresh id and a
uniqueness lock that lasts half the schedule's interval. A run that finishes releases the
lock, and a run whose process died holds it only until it expires, so the next tick runs
anyway. A tick the leader cannot enqueue is logged at `ERROR` and counted in
`outbox_comms_scheduler_enqueue_failures_total`.

Two runs can therefore overlap, which both jobs allow. Reconcile's status changes take rows
with `FOR UPDATE SKIP LOCKED` and re-check the status in the same statement, and its other
checks only re-send triggers that the fan-out, the dispatch claim and the retry id
deduplicate. Prune is idempotent.

## Queues

The request and dispatch triggers go through a small publisher and handler interface with two
implementations, asynq on Redis by default, or Amazon SQS with a poller per queue. A handler
that returns a permanent error drops the message instead of retrying it, so a malformed
trigger cannot loop. Delayed retries and the periodic jobs always use Redis.

## Storage

Postgres holds five tables, for requests, messages, delivery events, preferences and channel
destinations. Each carries the organization id, and every query filters on it.

A sixth table, the suppression list, is keyed by address alone, because a bounce describes
the address, not the organization. A seventh, purge tombstones, is keyed by provider message
id with no organization. An organization purge keeps each sent email's id and lowercased
address there, so later feedback still suppresses the address until the prune job deletes the
tombstone, after 30 days by default.

Proto enums are stored by name and proto messages as JSON, through two GORM serializers named
`outboxcomms_protoenum` and `outboxcomms_protojson`. `server.OpenDB` registers them once, so
importing a package registers nothing in GORM. Migrations are embedded in the binary and run
at start, under an advisory lock so replicas starting together migrate once.

A channel destination has a scope, the organization or one person in it, with one channel
per scope, usecase and channel. A send names its scope, and uses that scope's destination
only.

Message rows are unique per request, channel and recipient. Submit caps a recipient at 1024
bytes and an idempotency key at 255, so each entry of those unique indexes stays well under
Postgres's 2704-byte limit for a btree entry.

Attachments are read from a local directory or an S3 bucket at send time, and the request
carries only the key. The store reads `<organization_id>/<key>`, so a message can only attach
its own organization's files. The local store reads and writes through `os.Root`, so no
symlink carries a read or a write out of that folder.

## Senders

| Channel | Sender | Notes |
|---|---|---|
| Email | SMTP | Uses STARTTLS when the server offers it. No delivery feedback. |
| Email | SES | A plain send, or a raw MIME message when there is an attachment. Errors are classified by SES's error code, never its text. |
| Slack | Web API | `chat.postMessage` with a bot token per organization or one shared token. File uploads use Slack's external upload flow, and the upload URL must be on a Slack host. |

Every sender returns one of the outcomes in "State changes". The SES and Slack senders watch
each of their own HTTP requests with `httptrace` and note whether it was written in full, and
a credential or token fetch never counts.

After a full write, a failure without a provider error code is uncertain, because the provider
may have sent the message. So is a 5xx, any Slack HTTP error status but 429, an SES answer
with no message id, and Slack's `fatal_error` or `internal_error`. A failure before the write,
a timeout or a TLS reset included, is a plain retry.

A failure in the first two steps of a Slack file upload is a plain retry too, since only the
last step shares the file. The SES client makes no retries of its own, so the dispatcher's
ladder, which knows the delivery guarantee, is the only retry.

Each provider call runs under a deadline, 60 seconds by default, and a call still running then
is cut off and read the same way. The stuck-send threshold must be at least 2.5 times the
deadline, so reconcile never takes a row whose send is still running. On shutdown, the workers
wait 15 seconds longer than the deadline, so a send in flight can finish.

## Rendering

A request carries block content or typed params. Block content renders to escaped HTML and
plain text for email, and to Block Kit for Slack, from one body with optional per-channel
overrides. A usecase registered with a renderer turns typed params into content in Go.

Slack text escapes `<`, `>` and `&`, so content cannot become a link or a mention. A
call-to-action link must be http or https on both channels, and a dropped link is not logged.
Slack blocks a renderer returns must be a JSON array of at most 50 objects, or the message
ends `DEAD` with `render_bug`.

Every email is wrapped in a shell with the brand, a footer, and a line saying why the
recipient got it.

## Observability

The binary serves `/healthz` and `/metrics` on its HTTP port. Without a registry in
`server.Options`, it makes its own, with the Go and process collectors, and never touches
Prometheus's default registry.

Metrics count each message that reaches an outcome status, every dead and unconfirmed message,
every feedback event with no matching message, and every failed reconcile query. They time
each dispatch and each delivery, and gauge overdue retries and reputation rates. No label
carries an organization id, so the series count does not grow with the customer count.

`monitoring/` holds the alert rules, their tests, and a dashboard.
