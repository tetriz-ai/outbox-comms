# Reference

The exact rules: every RPC, status, usecase, content block, setting, port, operator command and metric. [architecture.md](architecture.md) explains the design behind them, and the [proto files](../proto/outboxcomms/v1/) are the source of truth for the API.

## What it does

outbox-comms is a gRPC service your applications call to send email and Slack messages.

- `SubmitComms` takes one request with up to 1000 recipients, stores it, and returns a handle. The handle is the request's idempotency key, which you choose. Behind it, the fan-out turns the request into one message per recipient, and the messages are delivered from a Redis queue or Amazon SQS.
- `GetCommsStatus`, `ListMessages` and `GetMessageEvents` report each message's status and its delivery events. `GetCommsStatus` also says when a request is `done`: every message has reached a final status.
- `CheckSendEligibility` answers, before you build an expensive message, whether a send would go out now. With `target_kind` set to `TARGET_KIND_CONFIGURED_DESTINATION`, it also checks that a Slack channel is configured. That is the person's channel when `person_id` is set, and the organization's otherwise, the same one the send would use.
- Preferences decide whether a message may go to a person. Destinations decide which Slack channel an organization's messages land in.
- `PurgeOrgComms` deletes an organization's requests, messages, events, preferences and destinations, so you can remove an organization that leaves. [Access](production.md#access) lists what a purge keeps.
- Email goes over SMTP or Amazon SES, and Slack goes to the Web API with a bot token. Hard bounces and complaints from SES suppress the address.
- Attachments: an email carries a file, and a Slack message uploads one (`SLACK_DELIVERY_FILE`), read from a local folder or an S3 bucket by the `attachment_key` param. [Attachments](#attachments) says how.
- Failed sends are retried on a backoff ladder, and a reconcile job finds every message a crash left behind and sends it again or marks it unconfirmed. `GUARANTEED` is the default for the built-in `notice` and `alert`, and a request can set its own guarantee.
- `outbox-comms admin` holds the operator commands. They re-drive dead messages, lift a suppression, render a message type, and send operator-written content once reviewers have received a preview.
- A binary of your own can add message types with Go renderers, and gates of your own, through the `cli`, `server`, `templates` and `gate` packages. [Register a usecase](guides.md#register-a-usecase) shows how.

What it does not do:

- It never chooses an audience. The caller names every recipient, or names the organization whose configured channel should receive the message.
- It has no authentication of its own. [Access](production.md#access) says what that allows.
- It schedules nothing. Every message is sent as soon as it is submitted, and only retries wait.
- It tracks no opens or clicks, and has no unsubscribe link for marketing mail.

## The gRPC API

`outboxcomms.v1.CommsService` on port 9090, with gRPC health. Reflection is disabled by default; the local compose stack opts in with `OUTBOX_COMMS_GRPC_REFLECTION=true`. The native server binds gRPC and HTTP to `127.0.0.1` by default; containers explicitly bind all interfaces. Requests are checked against the protos' `buf.validate` rules before the handler runs.

| RPC | What it does | Errors |
|---|---|---|
| `SubmitComms` | Validates, stores, enqueues once, returns the handle | `INVALID_ARGUMENT`, `FAILED_PRECONDITION` when the organization is switched off, `ALREADY_EXISTS` when the key was used for a different request |
| `GetCommsStatus` | The request's status and every message's. `done` when the request has fanned out and every message is terminal, and `is_partial` when at least one message ended undelivered or the fan-out failed | `NOT_FOUND` for an unknown handle |
| `CheckSendEligibility` | Per channel: eligible, or the reasons not. `no_destination` only for `TARGET_KIND_CONFIGURED_DESTINATION` | `INVALID_ARGUMENT` for an unknown usecase, an unknown `target_kind`, or a `target_kind` a requested channel cannot carry (the pairs `SubmitComms` accepts) |
| `ListMessages` | Messages newest first, 50 a page and at most 200, with a cursor | `INVALID_ARGUMENT` for a bad organization id or cursor |
| `GetMessageEvents` | One message's events, oldest first | `INVALID_ARGUMENT` for a message id that is not a UUID |
| `GetPersonPreferences`, `UpdatePersonPreferences` | One person's preferences | |
| `GetOrgPreferences`, `SetOrgPreferences` | The organization's preferences | |
| `GetDestinations`, `SetDestination` | The organization's routing, one channel per scope, usecase and channel. The scope is `ENTITY_TYPE_ORG` or `ENTITY_TYPE_PERSON` | `INVALID_ARGUMENT` for any other scope, an ORG id that is not the organization, a PERSON id that is the organization's, or a `destination_kind` other than `DESTINATION_KIND_CHANNEL` (unspecified reads as it) |
| `PurgeOrgComms` | Deletes the organization's rows, keeps suppressions, and keeps each sent address for late feedback | |

Submit rejects the whole request, and stores nothing, on any of these:

- **Size:** no entries, or more than 1000. Pinned by `TestEntriesCap` in `internal/api/limits_test.go`.
- **Keys:** an idempotency key over 255 bytes.
- **Usecase:** not registered, a channel it does not enable, or params its schema rejects.
- **Entry:** an unset channel or target kind, or a channel and target kind that do not pair.
- **Recipient:** an empty one, one over 1024 bytes, or an email recipient that is not a bare address.
- **Fields:** `slack_delivery` on a non-Slack entry, or a `subject_person_id` that is not a UUID.
- **Destination:** a `CONFIGURED_DESTINATION` entry with a recipient, with a `subject_person_id`, or without a `destination_scope` whose entity id is a UUID. A `destination_scope` that can never resolve, meaning a type other than `ENTITY_TYPE_ORG` or `ENTITY_TYPE_PERSON`, ORG with another organization's id, or PERSON with the organization's id. A `destination_scope` on any other entry.
- **Duplicates:** two entries with the same channel and recipient, or the same channel and scope. Email addresses compare without case.
- **Text:** a NUL byte in any content or params string.

## Message lifecycle

| Status | Meaning | Terminal |
|---|---|---|
| `QUEUED` | Waiting for a worker | No |
| `SENDING` | A worker has claimed it | No |
| `SENT` | The email provider accepted it | Yes for `PROVIDER_ACK`, no for `CONFIRMED` |
| `DELIVERED` | Slack accepted it, or the email provider reported delivery | Yes |
| `FAILED` | A retryable failure, with `next_retry_at` set | No |
| `DEAD` | Retries ran out, a worker stopped mid-send on every attempt, or the failure was permanent. `dead_reason` says which | Yes |
| `UNCONFIRMED` | An uncertain best-effort send, or no delivery event in 30 minutes | Yes |
| `SUPPRESSED` | The address hard-bounced or complained before | Yes |
| `BOUNCED` | The address hard-bounced after the send | Yes |
| `SKIPPED` | Deliberately not sent. `skip_reason` is `opted_out`, `no_destination` or `org_frozen` | Yes |

The outcome of each send decides the next status:

- **Success:** email is `SENT`, Slack is `DELIVERED`.
- **Retryable:** throttling, a Slack error code the service does not know, or a failure before the whole request was written, such as a refused dial, a DNS failure or a timeout while connecting. A failure in the first two steps of a Slack file upload is retryable too, because only the last step shares the file. The row is `FAILED` until its next try.
- **Retry ladder:** the tries wait 1m, 5m, 30m, 2h and 6h, and the last step repeats if `OUTBOX_COMMS_RETRY_MAX_ATTEMPTS` allows more tries than that. The sixth failure, by default, makes the row `DEAD`. A Slack 429 waits for the later of the ladder step and Slack's `Retry-After`.
- **Permanent:** a rejected message, an unverified sender or an unknown channel. The row is `DEAD` with `permanent_config`.
- **Revoked Slack token** or none configured: `DEAD` with `invalid_auth`.
- **Uncertain:** the whole request was written, and no answer with a provider error code came back. That is a timeout waiting for the answer, a reset or cut-off connection, an unreadable answer, a 5xx, any other Slack HTTP error but 429, an SES answer with no message id, or Slack's `fatal_error` or `internal_error`. `GUARANTEED` retries, and `BEST_EFFORT` stops at `UNCONFIRMED`.
- **SES retries:** none of its own. The ladder is the only retry.
- **Render error:** `DEAD` with `render_bug`, without a send.

The reconcile job runs every 10 minutes:

- **Stuck requests:** a request not yet split into messages 5 minutes after submit, whose status is still `ACCEPTED` or `FANNING_OUT`, is fanned out again.
- **Stuck sends:** a `SENDING` message older than 5 minutes is queued again if `GUARANTEED`, or marked `UNCONFIRMED` if `BEST_EFFORT`. A requeue counts as an attempt, and a `GUARANTEED` message whose attempts reach the cap goes `DEAD` with `crash_exhausted` instead.
- **Lost triggers:** a `QUEUED` message older than 5 minutes, or a `FAILED` one past its retry time, is dispatched again.
- **Gauges:** it sets the overdue-retry, bounce-rate and complaint-rate metrics.

Each query in these checks reads at most 1000 rows a run, so stuck sends and lost triggers, with two queries each, handle up to 2000. The prune job runs daily, strips stored provider payloads older than 90 days, and keeps the rows.

## Usecases

| Key | Category | Uncertain send |
|---|---|---|
| `notice` | transactional: skips preferences | retried (`GUARANTEED`) |
| `alert` | alert: on unless opted out | retried (`GUARANTEED`) |
| `digest` | product: on unless opted out | not retried (`BEST_EFFORT`) |

All three send to email and Slack, track to `PROVIDER_ACK`, and take block content. A request's `delivery_guarantee` overrides the usecase's. A usecase with no renderer accepts any params and sends the request's block content.

Your own binary registers usecases with `templates.MustRegister`, as [Register a usecase](guides.md#register-a-usecase) shows:

- **Commands:** `cli.Main` runs every stock command, `serve`, `migrate`, `admin` and the rest, with your usecases known.
- **Options for `serve`**, such as per-organization Slack tokens, a `*slog.Logger` or a Prometheus registry, go in `server.Options`, passed through `cli.MainWithOptions(ctx, args, opts)` in place of `cli.Main`. Without a logger, the service uses slog's default logger. Without a registry, it makes its own with the Go and process collectors, serves that on `/metrics`, and leaves Prometheus's default registry alone.
- **Content first:** a request that carries content sends that content, even when its usecase has a renderer. The renderer runs only for a request that carries params alone.
- **Usecase keys** are stored on every row, so renaming one leaves the old rows with a usecase nothing registers. `MustRegister` panics at start-up on a bad key or an unset field.

## Content blocks

A `MessageContent` has a `base` body and optional `email` and `slack` overrides. A body has a subject, an optional greeting name, a call-to-action link and these blocks:

| Block | Email | Slack |
|---|---|---|
| `heading`, `paragraph` | Escaped HTML and plain text | Header or bold section, mrkdwn |
| `card`, `stat_list` | A bordered card or a table, coloured by direction | A section with a marker |
| `divider` | A rule | A divider |
| `raw_html` | Inserted as given, and you escape it | Ignored |
| `raw_slack_blocks` | Ignored | Inserted as given |

Every email is wrapped in a document with the brand name, logo, footer links and a notice saying why the recipient got it. Links other than `http` and `https` are dropped, in email and in Slack.

Slack text escapes `<`, `>` and `&`, so `<!channel>` in content is text, not a mention. A Slack message keeps at most 50 blocks and 3000 characters a section, with a note when it cuts.

Content with no subject and no blocks is a render error. So is an email with no subject, or a card with no headline. A render error ends the message `DEAD` with `render_bug`, without a send.

## Attachments

A message can carry one file, read from the attachment store when it is sent:

- **Store:** put the file in the organization's folder, `<organization_id>/<key>` under `OUTBOX_COMMS_ATTACHMENTS_DIR` or under the S3 prefix, with the id in lowercase, such as `8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f/reports/2026-10.pdf`. `admin send --attachment` stores it there for you.
- **Params:** set `attachment_key` to the key alone, `reports/2026-10.pdf`, and `attachment_filename` to the name the recipient sees, `attachment.pdf` by default. A message reads only its own organization's folder.
- **Email** attaches the file. **Slack** uploads it when the entry's `slack_delivery` is `SLACK_DELIVERY_FILE`, and posts the rendered message otherwise.
- **A key that is refused or names no readable file** ends the message `DEAD` with `permanent_config`, without a send. That is an absolute key, one with a `..` segment, a backslash or a NUL byte, one too long for the file system, one naming a directory or passing through a file, a symlink loop, and a symlink leading out of the organization's folder. A symlink that stays inside the folder is followed.

## Configuration

`docker compose exec outbox-comms outbox-comms config` prints every variable with its default. The ones most deployments set:

| Variable | Default | Meaning |
|---|---|---|
| `OUTBOX_COMMS_DATABASE_URL` | `localhost:5432`, database `outbox_comms`, shown in full below | Postgres URL. Migrations run at start |
| `OUTBOX_COMMS_REDIS_ADDR` | `localhost:6379` | Redis for the queue, retries and the scheduler lock |
| `OUTBOX_COMMS_QUEUE` | `asynq` | `asynq` for Redis, or `sqs` with `OUTBOX_COMMS_SQS_REQUESTS_URL` and `OUTBOX_COMMS_SQS_DISPATCH_URL` |
| `OUTBOX_COMMS_EMAIL` | `smtp` | `smtp`, `ses` or `none` |
| `OUTBOX_COMMS_EMAIL_FROM` | `Outbox Comms <noreply@example.com>` | The sender |
| `OUTBOX_COMMS_SMTP_ADDR` | `localhost:1025` | The SMTP relay. STARTTLS when offered |
| `OUTBOX_COMMS_SQS_FEEDBACK_URL` | empty | The SQS queue SES feedback arrives on. Empty turns feedback off |
| `OUTBOX_COMMS_SLACK_BOT_TOKEN` | empty | The token for every organization without its own |
| `OUTBOX_COMMS_SLACK_ORG_TOKENS` | empty | `org-uuid=token,…` |
| `OUTBOX_COMMS_ATTACHMENTS` | `dir` | `dir` with `OUTBOX_COMMS_ATTACHMENTS_DIR`, or `s3` with `OUTBOX_COMMS_S3_BUCKET` |
| `OUTBOX_COMMS_RETRY_LADDER` | `1m,5m,30m,2h,6h` | Backoff after each failure. The last step repeats when more tries are allowed |
| `OUTBOX_COMMS_RETRY_MAX_ATTEMPTS` | `6` | Sends before a message is `DEAD`, crash requeues included |
| `OUTBOX_COMMS_SEND_TIMEOUT` | `60s` | The deadline of one provider call. A call cut off after its request was written is an uncertain send |
| `OUTBOX_COMMS_STUCK_SENDING_AFTER` | `5m` | When a `SENDING` message counts as crashed. It must be at least 2.5 times `OUTBOX_COMMS_SEND_TIMEOUT`, so no send still running is sent again |
| `OUTBOX_COMMS_PURGE_TOMBSTONE_TTL` | `720h` | How long a purge keeps each sent address for late bounces and complaints |
| `OUTBOX_COMMS_REPUTATION_MIN_SENDS` | `100` | Email sends in the window below which the reputation rates read 0 |
| `OUTBOX_COMMS_BRAND_NAME` | empty | The name in the email header and footer |

The default database URL is `postgres://outbox:outbox-local-only@localhost:5432/outbox_comms?sslmode=disable`.

A malformed value, including a malformed cron schedule among the variables `outbox-comms config` lists, stops the service at start with the variable's name.

The SES, SQS and S3 adapters read the AWS SDK's own variables, only when they are configured:

| Variable | Meaning |
|---|---|
| `AWS_REGION` | The region, or `AWS_DEFAULT_REGION` |
| `AWS_ENDPOINT_URL_SES`, `AWS_ENDPOINT_URL_SQS`, `AWS_ENDPOINT_URL_S3` | A different endpoint for one service, such as a local stand-in. `AWS_ENDPOINT_URL` sets all three |
| Credentials | The SDK's default chain: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, a profile, web identity, or the container or instance role |

## Ports

The compose stack publishes four ports on 127.0.0.1:

| Port | Serves | Variable that moves it |
|---|---|---|
| 9090 | the gRPC API | `OUTBOX_COMMS_GRPC_PORT` |
| 8080 | `/healthz` and `/metrics` | `OUTBOX_COMMS_HTTP_PORT` |
| 8090 | the Slack stub | `OUTBOX_COMMS_STUB_PORT` |
| 8025 | Mailpit's web page | `OUTBOX_COMMS_MAILPIT_PORT` |

Export the variable before `docker compose up`. The examples and scripts read the same variables, so they follow.

## Operator commands

```text
outbox-comms admin redrive-dead [--org UUID] [--reason REASON] [--limit N] [--execute]
outbox-comms admin unsuppress --email ADDRESS [--execute]
outbox-comms admin render --usecase KEY --channel email|slack [--params JSON] [--content FILE] [--send --to TARGET [--org UUID]]
outbox-comms admin send --org UUID --usecase KEY --content FILE --recipients FILE [--mode dry-run|preview|send] [--reviewer EMAIL]... [--preview-id UUID] [--attachment FILE [--attachment-name NAME]] [--addr HOST:PORT]
```

| Command | Without a write flag | With its write flag |
|---|---|---|
| `redrive-dead` | Lists DEAD messages | `--execute` resets them to QUEUED, and [reconcile](guides.md#operate-it) dispatches them |
| `unsuppress` | Shows the address's suppression | `--execute` lifts it |
| `render` | Prints the rendered email or Slack blocks | `--send --to` delivers it once, with the Slack token of `--org` for Slack, and is refused when `OUTBOX_COMMS_ENV` starts with `prod` or `production` as a delimited environment name (for example `production-eu`) |
| `send` | `--mode dry-run`, the default, prints the audience | `--mode preview --reviewer …`, then `--mode send --preview-id` |

`--addr` is the service's gRPC address, `localhost:9090` by default. Exit codes are 0, 1 for a failure and 2 for a usage error. The image has no shell, so copy a content file such as [examples/notice.json](../examples/notice.json) in with `docker compose cp examples/notice.json outbox-comms:/tmp/notice.json` and pass `--content /tmp/notice.json`, or run the command from a clone with `go run ./cmd/outbox-comms admin render`.

`send` refuses unless the preview is for the same organization, usecase, content, audience and attachment, is at most 24 hours old, and reached a reviewer. Without `--attachment`, `--mode send` sends the file the preview carried.

It submits in chunks of 1000 under keys derived from the preview, so a re-run sends nothing twice. A re-run that finishes a send cut off midway is allowed past the 24 hours, because a new preview would mean new keys and the submitted chunks sent again.

## Metrics and alerts

`/metrics` exports these, and no label carries an organization:

| Metric | What it counts |
|---|---|
| `outbox_comms_message_transitions_total` | Messages reaching an outcome status, by usecase, channel and status. The outcome statuses are `SENT`, `DELIVERED`, `FAILED`, `DEAD`, `UNCONFIRMED`, `SUPPRESSED`, `BOUNCED` and `SKIPPED` |
| `outbox_comms_dead_total` | Messages that went `DEAD`, by reason class: `transient_exhausted`, `crash_exhausted`, `permanent_config` or `render_bug` |
| `outbox_comms_unconfirmed_total` | Messages that ended `UNCONFIRMED`, by reason |
| `outbox_comms_orphan_feedback_total` | SES feedback events with no matching message |
| `outbox_comms_feedback_dropped_total` | SES feedback dropped on arrival because it could not be read, by reason: `malformed_json` or `missing_message_id` |
| `outbox_comms_scheduler_enqueue_failures_total` | Ticks of the reconcile or prune schedule the scheduler could not enqueue, by task. Each is also logged at `ERROR` |
| `outbox_comms_dispatch_duration_seconds` | Each provider call's duration, by channel |
| `outbox_comms_delivery_latency_seconds` | Time from fan-out to the provider's acceptance, by channel |
| `outbox_comms_overdue_pending` | `FAILED` rows more than 5 minutes past their retry time |
| `outbox_comms_bounce_rate` | Hard bounces per email send over 24 hours |
| `outbox_comms_complaint_rate` | Complaints per email send over 24 hours |
| `outbox_comms_reputation_sample_sends` | Email sends in that window, the sample both rates rest on |

[monitoring/alerts.yaml](../monitoring/alerts.yaml) holds five Prometheus alerts with promtool tests, and [monitoring/grafana-dashboard.json](../monitoring/grafana-dashboard.json) a dashboard.
