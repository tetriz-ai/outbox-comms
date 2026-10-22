# Before you rely on it

Read this before you send to real people. It lists the limits that matter. Each is inherent to the providers or chosen by design, and a named test pins it.

## Delivery

**Sends twice (harmful):**

- A `GUARANTEED` send whose answer is lost after the request left is retried, and the provider may have sent the first one, since neither SES nor Slack takes an idempotency key. That is any uncertain send, as [Message lifecycle](reference.md#message-lifecycle) defines it. Pinned by the "uncertain GUARANTEED" case of `TestDispatchOutcomes` in `internal/dispatch/dispatch_test.go` and the reset cases of `TestTransportFailureAfterTheRequestLeft` in `internal/dispatch/transport_test.go`.
- A worker killed after the provider accepted a `GUARANTEED` send, but before the row was updated, leaves the row `SENDING`, and reconcile sends it again. Each requeue counts as an attempt, so a worker that dies every time sends at most `OUTBOX_COMMS_RETRY_MAX_ATTEMPTS` copies (6 by default) before the message ends `DEAD` with `crash_exhausted`. Pinned by `TestCrashRecovery` in `server/server_test.go` and `TestCrashRequeuesCountTowardTheCap` in `internal/reconcile/reconcile_test.go`.
- Deduplication is per request. The same recipient in two requests with different keys gets two messages. Pinned by `TestNoCrossRequestDedup` in `internal/fanout/fanout_test.go`.

**Misses a send:**

- A `BEST_EFFORT` send whose answer is lost after the request left stops at `UNCONFIRMED` and is not retried, though it may never have arrived. Pinned by the "uncertain BEST_EFFORT" case of `TestDispatchOutcomes` in `internal/dispatch/dispatch_test.go` and `TestTransportFailureAfterTheRequestLeft` in `internal/dispatch/transport_test.go`.
- Delivery events come only from SES feedback, so SMTP never reports one. A usecase that needs `CONFIRMED` tracking ends `UNCONFIRMED` after 30 minutes on SMTP. Pinned by `TestReconcileArms` in `internal/reconcile/reconcile_test.go`.
- Slack's acceptance counts as delivery, so a Slack message is `DELIVERED` once Slack answers and never waits for a later event. An `ok: true` answer with no `ts` or file id still counts, and the row then has no provider message id. Pinned by the "slack success" case of `TestDispatchOutcomes` in `internal/dispatch/dispatch_test.go` and the `ok_without_id` cases of `TestSlackStatusAnswers` in `internal/sender/transport_test.go`.

## Access

**Allows what it should deny (harmful):**

- The gRPC API has no authentication, so anyone who reaches port 9090 can send as any organization, read its history, or purge it. Run it on a private network or behind your own auth before anyone else can reach it. Pinned by `TestEndToEnd` in `server/server_test.go`, which calls it with no credentials.
- `PurgeOrgComms` leaves a trace: for each email the organization had sent, it keeps the provider message id and the lowercased address, with no organization, for `OUTBOX_COMMS_PURGE_TOMBSTONE_TTL` (30 days), so a late bounce or complaint still suppresses the address and the prune job then deletes them. This trace lets late provider events suppress bounced or complaining addresses after the organization has been deleted. Pinned by `TestPurgeLeavesATombstone` in `internal/feedback/feedback_test.go` and `TestPruneDeletesExpiredTombstones` in `internal/reconcile/reconcile_test.go`.
- Organizations are separated by an explicit `organization_id` filter on every query, not by Postgres row-level security. A query that forgot the filter would read across organizations. Pinned by `TestOrgScoping` in `internal/dao/dao_test.go`.

**Denies what it should allow:** None known.

## Limits

**Misses what it should catch (harmful):**

- **Reputation rates** cover the last 24 hours and read 0 below `OUTBOX_COMMS_REPUTATION_MIN_SENDS` email sends in that window, 100 by default, so bounces in a small batch trip no alert. The sample floor avoids treating a handful of sends as a reliable rate, and `outbox_comms_reputation_sample_sends` shows the sample. Pinned by `TestReconcileSetsGauges` in `internal/reconcile/bounds_test.go`.

**Refuses what it could send:**

- **Attachments:** an email's assembled message must fit SES's 10 MiB cap, and Base64 makes a file about a third larger, so a file over about 7 MiB ends `DEAD` without a send. `admin send` takes files up to 6 MiB. Pinned by `TestEmailOverCapAttachment` in `internal/sender/email_test.go`.
- **Suppression** is global. An address that hard-bounces for one organization is suppressed for all, and `PurgeOrgComms` leaves it. Pinned by `TestDispatchSuppressed` in `internal/dispatch/dispatch_test.go` and `TestPurgeKeepsSuppression` in `internal/api/service_test.go`.

## Network and provider transport

Native listeners default to `127.0.0.1:9090` (gRPC) and `127.0.0.1:8080` (health and metrics). The container listens on all interfaces; compose publishes its ports on host loopback only. A reachable gRPC port grants all API operations. Restrict network access and require authenticated, authorized access before exposing it, including authorization for each requested organization. Health and metrics also need network restrictions.

Embedded hosts can pass TLS credentials and authentication interceptors through `server.Options.GRPCServerOptions`. Validation remains installed. Set `OUTBOX_COMMS_GRPC_REFLECTION=true` only when discovery tools need it; the local demo opts in. The standalone CLI has no TLS or authentication configuration: put it behind a trusted authenticated proxy that authorizes organization access, or embed the server with those policies.

SMTP uses opportunistic STARTTLS when the relay advertises it. There is no require-TLS setting: message bodies can travel in cleartext when the relay does not offer STARTTLS, even though password authentication refuses an unsafe remote connection. Use a trusted local relay and enforce TLS on its onward delivery, or choose SES over HTTPS. Do not rely on this SMTP client to enforce encryption on an untrusted network.

There is no inbound request rate limiter or per-provider outbound rate limiter. Worker concurrency and retries bound parallel work, not requests or sends per second. Apply ingress quotas and provider-aware controls outside this service; provider throttling may consume retries and end a message `DEAD`.

## Stored content and retention

Requests retain `params`, `content` and `entries`; messages retain recipient addresses and full rendered bodies. These fields have no automatic retention deadline. The prune job strips only `provider_payload` after `OUTBOX_COMMS_PRUNE_PAYLOAD_AFTER` (90 days by default), and deletes expired purge tombstones. It does not remove request content, recipient addresses or rendered bodies.

Use `PurgeOrgComms` when an organization's content must be removed, and set your own retention policy for active organizations, backups and attachment storage. Purge preserves global suppression records and short-lived address/provider-id tombstones as described above; deleting rows does not erase prior backups. Protect database, backups and attachments as personal data, and limit access to the history API.
