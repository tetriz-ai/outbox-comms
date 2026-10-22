<picture>
  <source media="(prefers-color-scheme: dark)" srcset=".github/assets/banner-dark.svg">
  <img alt="outbox-comms: a gRPC service that stores each email and Slack send in Postgres first, then sends, retries and tracks it" src=".github/assets/banner-light.svg" width="880">
</picture>

[![Go Reference](https://pkg.go.dev/badge/github.com/tetriz-ai/outbox-comms.svg)](https://pkg.go.dev/github.com/tetriz-ai/outbox-comms)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Go 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8.svg)

**outbox-comms sends your app's email and Slack messages, records each one before it sends it, and tracks what happened to it.**

Your app makes one call: "send this to these people." outbox-comms saves that request in Postgres before anything else happens, then delivers it, retries what fails, and lets you ask at any time what happened to each message.

<img src=".github/assets/demo.gif" width="1000" alt="Local email and Slack sends reach tracked statuses; submitting the same request key again creates no new messages.">

One request, tracked email and Slack sends, and a safe retry of the same request key. [Demo details](.github/assets/demo/README.md).

## Why you might want it

Sending a message looks easy until something goes wrong halfway:

- **A crash loses the message.** The worker dies before the email goes out, and nobody notices.
- **A retry sends it twice.** Your app timed out and called again, so the message went out once per call.
- **Nobody can say if it arrived.** Support asks "did the customer get the reset email?" and there is no record.

outbox-comms handles all three. Every send is a database row before it is anything else, so a crash never drops a request: a reconcile job finds it and drives it on. Your app can retry a request safely, because each carries a key you choose and a repeat sends nothing new. And every message ends in a status you can look up.

One limit no service can remove: when a provider's answer never comes back, or a worker dies mid-send, nobody knows whether the message went out, because neither SES nor Slack takes an idempotency key. You choose per message what happens then. `GUARANTEED` sends again and may deliver twice; `BEST_EFFORT` does not and may miss one, ending `UNCONFIRMED`. [Before you rely on it](docs/production.md) lists each case.

```mermaid
flowchart LR
    app["Your app"] -- "SubmitComms" --> svc["outbox-comms"]
    svc -- "saves first" --> pg[("Postgres")]
    svc -- "queues" --> q[("Redis or SQS")]
    q --> w["Workers"]
    w --> email["Email: SMTP or SES"]
    w --> slack["Slack"]
    app -- "GetCommsStatus" --> svc
```

## Is it a good fit?

**Use it when** you send transactional messages (receipts, password resets, alerts, reports) and want each one recorded, retried and traceable, with an explicit choice between risking a duplicate and risking a gap. It suits a team with many services, or many customer organizations, that wants one place to send from and one place to check.

**Look elsewhere when** you need marketing email with open tracking and unsubscribe links, scheduled sends, or SMS and push. outbox-comms also picks no audience: your app names every recipient.

## Try it locally

You need Docker with Compose (or Podman) and Go 1.26 or later. No account is needed: email goes to [Mailpit](https://mailpit.axllent.org), a local inbox, and Slack posts go to a local stub.

```sh
git clone https://github.com/tetriz-ai/outbox-comms && cd outbox-comms
docker compose up -d --build --wait
go run ./examples/send-a-notice
```

```text
handle: export-ready:2026-10-01
CHANNEL_EMAIL jane@example.com MESSAGE_STATUS_SENT
CHANNEL_SLACK C0EXPORTS MESSAGE_STATUS_DELIVERED
```

That sent one email and one Slack post, from [examples/send-a-notice/main.go](examples/send-a-notice/main.go). Open http://127.0.0.1:8025 to read the email, and http://127.0.0.1:8090/messages to see the Slack post.

Now run the program again. It prints the same lines, but Mailpit still holds just one email, because the request's key, `export-ready:2026-10-01`, was already used.

The first `up` builds the image and takes a few minutes. The stack listens on ports 9090, 8080, 8090 and 8025 on 127.0.0.1, and [Ports](docs/reference.md#ports) shows how to move one that is taken. With Podman, type `podman compose` wherever this page says `docker compose`.

When you are done:

```sh
docker compose down -v
```

## How it works

<details>
<summary>Implementation flow</summary>

You send one **request**. It names a message type (a **usecase**), the content, and up to 1000 recipients. outbox-comms turns it into one **message** per recipient and delivers each on its own.

- **The key:** every request carries an idempotency key you pick, such as `invoice-paid:INV-1042`. Sending the same request with the same key again changes nothing, so your app can retry freely. The key removes your app's duplicates, not a provider's: that is the guarantee below.
- **The status:** each message moves from `QUEUED` to a final status such as `DELIVERED`, `SENT`, `DEAD` or `SKIPPED`. [Message lifecycle](docs/reference.md#message-lifecycle) lists them all.
- **The guarantee:** when a send's outcome is unknown, `GUARANTEED` retries it and `BEST_EFFORT` does not. So the first may send a message twice, and the second may miss one.
- **The extras:** people can opt out of message categories, an organization can pick which Slack channel its messages land in, and an address that hard-bounces or complains on Amazon SES is never mailed again.

Three usecases ship built in: `notice` (transactional, so it ignores opt-outs), `alert` and `digest` (both respect them). You can add your own in Go, as [Register a usecase](docs/guides.md#register-a-usecase) shows.


</details>

## Use it from your app

outbox-comms speaks gRPC, so your app can be written in any language. These snippets run against the stack from [Try it locally](#try-it-locally).

### From Go

```go
// Command send-a-message submits one notice to an email address and prints its handle.
package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

func main() {
	addr := "127.0.0.1:" + cmp.Or(os.Getenv("OUTBOX_COMMS_GRPC_PORT"), "9090")
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	resp, err := pb.NewCommsServiceClient(conn).SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
		IdempotencyKey: "invoice-paid:INV-1042",
		Usecase:        "notice",
		Content:        &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Invoice INV-1042 is paid"}},
		Entries: []*pb.CommsEntry{{
			Channel:    pb.Channel_CHANNEL_EMAIL,
			TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS,
			Recipient:  "jane@example.com",
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("handle:", resp.GetHandle())
}
```

```text
handle: invoice-paid:INV-1042
```

The handle is your key. Pass it to `GetCommsStatus` to see where each message stands.

### From Python, Node.js or anything else

Generate a client from the [proto files](proto/outboxcomms/v1/) with your language's gRPC tools. [Call it from any language](docs/clients.md) walks through Python, Node.js and grpcurl, with code that runs as written.

```python
resp = comms.SubmitComms(messages_pb2.SubmitCommsRequest(
    organization_id="8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
    idempotency_key="password-reset:jane:2026-10-14T09:30",
    usecase="notice",
    content=content_pb2.MessageContent(base=content_pb2.RenderableContent(subject="Reset your password")),
    entries=[messages_pb2.CommsEntry(
        channel=enums_pb2.CHANNEL_EMAIL,
        target_kind=enums_pb2.TARGET_KIND_EMAIL_ADDRESS,
        recipient="jane@example.com",
    )],
))
```

### Common jobs

| You want to | Read | Runnable example |
|---|---|---|
| Send to people now and check on it | [Send a message](docs/guides.md#send-a-message) | [send-a-notice](examples/send-a-notice/) |
| Let each customer pick their Slack channel | [Route to a configured channel](docs/guides.md#route-to-a-configured-channel) | [route-to-a-configured-channel](examples/route-to-a-configured-channel/) |
| Honour opt-outs | [Respect opt-outs](docs/guides.md#respect-opt-outs) | [respect-opt-outs](examples/respect-opt-outs/) |
| Ask "would this send?" before building a costly message | [Respect opt-outs](docs/guides.md#respect-opt-outs) | [check-before-you-send](examples/check-before-you-send/) |
| Render your own message type in Go | [Register a usecase](docs/guides.md#register-a-usecase) | [register-a-usecase](examples/register-a-usecase/) |
| Re-send dead messages, or check health and metrics | [Operate it](docs/guides.md#operate-it) | [check-from-the-shell](examples/check-from-the-shell/) |
| Delete a customer who leaves | [The gRPC API](docs/reference.md#the-grpc-api), `PurgeOrgComms` | none |

## Run it for real

outbox-comms is one program. It needs a Postgres database, a Redis (or Amazon SQS), an email relay (SMTP or Amazon SES) and, for Slack, a bot token. It creates its own tables when it starts.

### Get it

| Way | What you get | Best for |
|---|---|---|
| Container image | `ghcr.io/tetriz-ai/outbox-comms:<version>`, for amd64 and arm64 | Docker, Kubernetes, ECS and most deployments |
| Prebuilt binary | `outbox-comms` for Linux, macOS and Windows, on the [Releases](https://github.com/tetriz-ai/outbox-comms/releases) page | A VM or bare metal |
| `go install` | `go install github.com/tetriz-ai/outbox-comms/cmd/outbox-comms@latest` | Go developers trying it out |
| Your own binary | A Go program that imports `cli` and `templates` and adds your usecases | Teams with message types of their own, as [Register a usecase](docs/guides.md#register-a-usecase) shows |

There is no npm or PyPI package, and none is needed. outbox-comms is a service your app talks to over the network, like a database, so your app needs only a gRPC client, which [Call it from any language](docs/clients.md#why-there-is-no-npm-or-pypi-package) explains.

### Configure it

Everything is set with `OUTBOX_COMMS_*` environment variables, and `outbox-comms config` prints them all with their defaults. Here is the service in a compose file next to your app, with the settings most deployments change:

```yaml
services:
  outbox-comms:
    image: ghcr.io/tetriz-ai/outbox-comms:v0.1.0
    environment:
      OUTBOX_COMMS_ENV: production
      OUTBOX_COMMS_DATABASE_URL: postgres://outbox:${PG_PASSWORD}@db.internal:5432/outbox_comms
      OUTBOX_COMMS_REDIS_ADDR: redis.internal:6379
      OUTBOX_COMMS_SMTP_ADDR: smtp.example.com:587
      OUTBOX_COMMS_SMTP_USERNAME: outbox
      OUTBOX_COMMS_SMTP_PASSWORD: ${SMTP_PASSWORD}
      OUTBOX_COMMS_EMAIL_FROM: Acme <noreply@acme.example>
      OUTBOX_COMMS_BRAND_NAME: Acme
      OUTBOX_COMMS_SLACK_BOT_TOKEN: ${SLACK_BOT_TOKEN}
    # No ports published: only services on this private network can reach it.
    expose: ["9090", "8080"]
```

[Configuration](docs/reference.md#configuration) covers the rest, including Amazon SES, SQS, S3 attachments and per-customer Slack tokens. Port 8080 serves `/healthz` for health checks and `/metrics` for Prometheus, and [monitoring/](monitoring/) has ready-made alerts and a Grafana dashboard.

### Before you send to real people

- **Keep it private.** The API has no login of its own, so anyone who can reach port 9090 can send as any organization. Put it on a private network or behind your own auth.
- **`GUARANTEED` can send twice.** Neither SES nor Slack accepts an idempotency key, so a send whose answer was lost is sent again. Use `BEST_EFFORT` for messages where a duplicate is worse than a gap.
- **SMTP never confirms delivery.** Email over SMTP stops at `SENT`, and only SES reports `DELIVERED`, bounces and complaints.
- **A bounce blocks the address everywhere.** Suppression is shared by all organizations.

[Before you rely on it](docs/production.md) lists every known limit, each pinned by a named test.

## Documentation

| Page | What is in it |
|---|---|
| [Guides](docs/guides.md) | Complete programs for each common job |
| [Call it from any language](docs/clients.md) | Go, Python, Node.js and grpcurl clients |
| [Reference](docs/reference.md) | Every RPC, status, setting, port, operator command and metric |
| [Before you rely on it](docs/production.md) | The limits that matter in production |
| [Architecture](docs/architecture.md) | How it works inside |
| [Examples](examples/README.md) | Six programs, each checked against its expected output |
| [Changelog](CHANGELOG.md) | What changed in each release |

## Contributing

Contributions of every size are welcome, from a typo fix to a new sender. Good places to start:

- **Try the quick start** and open an issue where anything was unclear. A confused newcomer is the best docs review there is.
- **Add an example** for a job the [Common jobs](#common-jobs) table does not cover.
- **Pick up an issue** from the [issue list](https://github.com/tetriz-ai/outbox-comms/issues).

Your first build and test run:

```sh
go build ./...
go vet ./...
go test ./...
```

A plain `go test ./...` skips the tests that need Postgres and Redis. [CONTRIBUTING.md](CONTRIBUTING.md) shows how to start both in two `docker run` lines, and lists every check to run before a pull request.

| Folder | What lives there |
|---|---|
| `server/`, `cli/`, `config/` | Starting the service, the command line, and the settings |
| `templates/`, `gate/` | Message types and their renderers, and the per-organization switches |
| `proto/`, `gen/` | The API definition, and the Go code `buf generate` makes from it |
| `internal/` | Everything else: the API, the queue, the senders, the database layer |
| `examples/`, `scripts/` | Runnable examples, and the scripts that check them end to end |

Please read the [Code of Conduct](CODE_OF_CONDUCT.md). To report a security problem privately, follow [SECURITY.md](SECURITY.md).

## Project

**Versions.** The current release is v0.1.0, and releases follow [semantic versioning](https://semver.org). Before v1, a minor release may change the API, the settings or the Go packages, and [CHANGELOG.md](CHANGELOG.md) says how. Database changes ship as migrations that run when the service starts.

**Releases.** The release workflow publishes the container image `ghcr.io/tetriz-ai/outbox-comms:<version>` for amd64 and arm64, and attaches `outbox-comms` binaries for Linux, macOS and Windows on amd64 and arm64, each archive with the license files. The quick start builds its own image from your clone, tagged `outbox-comms:local`. Once that version exists in the registry, add the release override to use it. The local synthetic providers still build from the demo target:

```sh
docker compose -f compose.yaml -f compose.release.yaml up -d
```

**Go.** The module supports Go 1.26 and 1.27. Use the latest patched release of either series; `go.mod` selects Go 1.27.3 by default, and the production image builds with that patch. CI checks both series with toolchain switching disabled.

**License.** [MIT](LICENSE). Copyright 2026 Workloom Solutions Private Limited. [CONTRIBUTORS.md](CONTRIBUTORS.md) lists the people who have contributed to this project, and the AI coding agents used on its code.
