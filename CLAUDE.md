# outbox-comms

## What this is

A gRPC service that stores each email and Slack send in Postgres first, then sends, retries
and tracks it. It picks no audience, schedules nothing, and has no authentication.

## Layout

- Public: `server/` runs it, `cli/` is the whole command line, `config/` reads the env,
  `templates/` registers and renders usecases, `gate/` holds the gates, `gen/` is from `proto/`.
- `internal/` holds the rest. Tests sit beside the code. `scripts/` runs `examples/`, acceptance and e2e.

## Commands

Go 1.26 is the floor (`golang.org/x/net` needs it). Tested on 1.26 and 1.27, other checks on 1.27.x.
Store-backed tests need both containers and the three variables, or they run nothing:

```sh
docker run -d --name outbox-comms-test-pg -p 127.0.0.1:15432:5432 -e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox-local-only -e POSTGRES_DB=outbox_comms docker.io/library/postgres:18-alpine
docker run -d --name outbox-comms-test-redis -p 127.0.0.1:16379:6379 docker.io/library/redis:8.10-alpine
until docker exec outbox-comms-test-pg pg_isready -h 127.0.0.1 -U outbox -d outbox_comms >/dev/null 2>&1; do sleep 1; done
export OUTBOX_COMMS_TEST_DATABASE_URL='postgres://outbox:outbox-local-only@localhost:15432/outbox_comms?sslmode=disable'
export OUTBOX_COMMS_TEST_REDIS_ADDR=localhost:16379
export OUTBOX_COMMS_REQUIRE_TEST_DB=1
go build ./... && go vet ./... && go test -race -count=1 ./...
```
CI checks both supported Go series against Postgres and Redis. Also run the local checks CONTRIBUTING.md lists:

| Check | Command |
|---|---|
| Lint | `golangci-lint run` (v2.14.0), `shellcheck scripts/*.sh examples/*/run.sh` (0.11.0), `hadolint Dockerfile` |
| Proto | `buf lint && buf generate && git diff --exit-code -- gen/` |
| Vulnerabilities | `go install golang.org/x/vuln/cmd/govulncheck@v1.8.0`, then `govulncheck ./...`, on Go 1.27.x |
| Licenses | `go install github.com/google/go-licenses/v2@v2.0.1`, then `scripts/third-party-licenses.sh --check` |
| Alert rules | `promtool check rules monitoring/alerts.yaml && promtool test rules monitoring/alerts_test.yaml` |
| Image | `docker build -t outbox-comms:ci .`, then Trivy, failing on a fixable HIGH or CRITICAL |
| Examples | `docker compose up -d --build --wait`, then `scripts/run-examples.sh` |
| Acceptance | `scripts/acceptance.sh` |
| E2E | `scripts/e2e.sh` (about 3 minutes; its own compose project and ports) |

- **Lint tools:** `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`.
  For shellcheck, CONTRIBUTING.md has the Linux lines. On macOS, `brew install shellcheck`.
- **Podman:** use `podman compose`, with your real `HOME`. If its docker-compose provider cannot
  find the machine, `export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')`.
- **Ports:** if 9090, 8080, 8090 or 8025 is taken, export its variable first (docs/reference.md, "Ports").
- **A second stack** needs `-p`, its own ports and `OUTBOX_COMMS_IMAGE` (CONTRIBUTING.md). Run
  acceptance on it with `COMPOSE="docker compose -p <project>" scripts/acceptance.sh`.
- **Agent allow list:** `.claude/settings.json` allows six commands as written: build, vet, test,
  lint, shellcheck, run-examples. They run the repo's code, so open untrusted PRs in a sandbox.

## Contract to keep

- `internal/api/service_test.go`: a repeated key never sends again (`TestSubmitCommsIdempotent`);
  a bad submit is `InvalidArgument`, nothing stored or enqueued (`TestSubmitCommsRejections`).
- `internal/dispatch/dispatch_test.go`: a duplicated trigger never sends again
  (`TestDispatchCASMiss`), and a suppressed address is never mailed (`TestDispatchSuppressed`).
- Every status change names the status it moves from: `internal/dao/transition_test.go`.
- The gRPC API has no authentication: `TestEndToEnd` in `server/server_test.go` dials with none.
- No test reads real AWS credentials: `TestMain` in `server/server_test.go` and
  `internal/admin/admin_test.go` clears `AWS_*` and makes any AWS config read panic.

## Making a change

- Write the test first: a behaviour change needs a test that fails before and passes after.
- Add a line under `Unreleased` in `CHANGELOG.md` for anything a user would notice.
- Never silence a lint finding inline. Fix it, or change `.golangci.yml` with a comment.
- An example's README has three parts: a prose paragraph, an `sh` command block, and a `text`
  stdout block that `scripts/run-examples.sh` matches exactly. Index it in `examples/README.md`.
- Examples run in index order, after the runner resets the local stack. `check-from-the-shell`
  lists every channel posted to in the run, so an example with a new channel updates its output.
- After a dependency change, run `scripts/third-party-licenses.sh` and update `NOTICE`.

## Do not

- Do not edit the generated files in `gen/` (only `doc.go` is hand-written). Change `proto/`, run `buf generate`.
- Do not point tests or examples at real SES, SQS, S3 or Slack. Use Mailpit, the stub, the fake SES and ElasticMQ.
- Do not bound an update with `IN (SELECT ... LIMIT n)`. Use the CTE in `internal/dao/reconcile_repo.go`.
- Do not run two test suites at once on one Redis. The server tests flush its database 9.
