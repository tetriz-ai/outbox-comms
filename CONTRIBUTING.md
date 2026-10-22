# Contributing

Bug reports, fixes and new usecases are welcome. Open an issue first for anything larger than
a fix, so the change can be agreed before you write it.

## Build and test

Use a current patched Go 1.26 or 1.27 release. `go.mod` selects Go 1.27.3 by default; CI uses `GOTOOLCHAIN=local` to check both supported series.

```sh
go build ./... && go vet ./... && go test -race -count=1 ./...
```

Store-backed tests skip without a Postgres and a Redis. Their packages then print `ok` after
running nothing, and only a `testdb: SKIPPING` line says so, which only `go test -v` shows. Set
`OUTBOX_COMMS_REQUIRE_TEST_DB=1` to make a missing database fail the run. To run them, start both
on local ports and point the suite at them:

```sh
docker run -d --name outbox-comms-test-pg -p 127.0.0.1:15432:5432 -e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox-local-only -e POSTGRES_DB=outbox_comms docker.io/library/postgres:18-alpine
docker run -d --name outbox-comms-test-redis -p 127.0.0.1:16379:6379 docker.io/library/redis:8.10-alpine
until docker exec outbox-comms-test-pg pg_isready -h 127.0.0.1 -U outbox -d outbox_comms >/dev/null 2>&1; do sleep 1; done
export OUTBOX_COMMS_TEST_DATABASE_URL='postgres://outbox:outbox-local-only@localhost:15432/outbox_comms?sslmode=disable'
export OUTBOX_COMMS_TEST_REDIS_ADDR=localhost:16379
export OUTBOX_COMMS_REQUIRE_TEST_DB=1
go test -race -count=1 ./...
```

With `OUTBOX_COMMS_REQUIRE_TEST_DB=1`, a missing database fails the suite instead of skipping.
Each test package migrates a fresh schema and drops it at the end, so the suite never touches
the data in the database's default schema.

Run one suite at a time against a Redis, because the server tests flush its database 9. Remove
the two containers with `docker rm -f -v outbox-comms-test-pg outbox-comms-test-redis`.

## Checks to run before a pull request

CI runs build, vet and race tests on Go 1.26.x and 1.27.x with real Postgres and Redis services, plus lint, vulnerability, secret, proto, shell and alert checks. Run the checks below locally too and say in the pull request which you ran; CI also runs the compose examples, acceptance and E2E checks; image scanners remain local.

| Check | What it runs |
|---|---|
| Test (Go 1.26.x and 1.27.x) | `go build ./... && go vet ./... && go test -race -count=1 ./...`, against a Postgres and a Redis |
| Lint | `golangci-lint run` with golangci-lint v2.14.0, `shellcheck scripts/*.sh examples/*/run.sh` with shellcheck 0.11.0, and `hadolint Dockerfile` |
| Proto | `buf lint && buf generate && git diff --exit-code -- gen/` |
| Vulnerability check | `go install golang.org/x/vuln/cmd/govulncheck@v1.8.0`, then `govulncheck ./...`, on Go 1.27.x |
| Third-party licenses | `go install github.com/google/go-licenses/v2@v2.0.1`, then `scripts/third-party-licenses.sh --check` |
| Alert rules | `promtool check rules monitoring/alerts.yaml && promtool test rules monitoring/alerts_test.yaml` |
| Image | `docker build -t outbox-comms:ci .`, then Trivy on the image, failing on a HIGH or CRITICAL finding that has a fix |
| Examples | `docker compose up -d --build --wait`, then `scripts/run-examples.sh` |
| Acceptance | `scripts/acceptance.sh` |
| E2E | `scripts/e2e.sh` |

Install the linter with
`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`. Do not silence a
finding inline. Fix it, or change `.golangci.yml` with a comment saying why.

Install shellcheck 0.11.0 with these lines, for Linux x86_64. On macOS, run
`brew install shellcheck` and check that `shellcheck --version` says 0.11.0. Elsewhere, take the
matching asset from the same release.

```sh
curl -fsSLO https://github.com/koalaman/shellcheck/releases/download/v0.11.0/shellcheck-v0.11.0.linux.x86_64.tar.xz
echo "8c3be12b05d5c177a04c29e3c78ce89ac86f1595681cab149b65b97c4e227198  shellcheck-v0.11.0.linux.x86_64.tar.xz" | sha256sum -c -
tar xJf shellcheck-v0.11.0.linux.x86_64.tar.xz --strip-components=1 shellcheck-v0.11.0/shellcheck
```

They leave the binary in the current directory. Put it on your `PATH` to run the command in
the table as written.

## Running the stack

With Podman, use `podman compose`. If its docker-compose provider cannot find the Podman
machine, point it at the machine's socket:

```sh
export DOCKER_HOST=unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')
```

A second stack can run beside the first. It needs its own project name, its own ports, and
its own image name, so its build does not replace the first stack's image:

```sh
export OUTBOX_COMMS_IMAGE=outbox-comms:second OUTBOX_COMMS_GRPC_PORT=19090 OUTBOX_COMMS_HTTP_PORT=18080 OUTBOX_COMMS_STUB_PORT=18090 OUTBOX_COMMS_MAILPIT_PORT=18025
docker compose -p outbox-comms-second up -d --build --wait
```

Run the examples against it with the same variables exported, and stop it with
`docker compose -p outbox-comms-second down -v`. Run the acceptance test against a named
project with `COMPOSE="docker compose -p <project>" scripts/acceptance.sh`.

`scripts/e2e.sh` runs the end-to-end lifecycle test, `e2e/e2e_test.go` (build tag `e2e`), in
about three minutes. It starts Postgres, Redis, Mailpit, the Slack stub, the fake SES
(`cmd/fake-ses`) and ElasticMQ in their own compose project, `outbox-comms-e2e`, on their own
ports, so a local stack keeps running. It runs the service on the host from `e2e/server`, kills
and restarts it, and removes the project's containers and volumes at the end.

The script reads a few variables, which its header lists in full:

- **`COMPOSE`:** the compose command, such as `COMPOSE='podman compose'`.
- **`E2E_PROJECT` and `E2E_PORT_OFFSET`:** a second run beside the first needs its own project
  name and a port offset, such as `E2E_PROJECT=outbox-comms-e2e-2 E2E_PORT_OFFSET=100`.
- **`E2E_KEEP=1`:** leaves the stack up for the next run.

A test scripts failures through the stub's and the fake SES's `/control/faults` endpoints,
which the package docs of `internal/slackstub` and `internal/fakeses` describe. They are for
local use only. ElasticMQ is an image that compose pulls, and the repository does not
ship it.

Podman finds its machine through your home directory, so run `podman` and `podman compose` with
your real `HOME`, not a temporary one.

`scripts/run-examples.sh` runs each example's command from its README and compares standard
output with the README's last block. It first deletes every request, message and delivery
event in the local stack, and empties the Slack stub and Mailpit, because the examples use
fixed idempotency keys and would otherwise send nothing on a reused stack. With Podman, run it
as `COMPOSE='podman compose' scripts/run-examples.sh`.

A new example gets its own folder, a README in the same three parts, and a row in
`examples/README.md`. `scripts/run-examples.sh` fails when a folder under `examples/` has no
row.

After a dependency change, run `scripts/third-party-licenses.sh` to refresh
`third_party/licenses/`, then update the module list in `NOTICE`.

## What the tests cover

The suite covers the API's validation, every status move, the reconcile checks, the senders' error
handling, the Slack client, the scheduler's leader election and the operator commands.
`TestEndToEnd` and its neighbours run the whole service in-process.

`scripts/acceptance.sh` deletes all messages in the local stack, and puts the normal stack back
when it ends. It kills the service while the Slack stub holds its post, restarts it, and checks
that the stub ends with one post and the row ends `DELIVERED`. The stub drops a post whose sender
disconnected, so the test shows recovery from a crash before Slack answered, not exactly-once
delivery.

With Podman, run it as `COMPOSE='podman compose' scripts/acceptance.sh`. To keep your own compose
override, name it in `COMPOSE_EXTRA`, as in `COMPOSE_EXTRA=compose.override.yaml scripts/acceptance.sh`.

## Pull requests

- **Tests:** every behaviour change comes with a test that fails before it and passes after.
- **Changelog:** add a line under `Unreleased` in `CHANGELOG.md` for anything a user would
  notice. Tests and docs changes need none.
- **Commits:** a short imperative subject (`Retry Slack 429 on the ladder`), and a body that
  says why when it is not obvious.
- **Generated code:** `gen/` comes from `buf generate`, apart from its hand-written
  `doc.go`. Change the protos, never the generated files.

## Licensing of contributions

No sign-off line and no separately signed agreement are needed. By opening a pull request you
agree that your contribution is released under the project's [LICENSE](LICENSE).
