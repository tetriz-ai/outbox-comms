# syntax=docker/dockerfile:1
# Copyright 2026 Workloom Solutions Private Limited
# SPDX-License-Identifier: MIT
FROM docker.io/library/golang:1.27.3-alpine@sha256:d530cf60723abf1ffae0cd16bb546b379ec0550d2f3815e5b16ed69dbc00fba4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/tetriz-ai/outbox-comms/cli.Version=${VERSION}" -o /out/outbox-comms ./cmd/outbox-comms \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/slack-stub ./cmd/slack-stub \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/fake-ses ./cmd/fake-ses \
 && mkdir -p /out/data/attachments

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime
LABEL org.opencontainers.image.source="https://github.com/tetriz-ai/outbox-comms" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="outbox-comms" \
      org.opencontainers.image.description="A gRPC service that stores each email and Slack send in Postgres first, then sends, retries and tracks it."
COPY --from=build /out/outbox-comms /usr/local/bin/
COPY --from=build --chown=65532:65532 /src/LICENSE /src/NOTICE /usr/share/doc/outbox-comms/
COPY --from=build --chown=65532:65532 /src/third_party/licenses /usr/share/doc/outbox-comms/licenses
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV OUTBOX_COMMS_GRPC_ADDR=:9090 OUTBOX_COMMS_HTTP_ADDR=:8080
EXPOSE 8080 9090
HEALTHCHECK --interval=5s --timeout=3s --retries=12 CMD ["/usr/local/bin/outbox-comms", "health"]
ENTRYPOINT ["/usr/local/bin/outbox-comms"]
CMD ["serve"]

# Local synthetic providers belong only to the compose demo image.
FROM runtime AS demo
COPY --from=build /out/slack-stub /out/fake-ses /usr/local/bin/

FROM runtime AS production
