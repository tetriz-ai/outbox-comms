# Changelog

All notable changes are recorded using [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-22

### Added

- Initial release.
- `SubmitComms` stores email and Slack requests in Postgres before asynchronous sending, retries and delivery tracking.
- SMTP, Amazon SES and Slack providers, with preferences, suppression and idempotency controls.
- Operator commands for previews, one-off sends, dead-message retries and suppression management.
- Loopback native listeners, optional gRPC reflection and embedding hooks for TLS credentials and interceptors.
- Production and local-demo container targets, runnable examples and store-backed tests.

[Unreleased]: https://github.com/tetriz-ai/outbox-comms/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/tetriz-ai/outbox-comms/releases/tag/v0.1.0
