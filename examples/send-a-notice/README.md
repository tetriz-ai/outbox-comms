This is the quick start's program. It sends one notice to an email address and a Slack channel, then prints its status. Use it to see the whole path once, submit to delivery, against the compose stack. The email lands in Mailpit (http://127.0.0.1:8025) and the post in the Slack stub (http://127.0.0.1:8090/messages); running it again with the same idempotency key sends nothing more.

```sh
go run ./examples/send-a-notice
```

```text
handle: export-ready:2026-10-01
CHANNEL_EMAIL jane@example.com MESSAGE_STATUS_SENT
CHANNEL_SLACK C0EXPORTS MESSAGE_STATUS_DELIVERED
```
