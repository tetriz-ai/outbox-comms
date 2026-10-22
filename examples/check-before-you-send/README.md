Asks, before any message is built, whether an alert to one person would go out on email and on the organization's configured Slack channel, and why not where it would not. It checks each channel as the entry the send would carry: the email with `TARGET_KIND_EMAIL_ADDRESS` and the person's id, so her preferences count, and Slack with `TARGET_KIND_CONFIGURED_DESTINATION` and no person id, so the organization's channel is the one looked up. It checks a fresh organization, then the same organization after an admin sets the alert channel and the person turns alert email off, then a transactional notice, which skips preferences. The check is advisory, because the dispatcher decides again at send time, and the example purges its own organization first, so every run prints the same lines.

```sh
go run ./examples/check-before-you-send
```

```text
alert, fresh organization
  CHANNEL_EMAIL eligible
  CHANNEL_SLACK blocked [no_destination]
alert, after the channel is set and Jane opts out of alert email
  CHANNEL_EMAIL blocked [opted_out]
  CHANNEL_SLACK eligible
notice, which is transactional
  CHANNEL_EMAIL eligible
```
