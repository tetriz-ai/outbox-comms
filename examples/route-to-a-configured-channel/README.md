Sets the Slack channel that receives an organization's alerts, then submits an alert addressed to the organization's configured channel instead of a channel id. Use it when the producer knows who a message is for but an admin chooses where it lands; changing the channel later needs no producer change.

```sh
go run ./examples/route-to-a-configured-channel
```

```text
routed to C0ONCALL MESSAGE_STATUS_DELIVERED
```
