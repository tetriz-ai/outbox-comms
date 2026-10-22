# Examples

| You want | Example |
|---|---|
| To send one message to an email address and a Slack channel and see it delivered | [send-a-notice](send-a-notice/) |
| To send to the channel an admin configured, not a channel id | [route-to-a-configured-channel](route-to-a-configured-channel/) |
| Opt-outs honoured, and transactional mail still sent | [respect-opt-outs](respect-opt-outs/) |
| To know, before building a message, whether each channel would send it and why not | [check-before-you-send](check-before-you-send/) |
| Your own usecase with a typed schema and a Go renderer | [register-a-usecase](register-a-usecase/) |
| To check the stack from a shell | [check-from-the-shell](check-from-the-shell/) |

`scripts/run-examples.sh` deletes the local stack's messages and empties the Slack stub and Mailpit, then runs them all, in this order, against the compose stack (`docker compose up -d --build --wait` first), and compares each one's output with the last block of its README exactly. No call leaves the machine: email goes to Mailpit and Slack posts to the local stub. On moved ports, export the same `OUTBOX_COMMS_*_PORT` variables you gave compose, and the examples follow.
