Checks the running stack with `curl` alone: the health endpoint, the sender-reputation gauges from `/metrics`, and the channels the Slack stub has received posts for in this run (after the examples above).

```sh
examples/check-from-the-shell/run.sh
```

```text
ok
outbox_comms_bounce_rate 0
outbox_comms_complaint_rate 0
"channel":"C0EXPORTS"
"channel":"C0ONCALL"
```
