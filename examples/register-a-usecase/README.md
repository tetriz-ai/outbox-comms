An outbox-comms binary of your own: it registers a usecase with a typed params schema and a Go renderer, then hands the command line to `cli.Main`, so every command knows the usecase. Here `admin render` renders it from sample params, offline, with no stack. The same binary run as `serve` is the service, sending the usecase too.

```sh
go run ./examples/register-a-usecase admin render --usecase invoice.paid --channel slack --params '{"number":"INV-1042","amount":"€120.00"}'
```

```text
[{"type":"section","text":{"type":"mrkdwn","text":"Invoice *INV-1042* is paid: €120.00"}}]
```
