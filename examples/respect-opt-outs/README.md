One person opts out of product email. A digest to them and a colleague is sent to the colleague only, and the request reports itself partial, with the reason on the skipped row; a transactional notice to the same person still goes through, because transactional usecases bypass preferences. Each entry names its subject person, so the person's own preference applies.

```sh
go run ./examples/respect-opt-outs
```

```text
digest (partial=true)
  jane@example.com MESSAGE_STATUS_SKIPPED (opted_out)
  john@example.com MESSAGE_STATUS_SENT
notice (partial=false)
  jane@example.com MESSAGE_STATUS_SENT
```
