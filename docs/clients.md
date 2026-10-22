# Call it from any language

outbox-comms is a gRPC server, so any language with a gRPC library can call it. You do not install an outbox-comms package. You take the proto files, which describe every call, and generate a client or load them at run time.

Every snippet on this page sends one real message to the [quick-start](../README.md#try-it-locally) stack on `localhost:9090`.

## Get the proto files

The API lives in [proto/outboxcomms/v1/](../proto/outboxcomms/v1/), and it imports `buf/validate/validate.proto`. [buf](https://buf.build/docs/installation) exports both into one folder you can hand to any generator:

```bash
git clone https://github.com/tetriz-ai/outbox-comms
buf export outbox-comms -o protos
```

Go needs none of this, because the generated Go code ships in the module.

## Go

Add the module, then import the generated package, `github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1`:

```bash
go get github.com/tetriz-ai/outbox-comms@latest
```

[Guides](guides.md) has a complete program for each common job.

## The command line, with grpcurl

The local compose stack enables gRPC reflection (`OUTBOX_COMMS_GRPC_REFLECTION=true`); other deployments disable it by default. With reflection enabled, [grpcurl](https://github.com/fullstorydev/grpcurl) needs no proto files at all. It is the quickest way to try a call by hand:

```bash
grpcurl -plaintext -d '{
  "organization_id": "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
  "idempotency_key": "welcome:jane@example.com",
  "usecase": "notice",
  "content": {"base": {"subject": "Welcome aboard", "blocks": [{"paragraph": {"text": "Your account is ready."}}]}},
  "entries": [{"channel": "CHANNEL_EMAIL", "target_kind": "TARGET_KIND_EMAIL_ADDRESS", "recipient": "jane@example.com"}]
}' localhost:9090 outboxcomms.v1.CommsService/SubmitComms
```

```text
{
  "handle": "welcome:jane@example.com"
}
```

Ask where the message stands with the same handle:

```bash
grpcurl -plaintext -d '{"organization_id": "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", "handle": "welcome:jane@example.com"}' \
  localhost:9090 outboxcomms.v1.CommsService/GetCommsStatus
```

`grpcurl -plaintext localhost:9090 describe outboxcomms.v1.CommsService` lists every call.

## Python

Generate the client once with `grpcio-tools`, from the `protos` folder above:

```bash
pip install grpcio grpcio-tools
mkdir -p gen
python -m grpc_tools.protoc -I protos --python_out=gen --grpc_python_out=gen $(find protos -name '*.proto')
```

Then send:

```python
import sys

import grpc

sys.path.insert(0, "gen")
from outboxcomms.v1 import content_pb2, enums_pb2, messages_pb2, service_pb2_grpc  # noqa: E402

comms = service_pb2_grpc.CommsServiceStub(grpc.insecure_channel("localhost:9090"))

resp = comms.SubmitComms(messages_pb2.SubmitCommsRequest(
    organization_id="8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
    idempotency_key="password-reset:jane:2026-10-14T09:30",
    usecase="notice",
    content=content_pb2.MessageContent(base=content_pb2.RenderableContent(subject="Reset your password")),
    entries=[messages_pb2.CommsEntry(
        channel=enums_pb2.CHANNEL_EMAIL,
        target_kind=enums_pb2.TARGET_KIND_EMAIL_ADDRESS,
        recipient="jane@example.com",
    )],
))
print("handle:", resp.handle)
```

```text
handle: password-reset:jane:2026-10-14T09:30
```

## Node.js

`@grpc/proto-loader` reads the proto files at run time, so there is no build step:

```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

```js
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const definition = protoLoader.loadSync("outboxcomms/v1/service.proto", {
  includeDirs: ["protos"],
  enums: String,
});
const { CommsService } = grpc.loadPackageDefinition(definition).outboxcomms.v1;
const comms = new CommsService("localhost:9090", grpc.credentials.createInsecure());

comms.SubmitComms(
  {
    organizationId: "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
    idempotencyKey: "order-shipped:A-1001",
    usecase: "notice",
    content: { base: { subject: "Your order A-1001 has shipped" } },
    entries: [
      {
        channel: "CHANNEL_SLACK",
        targetKind: "TARGET_KIND_SLACK_CHANNEL",
        recipient: "C0ORDERS",
      },
    ],
  },
  (err, resp) => {
    if (err) throw err;
    console.log("handle:", resp.handle);
  },
);
```

```text
handle: order-shipped:A-1001
```

For TypeScript types, generate the client with a plugin such as `ts-proto` or `@bufbuild/protoc-gen-es` instead.

## Other languages

Java, Kotlin, C#, Ruby, Rust, PHP and the rest work the same way. Run `protoc` or `buf generate` with your language's gRPC plugin over the `protos` folder, as [grpc.io](https://grpc.io/docs/languages/) shows for each one.

## What every client should know

- **Organization:** every call carries an `organization_id`, a UUID you choose for each of your tenants. A single-tenant app can use one fixed UUID.
- **Idempotency key:** name the event, such as `invoice-paid:INV-1042`. Sending the same request again returns the same handle and sends nothing, so it is safe to retry a call that timed out.
- **Errors:** a bad request is `INVALID_ARGUMENT` and stores nothing. A key reused for a different request is `ALREADY_EXISTS`. [The gRPC API](reference.md#the-grpc-api) lists every error.
- **Status:** a handle can have many messages, one per recipient. Poll `GetCommsStatus` until `done` is true, or read one message's history with `GetMessageEvents`.
- **No authentication:** the server trusts every caller. Reach it only from your own private network, as [Access](production.md#access) explains.

## Why there is no npm or PyPI package

outbox-comms runs as its own service, next to your app, like a database. A package manager for one language would only wrap a server that every language reaches the same way, over gRPC.

What a Node or Python developer needs is a client, and the proto files above give one in any language. Ready-made client packages, generated from the same protos, may follow once the API reaches v1.
