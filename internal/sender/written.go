// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
)

// ErrMaybeSent marks a provider call that failed after its request was written
// in full, with no provider error code to say what happened. The provider may
// have acted on it, so the send is uncertain.
var ErrMaybeSent = errors.New("the request reached the provider and no usable answer came back")

// ErrNotSent marks a failure in a step that comes before the message is
// posted, such as the first two steps of a Slack file upload. Nothing was
// delivered, whatever the step's own transport did.
var ErrNotSent = errors.New("the message was not posted")

// TraceWrites returns ctx with an httptrace hook, and a function that reports
// whether any HTTP request made under that context was written in full. A
// failed or partial write does not count: the provider cannot act on a
// request it did not get whole.
func TraceWrites(ctx context.Context) (context.Context, func() bool) {
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			wrote.Store(true)
		}
	}}
	return httptrace.WithClientTrace(ctx, trace), wrote.Load
}

// writeRecorder holds whether the provider's own request was written in full.
type writeRecorder struct{ wrote atomic.Bool }

type recorderKey struct{}

// withWriteRecorder returns ctx carrying a recorder for one provider call, and
// a function that reports whether that call's request was written in full.
// Only RecordWrite sets it, so an HTTP request made under ctx for anything else,
// such as a credential fetch, does not count.
func withWriteRecorder(ctx context.Context) (context.Context, func() bool) {
	rec := &writeRecorder{}
	return context.WithValue(ctx, recorderKey{}, rec), rec.wrote.Load
}

// RecordWrite marks the provider request made under ctx as written in full.
// The HTTP client NewSESClient installs calls it from that request's own
// WroteRequest trace. A test SESAPI calls it to stand for a write.
func RecordWrite(ctx context.Context) {
	if rec, ok := ctx.Value(recorderKey{}).(*writeRecorder); ok {
		rec.wrote.Store(true)
	}
}

// operationClient is the HTTP client NewSESClient installs. It traces each
// request the SES client itself sends and reports a full write to the
// recorder in the request's context. Credential providers send through their
// own clients, so their requests never count as the send.
type operationClient struct{ next awsses.HTTPClient }

// GetTimeout reports the wrapped client's HTTP timeout, as the SDK's
// BuildableClient does, so the timeout NewSESClientWithTimeout set stays
// visible through the wrapper. Zero means none.
func (c operationClient) GetTimeout() time.Duration {
	if t, ok := c.next.(interface{ GetTimeout() time.Duration }); ok {
		return t.GetTimeout()
	}
	return 0
}

func (c operationClient) Do(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if _, ok := ctx.Value(recorderKey{}).(*writeRecorder); ok {
		req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				RecordWrite(ctx)
			}
		}}))
	}
	return c.next.Do(req)
}

// withOperationClient wraps the SES client's HTTP client in operationClient.
func withOperationClient(o *awsses.Options) {
	next := o.HTTPClient
	if next == nil {
		next = awshttp.NewBuildableClient()
	}
	o.HTTPClient = operationClient{next: next}
}
