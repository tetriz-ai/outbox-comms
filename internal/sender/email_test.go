// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	awssestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// fakeFetcher returns canned bytes (or an error) for any key.
type fakeFetcher struct {
	data []byte
	err  error
}

func (f fakeFetcher) Get(context.Context, string, string) ([]byte, error) { return f.data, f.err }

// fakeSES stands in for the SES client: each call runs the matching hook.
type fakeSES struct {
	onSend func(context.Context, *awsses.SendEmailInput) (*awsses.SendEmailOutput, error)
	onRaw  func(context.Context, *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error)
}

func (f *fakeSES) SendEmail(ctx context.Context, in *awsses.SendEmailInput, _ ...func(*awsses.Options)) (*awsses.SendEmailOutput, error) {
	if f.onSend == nil {
		panic("unexpected SendEmail")
	}
	return f.onSend(ctx, in)
}

func (f *fakeSES) SendRawEmail(ctx context.Context, in *awsses.SendRawEmailInput, _ ...func(*awsses.Options)) (*awsses.SendRawEmailOutput, error) {
	if f.onRaw == nil {
		panic("unexpected SendRawEmail")
	}
	return f.onRaw(ctx, in)
}

func rendered() *pb.RenderedPayload {
	return &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hello</p>"}
}

func paramsWith(t *testing.T, key string) *structpb.Struct {
	t.Helper()
	return paramsMap(t, map[string]any{"attachment_key": key})
}

func paramsMap(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("params: %v", err)
	}
	return s
}

// TestEmailPlainSend proves a no-attachment send uses SendEmail and records the
// SES MessageId as the provider id.
func TestEmailPlainSend(t *testing.T) {
	ses := &fakeSES{}
	ses.onSend = func(context.Context, *awsses.SendEmailInput) (*awsses.SendEmailOutput, error) {
		return &awsses.SendEmailOutput{MessageId: aws.String("ses-1")}, nil
	}

	es := sender.NewEmailSender(ses, fakeFetcher{}, "noreply@x", "cfgset")
	res := es.Send(context.Background(), dispatch.SendInput{
		Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered(),
	})
	if res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID != "ses-1" {
		t.Fatalf("want success+ses-1, got %+v", res)
	}
}

// TestEmailPlainSendUTF8Charset proves the structured SendEmail path declares
// UTF-8 on Subject, Html, and Text: SES's Content defaults to 7-bit ASCII, and
// producer copy (names, em-dashes) routinely carries non-ASCII bytes
// that an undeclared charset would let SES reject or mangle.
func TestEmailPlainSendUTF8Charset(t *testing.T) {
	ses := &fakeSES{}
	var got *awsses.SendEmailInput
	ses.onSend = func(_ context.Context, in *awsses.SendEmailInput) (*awsses.SendEmailOutput, error) {
		got = in
		return &awsses.SendEmailOutput{MessageId: aws.String("ses-1")}, nil
	}
	es := sender.NewEmailSender(ses, fakeFetcher{}, "noreply@x", "cfgset")
	res := es.Send(context.Background(), dispatch.SendInput{
		Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com",
		Rendered: &pb.RenderedPayload{EmailSubject: "José wrote — s", EmailHtml: "<p>José</p>", EmailText: "José"},
	})
	if res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("want success, got %+v", res)
	}
	for name, c := range map[string]*awssestypes.Content{
		"Subject":   got.Message.Subject,
		"Body.Html": got.Message.Body.Html,
		"Body.Text": got.Message.Body.Text,
	} {
		if aws.ToString(c.Charset) != "UTF-8" {
			t.Errorf("%s.Charset = %q, want UTF-8", name, aws.ToString(c.Charset))
		}
	}
}

// TestEmailPlainSendTextAlternative proves a rendered text part rides as the
// SES Body.Text alternative, and its absence sends HTML-only.
func TestEmailPlainSendTextAlternative(t *testing.T) {
	tests := []struct {
		name     string
		rendered *pb.RenderedPayload
		wantText bool
	}{
		{"text part present", &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hello</p>", EmailText: "hello\n"}, true},
		{"no text part", rendered(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ses := &fakeSES{}
			var got *awsses.SendEmailInput
			ses.onSend = func(_ context.Context, in *awsses.SendEmailInput) (*awsses.SendEmailOutput, error) {
				got = in
				return &awsses.SendEmailOutput{MessageId: aws.String("ses-1")}, nil
			}
			es := sender.NewEmailSender(ses, fakeFetcher{}, "noreply@x", "cfgset")
			res := es.Send(context.Background(), dispatch.SendInput{
				Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: tt.rendered,
			})
			if res.Outcome != dispatch.OutcomeSuccess {
				t.Fatalf("want success, got %+v", res)
			}
			if (got.Message.Body.Text != nil) != tt.wantText {
				t.Fatalf("Body.Text presence = %v, want %v", got.Message.Body.Text != nil, tt.wantText)
			}
		})
	}
}

// TestEmailAttachmentMIMEStructure proves the raw path nests a text+HTML
// multipart/alternative inside multipart/mixed when a text part exists, and
// stays HTML-only otherwise.
func TestEmailAttachmentMIMEStructure(t *testing.T) {
	tests := []struct {
		name     string
		rendered *pb.RenderedPayload
		wantAlt  bool
	}{
		{"text part present", &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hello</p>", EmailText: "hello\n"}, true},
		{"no text part", rendered(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ses := &fakeSES{}
			var raw []byte
			ses.onRaw = func(_ context.Context, in *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
				raw = in.RawMessage.Data
				return &awsses.SendRawEmailOutput{MessageId: aws.String("ses-raw-1")}, nil
			}
			es := sender.NewEmailSender(ses, fakeFetcher{data: []byte("%PDF-1.4 small")}, "noreply@x", "cfgset")
			res := es.Send(context.Background(), dispatch.SendInput{
				Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: tt.rendered,
				Params: paramsWith(t, "comms/org/report.pdf"),
			})
			if res.Outcome != dispatch.OutcomeSuccess {
				t.Fatalf("want success, got %+v", res)
			}
			msg := string(raw)
			hasAlt := strings.Contains(msg, "multipart/alternative") && strings.Contains(msg, "text/plain; charset=UTF-8")
			if hasAlt != tt.wantAlt {
				t.Fatalf("alternative part presence = %v, want %v; raw:\n%s", hasAlt, tt.wantAlt, msg)
			}
			for _, want := range []string{"multipart/mixed", "text/html; charset=UTF-8", "application/pdf"} {
				if !strings.Contains(msg, want) {
					t.Errorf("raw MIME missing %q", want)
				}
			}
		})
	}
}

// TestEmailAttachmentMIMENonASCIIRoundTrips proves the text/html and text/plain
// parts declare "base64" (never "7bit") and that a non-ASCII body — a name with
// an accent, an em-dash — round-trips intact: declaring 7bit over such bytes
// breaks DKIM/DMARC on receipt even though the send itself succeeds.
func TestEmailAttachmentMIMENonASCIIRoundTrips(t *testing.T) {
	htmlBody := "<p>Hi José — welcome</p>"
	textBody := "Hi José — welcome"
	ses := &fakeSES{}
	var raw []byte
	ses.onRaw = func(_ context.Context, in *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
		raw = in.RawMessage.Data
		return &awsses.SendRawEmailOutput{MessageId: aws.String("ses-raw-1")}, nil
	}
	es := sender.NewEmailSender(ses, fakeFetcher{data: []byte("%PDF-1.4 small")}, "noreply@x", "cfgset")
	res := es.Send(context.Background(), dispatch.SendInput{
		Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com",
		Rendered: &pb.RenderedPayload{EmailSubject: "s", EmailHtml: htmlBody, EmailText: textBody},
		Params:   paramsWith(t, "comms/org/report.pdf"),
	})
	if res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("want success, got %+v", res)
	}
	msg := string(raw)
	if strings.Contains(msg, "Content-Transfer-Encoding: 7bit") {
		t.Fatalf("body declares 7bit over non-ASCII bytes; raw:\n%s", msg)
	}
	wrap76 := func(body string) string {
		enc := base64.StdEncoding.EncodeToString([]byte(body))
		var b strings.Builder
		for len(enc) > 76 {
			b.WriteString(enc[:76] + "\r\n")
			enc = enc[76:]
		}
		b.WriteString(enc)
		return b.String()
	}
	if !strings.Contains(msg, wrap76(textBody)) {
		t.Errorf("text part not base64-encoded correctly; raw:\n%s", msg)
	}
	if !strings.Contains(msg, wrap76(htmlBody)) {
		t.Errorf("html part not base64-encoded correctly; raw:\n%s", msg)
	}
}

// TestEmailAttachmentNilFetcher proves a sender constructed without an
// ObjectFetcher fails permanently instead of panicking
// when params carry an attachment_key.
func TestEmailAttachmentNilFetcher(t *testing.T) {
	ses := &fakeSES{}
	es := sender.NewEmailSender(ses, nil, "noreply@x", "cfgset")
	res := es.Send(context.Background(), dispatch.SendInput{
		Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered(),
		Params: paramsWith(t, "comms/org/report.pdf"),
	})
	if res.Outcome != dispatch.OutcomePermanent {
		t.Fatalf("want permanent, got %+v", res)
	}
	if res.Err == nil || !strings.Contains(res.Err.Error(), "no attachment store") {
		t.Fatalf("want no-attachment-store error, got %+v", res)
	}
}

// TestEmailAttachmentSend proves an attachment send fetches the PDF and uses
// SendRawEmail (MIME).
func TestEmailAttachmentSend(t *testing.T) {
	ses := &fakeSES{}
	ses.onRaw = func(context.Context, *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
		return &awsses.SendRawEmailOutput{MessageId: aws.String("ses-raw-1")}, nil
	}

	es := sender.NewEmailSender(ses, fakeFetcher{data: []byte("%PDF-1.4 small")}, "noreply@x", "cfgset")
	res := es.Send(context.Background(), dispatch.SendInput{
		Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered(),
		Params: paramsWith(t, "comms/org/report.pdf"),
	})
	if res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID != "ses-raw-1" {
		t.Fatalf("want success+ses-raw-1, got %+v", res)
	}
}

// TestEmailAttachmentCustomFilename proves attachment_filename rides in the MIME
// Content-Disposition and drives Content-Type; the attachment.pdf default
// (attachment.pdf / application/pdf) applies when the param is omitted.
func TestEmailAttachmentCustomFilename(t *testing.T) {
	tests := []struct {
		name         string
		params       *structpb.Struct
		wantFilename string
		wantType     string
	}{
		{
			name:         "attachment.pdf default on a nested key",
			params:       paramsWith(t, "comms/files/org/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.pdf"),
			wantFilename: "attachment.pdf",
			wantType:     "application/pdf",
		},
		{
			name: "adhoc original name",
			params: paramsMap(t, map[string]any{
				"attachment_key":      "comms/files/adhoc/org/id/notice.csv",
				"attachment_filename": "Q2 notice.csv",
			}),
			wantFilename: "Q2 notice.csv",
			wantType:     "text/csv",
		},
		{
			name: "path-shaped filename is reduced to the basename",
			params: paramsMap(t, map[string]any{
				"attachment_key":      "comms/files/adhoc/org/id/notice.pdf",
				"attachment_filename": `..\..\evil.pdf`,
			}),
			wantFilename: "evil.pdf",
			wantType:     "application/pdf",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ses := &fakeSES{}
			var raw []byte
			ses.onRaw = func(_ context.Context, in *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
				raw = in.RawMessage.Data
				return &awsses.SendRawEmailOutput{MessageId: aws.String("ses-raw-1")}, nil
			}
			es := sender.NewEmailSender(ses, fakeFetcher{data: []byte("attach-bytes")}, "noreply@x", "cfgset")
			res := es.Send(context.Background(), dispatch.SendInput{
				Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered(),
				Params: tt.params,
			})
			if res.Outcome != dispatch.OutcomeSuccess {
				t.Fatalf("want success, got %+v", res)
			}
			msg := string(raw)
			if !strings.Contains(msg, `filename="`+tt.wantFilename+`"`) {
				t.Errorf("raw MIME missing filename %q; raw:\n%s", tt.wantFilename, msg)
			}
			if !strings.Contains(msg, "Content-Type: "+tt.wantType) {
				t.Errorf("raw MIME missing Content-Type %q; raw:\n%s", tt.wantType, msg)
			}
		})
	}
}

// TestEmailOverCapAttachment pins the size cap on the assembled message: SES
// takes at most 10 MiB of raw MIME, and base64 makes a file about a third
// larger. A 5 MiB file fits and is sent; an 8 MiB file does not fit and ends
// permanent before any SES call, as does an 11 MiB one.
func TestEmailOverCapAttachment(t *testing.T) {
	for _, tt := range []struct {
		mib  int
		want dispatch.Outcome
	}{{5, dispatch.OutcomeSuccess}, {8, dispatch.OutcomePermanent}, {11, dispatch.OutcomePermanent}} {
		t.Run(fmt.Sprintf("%d MiB", tt.mib), func(t *testing.T) {
			calls, rawLen := 0, 0
			ses := &fakeSES{onRaw: func(_ context.Context, in *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
				calls++
				rawLen = len(in.RawMessage.Data)
				return &awsses.SendRawEmailOutput{MessageId: aws.String("raw-1")}, nil
			}}
			es := sender.NewEmailSender(ses, fakeFetcher{data: make([]byte, tt.mib*1024*1024)}, "noreply@x", "cfgset")
			res := es.Send(context.Background(), dispatch.SendInput{
				Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered(),
				Params: paramsWith(t, "comms/org/big.pdf"),
			})
			if res.Outcome != tt.want {
				t.Fatalf("outcome = %v, want %v (%v)", res.Outcome, tt.want, res.Err)
			}
			if wantCalls := map[bool]int{true: 1, false: 0}[tt.want == dispatch.OutcomeSuccess]; calls != wantCalls {
				t.Fatalf("SES calls = %d, want %d", calls, wantCalls)
			}
			if rawLen > 10*1024*1024 {
				t.Fatalf("sent a %d-byte message over SES's cap", rawLen)
			}
		})
	}
}

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: "detail that may echo dev@example.com"}
}

// TestEmailErrorClassification proves the SES error mapping is by whether the
// request was written in full and by API error code, never by the error text.
// The fake reports the write with sender.RecordWrite, as the client
// NewSESClient installs does. Before the write every failure is transient; after it, a
// failure with no code, or a 5xx, is uncertain, and a code decides otherwise.
// Every case runs on the plain path and on the attachment (raw-message) path.
func TestEmailErrorClassification(t *testing.T) {
	reset := sesOpErr(&net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)})
	tests := []struct {
		name     string
		sendErr  error
		wrote    bool
		want     dispatch.Outcome
		wantCode string
	}{
		{"MessageRejected → permanent", apiErr("MessageRejected"), true, dispatch.OutcomePermanent, "MessageRejected"},
		{"AccessDenied (IAM) → permanent", apiErr("AccessDenied"), true, dispatch.OutcomePermanent, "AccessDenied"},
		{"paused account → permanent", apiErr("AccountSendingPausedException"), true, dispatch.OutcomePermanent, "AccountSendingPausedException"},
		{"unverified MAIL FROM → permanent", apiErr("MailFromDomainNotVerifiedException"), true, dispatch.OutcomePermanent, "MailFromDomainNotVerifiedException"},
		{"Throttling → transient", apiErr("Throttling"), true, dispatch.OutcomeTransient, "Throttling"},
		{"unknown code → transient", apiErr("ServiceUnavailable"), true, dispatch.OutcomeTransient, "ServiceUnavailable"},
		{"wrapped API error is still classified", fmt.Errorf("operation SendEmail: %w", apiErr("AccessDenied")), true, dispatch.OutcomePermanent, "AccessDenied"},
		{"a non-API error naming a code in its text, before the write → transient", errors.New("dial tcp: lookup MessageRejected.example.net: no such host"), false, dispatch.OutcomeTransient, ""},
		{"timeout before the write → transient", context.DeadlineExceeded, false, dispatch.OutcomeTransient, ""},
		{"timeout after the write → uncertain", context.DeadlineExceeded, true, dispatch.OutcomeUncertain, ""},
		{"cancellation before the write → transient", context.Canceled, false, dispatch.OutcomeTransient, ""},
		{"cancellation after the write → uncertain", context.Canceled, true, dispatch.OutcomeUncertain, ""},
		{"connection reset before the write → transient", reset, false, dispatch.OutcomeTransient, ""},
		{"connection reset after the write → uncertain", reset, true, dispatch.OutcomeUncertain, ""},
		{"response cut off → uncertain", sesOpErr(io.ErrUnexpectedEOF), true, dispatch.OutcomeUncertain, ""},
		{"5xx answer → uncertain", sesOpErr(&smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}, Err: errors.New("service unavailable")}), true, dispatch.OutcomeUncertain, ""},
		{"5xx answer with a code → uncertain", sesOpErr(&smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}, Err: apiErr("ServiceUnavailable")}), true, dispatch.OutcomeUncertain, "ServiceUnavailable"},
		{"dial refused, nothing written → transient", sesOpErr(&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}), false, dispatch.OutcomeTransient, ""},
		{"DNS failure, nothing written → transient", sesOpErr(&net.DNSError{Err: "no such host", Name: "email.example", IsNotFound: true}), false, dispatch.OutcomeTransient, ""},
	}
	for _, tt := range tests {
		for _, raw := range []bool{false, true} {
			name := "plain/" + tt.name
			if raw {
				name = "attachment/" + tt.name
			}
			t.Run(name, func(t *testing.T) {
				fail := func(ctx context.Context) error {
					if tt.wrote {
						sender.RecordWrite(ctx)
					}
					return tt.sendErr
				}
				ses := &fakeSES{
					onSend: func(ctx context.Context, _ *awsses.SendEmailInput) (*awsses.SendEmailOutput, error) {
						return nil, fail(ctx)
					},
					onRaw: func(ctx context.Context, _ *awsses.SendRawEmailInput) (*awsses.SendRawEmailOutput, error) {
						return nil, fail(ctx)
					},
				}
				in := dispatch.SendInput{Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com", Rendered: rendered()}
				if raw {
					in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
				}
				res := sender.NewEmailSender(ses, fakeFetcher{data: []byte("%PDF-1.4")}, "noreply@x", "").Send(context.Background(), in)
				if res.Outcome != tt.want || res.ErrCode != tt.wantCode {
					t.Fatalf("outcome = %v code %q, want %v code %q", res.Outcome, res.ErrCode, tt.want, tt.wantCode)
				}
			})
		}
	}
}

// sesOpErr wraps a transport error the way the AWS SDK returns one.
func sesOpErr(err error) error {
	return &smithy.OperationError{ServiceID: "SES", OperationName: "SendEmail", Err: err}
}
