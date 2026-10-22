// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package sender holds the per-channel implementations of dispatch.Sender:
// email over Amazon SES (this file) or SMTP (smtp.go), and Slack (slack.go).
package sender

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsses "github.com/aws/aws-sdk-go-v2/service/ses"
	awssestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
)

// maxRawMessageBytes is SES's cap on a whole raw message, headers and base64
// body included. SMTP applies it too, so a message behaves the same on both.
// Base64 makes a file about a third larger, so a file over about 7 MiB never fits.
const maxRawMessageBytes = 10 * 1024 * 1024

// checkRawSize refuses a message over maxRawMessageBytes: permanent, never
// truncated, before any provider call.
func checkRawSize(raw []byte) *dispatch.SendResult {
	if len(raw) <= maxRawMessageBytes {
		return nil
	}
	return &dispatch.SendResult{Outcome: dispatch.OutcomePermanent, ErrCode: "message_too_large",
		Err: fmt.Errorf("the message is %d bytes with its attachment, over the %d-byte cap", len(raw), maxRawMessageBytes)}
}

// AttachmentKeyParam and AttachmentFilenameParam are the params a producer sets
// to attach a stored object: the key in the attachment store, and the filename
// the recipient sees.
const (
	AttachmentKeyParam      = "attachment_key"
	AttachmentFilenameParam = "attachment_filename"
)

// ObjectFetcher fetches an attachment's bytes by key. attachments.Store
// satisfies it.
type ObjectFetcher interface {
	Get(ctx context.Context, orgID, key string) ([]byte, error)
}

// SESAPI is the subset of the SES v1 client the email sender calls;
// *ses.Client satisfies it.
type SESAPI interface {
	SendEmail(ctx context.Context, in *awsses.SendEmailInput, optFns ...func(*awsses.Options)) (*awsses.SendEmailOutput, error)
	SendRawEmail(ctx context.Context, in *awsses.SendRawEmailInput, optFns ...func(*awsses.Options)) (*awsses.SendRawEmailOutput, error)
}

// EmailSender sends email through Amazon SES.
type EmailSender struct {
	ses         SESAPI
	s3          ObjectFetcher
	fromAddress string
	configSet   string
}

// NewEmailSender constructs the SES egress. configSet is the SES configuration set
// whose event destination feeds the feedback queue (may be empty).
func NewEmailSender(sesClient SESAPI, store ObjectFetcher, fromAddress, configSet string) *EmailSender {
	return &EmailSender{ses: sesClient, s3: store, fromAddress: fromAddress, configSet: configSet}
}

var _ dispatch.Sender = (*EmailSender)(nil)

// Send delivers the rendered email. With an attachment_key in params it fetches
// the object and sends a raw MIME message, refusing one over the 10 MiB cap as
// permanent, never truncated; otherwise a plain HTML SendEmail. The provider id
// is the SES MessageId. attachment_filename, when set, is the MIME filename; otherwise
// attachment.pdf.
func (e *EmailSender) Send(ctx context.Context, in dispatch.SendInput) dispatch.SendResult {
	key := paramString(in, AttachmentKeyParam)
	if key == "" {
		return e.sendPlain(ctx, in)
	}
	return e.sendWithAttachment(ctx, in, key)
}

// sesUTF8 wraps s as an SES Content with an explicit UTF-8 charset: Content
// defaults to 7-bit ASCII, and producer copy (names, em-dashes) routinely carries
// non-ASCII bytes that would otherwise be rejected or mangled.
func sesUTF8(s string) *awssestypes.Content {
	return &awssestypes.Content{Data: aws.String(s), Charset: aws.String("UTF-8")}
}

func (e *EmailSender) sendPlain(ctx context.Context, in dispatch.SendInput) dispatch.SendResult {
	body := &awssestypes.Body{Html: sesUTF8(in.Rendered.GetEmailHtml())}
	if txt := in.Rendered.GetEmailText(); txt != "" {
		body.Text = sesUTF8(txt)
	}
	tctx, wrote := withWriteRecorder(ctx)
	out, err := e.ses.SendEmail(tctx, &awsses.SendEmailInput{
		Source:      aws.String(e.fromAddress),
		Destination: &awssestypes.Destination{ToAddresses: []string{in.Recipient}},
		Message: &awssestypes.Message{
			// headerSafe guards this path the same way buildMIME's QEncoding does for
			// the raw path: the subject is producer-composed, so a stray CR/LF must
			// not ride into the SES-constructed header.
			Subject: sesUTF8(headerSafe(in.Rendered.GetEmailSubject())),
			Body:    body,
		},
		ConfigurationSetName: e.configSetPtr(),
	})
	if err != nil {
		return classifyEmailErr(err, wrote())
	}
	return sesAccepted(out.MessageId)
}

// loadAttachment fetches and size-checks the attachment for key. A non-nil
// result is the failure to return as-is.
func loadAttachment(ctx context.Context, store ObjectFetcher, orgID, key string) ([]byte, *dispatch.SendResult) {
	if store == nil {
		return nil, &dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: errors.New("attachment_key set but the sender has no attachment store")}
	}
	attachment, err := store.Get(ctx, orgID, key)
	if err != nil {
		// A key that is invalid, names no object, or names a file the service
		// may not read cannot succeed on a retry: permanent. Any other failure
		// may be a store outage: transient.
		outcome := dispatch.OutcomeTransient
		if errors.Is(err, fs.ErrInvalid) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
			outcome = dispatch.OutcomePermanent
		}
		return nil, &dispatch.SendResult{Outcome: outcome, Err: fmt.Errorf("fetch attachment %s: %w", key, err)}
	}
	if len(attachment) > maxRawMessageBytes {
		// A file this size can never fit the message cap; skip building it.
		return nil, &dispatch.SendResult{Outcome: dispatch.OutcomePermanent, ErrCode: "message_too_large", Err: fmt.Errorf("attachment %s is %d bytes, over the %d-byte message cap", key, len(attachment), maxRawMessageBytes)}
	}
	return attachment, nil
}

func (e *EmailSender) sendWithAttachment(ctx context.Context, in dispatch.SendInput, key string) dispatch.SendResult {
	attachment, fail := loadAttachment(ctx, e.s3, in.OrgID, key)
	if fail != nil {
		return *fail
	}
	filename := attachmentDisplayName(in)
	raw := buildMIME(e.fromAddress, in.Recipient, in.Rendered.GetEmailSubject(), in.Rendered.GetEmailHtml(), in.Rendered.GetEmailText(), attachment, filename)
	if fail := checkRawSize(raw); fail != nil {
		return *fail
	}
	tctx, wrote := withWriteRecorder(ctx)
	out, err := e.ses.SendRawEmail(tctx, &awsses.SendRawEmailInput{
		RawMessage:           &awssestypes.RawMessage{Data: raw},
		ConfigurationSetName: e.configSetPtr(),
	})
	if err != nil {
		return classifyEmailErr(err, wrote())
	}
	return sesAccepted(out.MessageId)
}

func (e *EmailSender) configSetPtr() *string {
	if e.configSet == "" {
		return nil
	}
	return aws.String(e.configSet)
}

// buildMIME assembles a multipart/mixed message: the body (a text+HTML
// multipart/alternative when a text part exists, HTML-only otherwise) + a
// base64 attachment. Kept dependency-free (textproto headers + base64).
func buildMIME(from, to, subject, html, text string, attachment []byte, filename string) []byte {
	const boundary = "comms-mixed-boundary-7c1f"
	const altBoundary = "comms-alt-boundary-9e2a"
	var b bytes.Buffer
	// Strip any CR/LF from the address headers: a recipient carrying "\r\n" would
	// otherwise inject extra MIME headers (e.g. a hidden Bcc). The Subject is
	// RFC-2047-encoded below for the same reason; the address headers need the
	// equivalent guard since submit only validates the recipient non-empty.
	fmt.Fprintf(&b, "From: %s\r\n", headerSafe(from))
	fmt.Fprintf(&b, "To: %s\r\n", headerSafe(to))
	// RFC 2047-encode the rendered subject so a CR/LF in it cannot inject extra
	// MIME headers or body (the non-attachment path uses structured SendEmail,
	// which encodes for us; this raw path must do it itself). QEncoding emits a
	// single header-safe token even for plain ASCII.
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	b.WriteString("MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/mixed; boundary=\"%s\"\r\n\r\n", boundary)

	// Body part: alternative (text then HTML, least-preferred first per RFC 2046)
	// when a text alternative exists, bare HTML otherwise. Both are base64
	// (never "7bit") since a greeting name or an em-dash in producer copy is
	// routinely non-ASCII — declaring 7bit over such bytes breaks DKIM/DMARC.
	fmt.Fprintf(&b, "--%s\r\n", boundary)
	if text != "" {
		fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", altBoundary)
		fmt.Fprintf(&b, "--%s\r\n", altBoundary)
		b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
		b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(text))))
		b.WriteString("\r\n")
		fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	}
	b.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(html))))
	b.WriteString("\r\n")
	if text != "" {
		fmt.Fprintf(&b, "--%s--\r\n", altBoundary)
	}

	// Attachment part (base64, 76-col wrapped). Filename is interpolated into a
	// quoted-string parameter, so quotes and CR/LF are stripped first — otherwise
	// a producer-supplied name could inject another MIME header.
	fmt.Fprintf(&b, "--%s\r\n", boundary)
	fmt.Fprintf(&b, "Content-Type: %s\r\n", attachmentContentType(filename))
	b.WriteString("Content-Transfer-Encoding: base64\r\n")
	fmt.Fprintf(&b, "Content-Disposition: attachment; filename=\"%s\"\r\n\r\n", filename)
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString(attachment)))
	fmt.Fprintf(&b, "\r\n--%s--\r\n", boundary)
	return b.Bytes()
}

// buildAlternativeMIME assembles a message with no attachment: text and HTML as
// multipart/alternative when a text part exists, HTML alone otherwise. Header
// values get the same CR/LF guards as buildMIME.
func buildAlternativeMIME(from, to, subject, html, text string) []byte {
	const altBoundary = "comms-alt-boundary-9e2a"
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", headerSafe(from))
	fmt.Fprintf(&b, "To: %s\r\n", headerSafe(to))
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	b.WriteString("MIME-Version: 1.0\r\n")
	if text != "" {
		fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=\"%s\"\r\n\r\n", altBoundary)
		fmt.Fprintf(&b, "--%s\r\n", altBoundary)
		b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
		b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(text))))
		b.WriteString("\r\n")
		fmt.Fprintf(&b, "--%s\r\n", altBoundary)
	}
	b.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	b.WriteString(wrap76(base64.StdEncoding.EncodeToString([]byte(html))))
	b.WriteString("\r\n")
	if text != "" {
		fmt.Fprintf(&b, "--%s--\r\n", altBoundary)
	}
	return b.Bytes()
}

const defaultAttachmentFilename = "attachment.pdf"

// attachmentDisplayName is the filename the MIME part (and Slack FILE upload)
// shows. attachment_filename in params wins; otherwise attachment.pdf.
func attachmentDisplayName(in dispatch.SendInput) string {
	name := paramString(in, AttachmentFilenameParam)
	if name == "" {
		return defaultAttachmentFilename
	}
	name = headerSafe(strings.ReplaceAll(name, `"`, ""))
	name = path.Base(strings.ReplaceAll(name, `\`, "/"))
	name = strings.ReplaceAll(name, "\x00", "")
	if name == "" || name == "." || name == "/" {
		return defaultAttachmentFilename
	}
	return name
}

func attachmentContentType(filename string) string {
	if ct := mime.TypeByExtension(strings.ToLower(path.Ext(filename))); ct != "" {
		return ct
	}
	if filename == defaultAttachmentFilename {
		return "application/pdf"
	}
	return "application/octet-stream"
}

// headerSafe removes CR and LF so a value interpolated into a MIME header line
// cannot terminate the header and inject another (header-injection guard).
func headerSafe(s string) string {
	return strings.NewReplacer("\r", "", "\n", "").Replace(s)
}

func wrap76(s string) string {
	var b strings.Builder
	for len(s) > 76 {
		b.WriteString(s[:76])
		b.WriteString("\r\n")
		s = s[76:]
	}
	b.WriteString(s)
	return b.String()
}

// sesAccepted is the result of an SES call that returned no error. An answer
// without a MessageId says nothing about whether SES took the message, so it
// is uncertain.
func sesAccepted(messageID *string) dispatch.SendResult {
	id := aws.ToString(messageID)
	if id == "" {
		return dispatch.SendResult{Outcome: dispatch.OutcomeUncertain, ErrCode: "empty_message_id", Err: errors.New("SES answered without a MessageId")}
	}
	return dispatch.SendResult{ProviderMessageID: id, Outcome: dispatch.OutcomeSuccess}
}

// sesPermanentCodes are the SES API error codes no retry can fix: a rejected
// message, an unverified or paused sender, a missing configuration set, bad
// parameters, and credentials or IAM permissions that do not allow the send.
// Every other code (throttling, service errors) is transient.
var sesPermanentCodes = map[string]bool{
	"MessageRejected":                        true,
	"MailFromDomainNotVerifiedException":     true,
	"MailFromDomainNotVerified":              true,
	"AccountSendingPausedException":          true,
	"ConfigurationSetSendingPausedException": true,
	"ConfigurationSetDoesNotExist":           true,
	"ConfigurationSetDoesNotExistException":  true,
	"InvalidParameterValue":                  true,
	"AccessDenied":                           true,
	"AccessDeniedException":                  true,
	"InvalidClientTokenId":                   true,
	"UnrecognizedClientException":            true,
	"SignatureDoesNotMatch":                  true,
}

// classifyEmailErr maps an SES send error to a dispatch Outcome by whether the
// request was written in full and by its API error code, never by the error's
// text. Before the write nothing was sent: a code in sesPermanentCodes is
// permanent and anything else transient, a deadline or a refused dial
// included. After the write, an error with no API error code, or a 5xx, may
// have sent, so it is uncertain; otherwise the code decides.
func classifyEmailErr(err error, wrote bool) dispatch.SendResult {
	code := sesErrCode(err)
	var re *smithyhttp.ResponseError
	serverError := errors.As(err, &re) && re.HTTPStatusCode() >= 500
	switch {
	case wrote && (code == "" || serverError):
		return dispatch.SendResult{Outcome: dispatch.OutcomeUncertain, Err: fmt.Errorf("%w: %w", ErrMaybeSent, err), ErrCode: code}
	case sesPermanentCodes[code]:
		return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: err, ErrCode: code}
	default:
		return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err, ErrCode: code}
	}
}

// sesErrCode returns the SES API error code (e.g. "MessageRejected") — PII-free,
// safe to log — or "" when err is not an AWS API error. The raw error is never
// logged: it can echo the recipient address.
func sesErrCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func paramString(in dispatch.SendInput, key string) string {
	if in.Params == nil {
		return ""
	}
	v, ok := in.Params.GetFields()[key]
	if !ok {
		return ""
	}
	return v.GetStringValue()
}

// NewSESClient builds the SES client the email sender uses, with the SDK's own
// retries off. The dispatcher's ladder is then the only retry, and it knows the
// message's delivery guarantee; an SDK retry would resend a BEST_EFFORT message
// whose first attempt may have gone out. Its HTTP client records whether the
// SES request itself was written, which the sender's outcome depends on.
func NewSESClient(cfg aws.Config) *awsses.Client {
	return NewSESClientWithTimeout(cfg, dispatch.DefaultSendTimeout)
}

// NewSESClientWithTimeout is NewSESClient with its HTTP timeout set to timeout,
// the dispatcher's send deadline, when the SDK's own HTTP client is in use. The
// dispatcher's deadline ends the call either way; this keeps the client from
// holding a connection past it.
func NewSESClientWithTimeout(cfg aws.Config, timeout time.Duration) *awsses.Client {
	return awsses.NewFromConfig(cfg, func(o *awsses.Options) {
		o.Retryer = aws.NopRetryer{}
		if c, ok := o.HTTPClient.(*awshttp.BuildableClient); ok && timeout > 0 {
			o.HTTPClient = c.WithTimeout(timeout)
		}
		withOperationClient(o)
	})
}
