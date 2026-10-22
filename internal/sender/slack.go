// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"log/slog"

	"github.com/google/uuid"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
)

// ErrNoSlackToken is what a TokenProvider returns when it has no Slack token for
// the org. The send fails as an auth failure: no retry will find a token.
var ErrNoSlackToken = errors.New("no Slack token configured for this organization")

// TokenProvider returns the Slack bot token for an org. The token is held only for
// the call and never persisted.
type TokenProvider interface {
	Token(ctx context.Context, orgID string) (string, error)
}

// StaticTokens is a TokenProvider over configuration: a per-org map, falling back
// to one workspace token for every org.
type StaticTokens struct {
	Default string
	PerOrg  map[string]string
}

// Token returns the org's token, else the default, else ErrNoSlackToken. PerOrg
// is keyed by the canonical lowercase UUID, so orgID is looked up in that form.
func (t StaticTokens) Token(_ context.Context, orgID string) (string, error) {
	if id, err := uuid.Parse(orgID); err == nil {
		orgID = id.String()
	}
	if tok := t.PerOrg[orgID]; tok != "" {
		return tok, nil
	}
	if t.Default != "" {
		return t.Default, nil
	}
	return "", ErrNoSlackToken
}

// Connections answers whether an org has a Slack connection by asking the same
// TokenProvider the Slack sender uses, so eligibility and the send agree: a
// token means connected, ErrNoSlackToken means not, and any other error is a
// failed lookup. Every other channel needs no connection.
type Connections struct{ Tokens TokenProvider }

// IsConnected reports the org's connection for channel.
func (c Connections) IsConnected(ctx context.Context, orgID string, channel pb.Channel) (bool, error) {
	if channel != pb.Channel_CHANNEL_SLACK {
		return true, nil
	}
	_, err := c.Tokens.Token(ctx, orgID)
	if errors.Is(err, ErrNoSlackToken) {
		return false, nil
	}
	return err == nil, err
}

// SlackPoster is the Slack Web API egress (chat.postMessage and the file
// upload). slackclient.Client implements it; an interface keeps the sender's
// classification logic testable.
type SlackPoster interface {
	PostMessage(ctx context.Context, token, channel, text, blocksJSON string) (ts string, err error)
	UploadFile(ctx context.Context, token, channel string, content []byte, filename string) (ts string, err error)
	// JoinChannel makes the bot a member of a public channel (conversations.join)
	// so a subsequent files.upload succeeds — files.upload has no
	// chat:write.public equivalent and rejects non-member channels. Private
	// channels cannot be self-joined (manual invite required); the caller treats
	// the error as best-effort and lets UploadFile surface not_in_channel.
	JoinChannel(ctx context.Context, token, channel string) error
}

// SlackError carries the Slack API error code so the sender can classify it.
type SlackError struct {
	Code string
	// RetryAfter is the wait Slack asked for with a 429, or zero.
	RetryAfter time.Duration
}

func (e *SlackError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("slack error: %s (retry after %s)", e.Code, e.RetryAfter)
	}
	return "slack error: " + e.Code
}

// SlackSender sends Slack messages with the org's bot token. LINK mode posts
// Block Kit; FILE mode uploads the attachment.
type SlackSender struct {
	tokens TokenProvider
	poster SlackPoster
	s3     ObjectFetcher
	logger *slog.Logger
}

// SetLogger sets where the sender logs; nil means slog's default.
func (s *SlackSender) SetLogger(l *slog.Logger) { s.logger = l }

// NewSlackSender constructs the Slack egress.
func NewSlackSender(tokens TokenProvider, poster SlackPoster, store ObjectFetcher) *SlackSender {
	return &SlackSender{tokens: tokens, poster: poster, s3: store}
}

var _ dispatch.Sender = (*SlackSender)(nil)

// Send fetches the org's token, then posts a Block Kit message (LINK) or uploads
// the attachment (FILE). A Slack ack is delivery (there is no async feedback); an
// auth failure is OutcomeAuth, so the dispatcher calls the auth-failure hook.
func (s *SlackSender) Send(ctx context.Context, in dispatch.SendInput) dispatch.SendResult {
	token, err := s.tokens.Token(ctx, in.OrgID)
	if err != nil {
		return classifyTokenErr(err)
	}

	var ts string
	if in.SlackDelivery == pb.SlackDelivery_SLACK_DELIVERY_FILE {
		key := paramString(in, AttachmentKeyParam)
		if key == "" {
			return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: errors.New("slack FILE delivery without attachment_key")}
		}
		file, fail := loadAttachment(ctx, s.s3, in.OrgID, key)
		if fail != nil {
			return *fail
		}
		// Best-effort join so the bot can upload to a public channel it was never
		// invited to (files.upload has no chat:write.public bypass). The error is
		// not acted on: a private channel can't be self-joined and already_in_channel
		// is benign — UploadFile then surfaces not_in_channel if membership is
		// genuinely missing, which is the authoritative result. Logged at debug only
		// so a genuinely surprising join failure is not wholly invisible.
		if jerr := s.poster.JoinChannel(ctx, token, in.Recipient); jerr != nil {
			s.log().DebugContext(ctx, "slack sender: best-effort conversations.join failed (ignored)", "error", jerr)
		}
		ts, err = s.poster.UploadFile(ctx, token, in.Recipient, file, attachmentDisplayName(in))
	} else {
		ts, err = s.poster.PostMessage(ctx, token, in.Recipient, in.Rendered.GetSlackText(), in.Rendered.GetSlackBlocksJson())
	}
	if err != nil {
		return classifySlackErr(err)
	}
	return dispatch.SendResult{ProviderMessageID: ts, Outcome: dispatch.OutcomeSuccess}
}

// slackPermanentCodes are config/recipient errors that never succeed on retry.
var slackPermanentCodes = map[string]bool{
	"channel_not_found": true, "not_in_channel": true, "msg_too_long": true,
	"invalid_blocks": true, "is_archived": true,
}

// slackAuthCodes are grant failures → reconnect.
var slackAuthCodes = map[string]bool{"invalid_auth": true, "token_revoked": true, "account_inactive": true}

// slackUncertainCodes are ok:false answers that say Slack failed while
// handling the call, after it may have posted the message.
var slackUncertainCodes = map[string]bool{"fatal_error": true, "internal_error": true}

// classifyTokenErr maps a TokenProvider failure to a dispatch Outcome. Only a
// missing token is OutcomeAuth; any other failure (a secret store outage, a
// deadline) is transient, since nothing has been sent, so an outage never marks
// a healthy connection invalid.
func classifyTokenErr(err error) dispatch.SendResult {
	wrapped := fmt.Errorf("get slack token: %w", err)
	if errors.Is(err, ErrNoSlackToken) {
		return dispatch.SendResult{Outcome: dispatch.OutcomeAuth, Err: wrapped, ErrCode: "no_token"}
	}
	return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: wrapped}
}

// classifySlackErr maps a Slack error to a dispatch Outcome. A failure marked
// ErrMaybeSent (written in full, no Slack error code back), an ok:false with no
// code, or a fatal_error or internal_error is uncertain, unless it is marked
// ErrNotSent. Otherwise: 429 → transient; invalid_auth/token_revoked → auth;
// channel_not_found, msg_too_long and the like → permanent; else transient, a
// failure before the request was written included.
func classifySlackErr(err error) dispatch.SendResult {
	var se *SlackError
	isSlack := errors.As(err, &se)
	maybeSent := errors.Is(err, ErrMaybeSent) || (isSlack && (se.Code == "" || slackUncertainCodes[se.Code]))
	if maybeSent && !errors.Is(err, ErrNotSent) {
		return dispatch.SendResult{Outcome: dispatch.OutcomeUncertain, Err: err}
	}
	if isSlack {
		switch {
		case slackAuthCodes[se.Code]:
			return dispatch.SendResult{Outcome: dispatch.OutcomeAuth, Err: err}
		case slackPermanentCodes[se.Code]:
			return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: err}
		case se.Code == "rate_limited" || se.Code == "ratelimited":
			return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err, RetryAfter: se.RetryAfter}
		}
	}
	// Match on substrings too (a wrapped error), else default transient.
	msg := err.Error()
	for code := range slackAuthCodes {
		if strings.Contains(msg, code) {
			return dispatch.SendResult{Outcome: dispatch.OutcomeAuth, Err: err}
		}
	}
	for code := range slackPermanentCodes {
		if strings.Contains(msg, code) {
			return dispatch.SendResult{Outcome: dispatch.OutcomePermanent, Err: err}
		}
	}
	return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: err}
}

func (s *SlackSender) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}
