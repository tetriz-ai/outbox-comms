// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package slackclient implements sender.SlackPoster over the Slack Web API with
// a plain HTTP client. The bot token is passed per call and never stored. The
// base URL is configurable so tests and local runs can point it at a stub.
// chat.postMessage takes JSON; the file upload uses Slack's external-upload flow
// (files.getUploadURLExternal, a POST of the bytes to the returned URL, then
// files.completeUploadExternal).
package slackclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// DefaultBaseURL is the Slack Web API base.
const DefaultBaseURL = "https://slack.com/api"

// HTTPDoer is the subset of *http.Client the client uses.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client implements sender.SlackPoster.
type Client struct {
	doer    HTTPDoer
	webBase string
	// baseHost is the host of webBase. An upload URL on this host is accepted
	// alongside Slack's own hosts, so a stub can serve the whole flow.
	baseHost string
	logger   *slog.Logger
}

// SetLogger sets where the client logs; nil means slog's default.
func (p *Client) SetLogger(l *slog.Logger) { p.logger = l }

var _ sender.SlackPoster = (*Client)(nil)

// New builds a client over doer (nil means an http.Client with a 30 s timeout).
// webBase is the Web API base, for example DefaultBaseURL.
func New(doer HTTPDoer, webBase string) (*Client, error) {
	if doer == nil {
		doer = &http.Client{Timeout: 30 * time.Second}
	}
	u, err := url.Parse(webBase)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("slack base URL %q must be an absolute http(s) URL", webBase)
	}
	return &Client{doer: doer, webBase: strings.TrimRight(webBase, "/"), baseHost: u.Hostname()}, nil
}

// PostMessage posts a Block Kit message via chat.postMessage, with text as the
// plain-text fallback Slack shows in notifications and to screen readers. An
// empty text is derived from the first text in the blocks. A Slack ok:false is
// surfaced as a sender.SlackError carrying the Slack error code so the dispatcher
// can classify it (auth, permanent, transient).
func (p *Client) PostMessage(ctx context.Context, token, channel, text, blocksJSON string) (string, error) {
	blocks, err := parseBlocks(blocksJSON)
	// errNoBlocks means an empty blocks string: the blocks field is omitted.
	// Blocks that do not parse were never going to post, and no retry fixes
	// them: not sent, and permanent as Slack's own invalid_blocks.
	if err != nil && !errors.Is(err, errNoBlocks) {
		return "", fmt.Errorf("parse blocks: %w: %w: %w", sender.ErrNotSent, &sender.SlackError{Code: "invalid_blocks"}, err)
	}
	if text == "" {
		text = fallbackText(blocks)
	}
	payload := map[string]any{"channel": channel, "text": text}
	if len(blocks) > 0 {
		payload["blocks"] = blocks
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal chat.postMessage: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.webBase+"/chat.postMessage", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build chat.postMessage request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	respBody, err := p.doRead(ctx, req)
	if err != nil {
		return "", fmt.Errorf("chat.postMessage: %w", err)
	}
	var resp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		TS    string `json:"ts"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", fmt.Errorf("decode chat.postMessage response: %w: %w", sender.ErrMaybeSent, err)
	}
	if !resp.OK {
		return "", &sender.SlackError{Code: resp.Error}
	}
	return resp.TS, nil
}

// UploadFile shares a file to a channel via Slack's external-upload flow:
// files.getUploadURLExternal, then the bytes to the returned URL, then
// files.completeUploadExternal with the channel. Returns the Slack file id as the
// provider message id. A non-ok Slack response surfaces as a sender.SlackError.
// Only the last step shares the file, so a failure in either of the first two
// is marked sender.ErrNotSent.
func (p *Client) UploadFile(ctx context.Context, token, channel string, content []byte, filename string) (string, error) {
	getForm := url.Values{}
	getForm.Set("filename", filename)
	getForm.Set("length", fmt.Sprintf("%d", len(content)))
	var getResp struct {
		OK        bool   `json:"ok"`
		Error     string `json:"error"`
		UploadURL string `json:"upload_url"`
		FileID    string `json:"file_id"`
	}
	if err := p.slackForm(ctx, token, "files.getUploadURLExternal", getForm, &getResp); err != nil {
		return "", fmt.Errorf("files.getUploadURLExternal: %w: %w", sender.ErrNotSent, err)
	}
	if !getResp.OK {
		return "", fmt.Errorf("files.getUploadURLExternal: %w: %w", sender.ErrNotSent, &sender.SlackError{Code: getResp.Error})
	}

	// Upload the bytes to the returned pre-signed URL (multipart; the bot token
	// is deliberately not sent to this endpoint).
	if err := p.putUpload(ctx, getResp.UploadURL, filename, content); err != nil {
		return "", fmt.Errorf("slack upload bytes: %w: %w", sender.ErrNotSent, err)
	}

	filesJSON, err := json.Marshal([]map[string]string{{"id": getResp.FileID, "title": filename}})
	if err != nil {
		return "", fmt.Errorf("marshal slack files payload: %w", err)
	}
	complForm := url.Values{}
	complForm.Set("files", string(filesJSON))
	complForm.Set("channel_id", channel)
	var complResp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := p.slackForm(ctx, token, "files.completeUploadExternal", complForm, &complResp); err != nil {
		return "", fmt.Errorf("files.completeUploadExternal: %w", err)
	}
	if !complResp.OK {
		return "", &sender.SlackError{Code: complResp.Error}
	}
	return getResp.FileID, nil
}

// JoinChannel adds the bot to a public channel via conversations.join so a
// subsequent upload (which has no chat:write.public bypass) is accepted. A non-ok
// Slack response surfaces as a sender.SlackError; the caller treats it as best
// effort (private channels return a non-ok the caller ignores).
func (p *Client) JoinChannel(ctx context.Context, token, channel string) error {
	form := url.Values{}
	form.Set("channel", channel)
	var resp struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := p.slackForm(ctx, token, "conversations.join", form, &resp); err != nil {
		return fmt.Errorf("conversations.join: %w", err)
	}
	if !resp.OK {
		return &sender.SlackError{Code: resp.Error}
	}
	return nil
}

// slackForm POSTs an application/x-www-form-urlencoded Web API call with the bot
// token and decodes the JSON response into out.
func (p *Client) slackForm(ctx context.Context, token, method string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.webBase+"/"+method, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	body, err := p.doRead(ctx, req)
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode %s response: %w: %w", method, sender.ErrMaybeSent, err)
	}
	return nil
}

// putUpload posts the file bytes to the pre-signed upload URL as a multipart
// form. The URL comes from Slack's getUploadURLExternal response; it is checked to
// be a Slack host (or the configured base host) before the file content goes
// there.
func (p *Client) putUpload(ctx context.Context, uploadURL, filename string, content []byte) error {
	u, err := url.Parse(uploadURL)
	if err != nil {
		return fmt.Errorf("parse upload URL %q: %w", uploadURL, err)
	}
	if !p.allowedUploadHost(u.Hostname()) {
		return fmt.Errorf("refusing to upload to non-Slack host: %q", uploadURL)
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return fmt.Errorf("build upload form: %w", err)
	}
	if _, err := fw.Write(content); err != nil {
		return fmt.Errorf("write upload bytes: %w", err)
	}
	if err := mw.Close(); err != nil {
		return fmt.Errorf("close upload form: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &buf)
	if err != nil {
		return fmt.Errorf("build upload request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if _, err := p.doRead(ctx, req); err != nil {
		return fmt.Errorf("post upload bytes: %w", err)
	}
	return nil
}

// doRead executes a request, reads the full body, and fails on a non-2xx status.
// A completed-but-failed HTTP exchange returns a nil error from Do, so without
// this check a 4xx/5xx from the upload endpoint (which signals failure by status,
// not a JSON body) would be treated as success. HTTP 429 becomes the Slack
// "ratelimited" code, which the sender classifies as transient. Any other
// failure after the request was written in full is marked sender.ErrMaybeSent.
func (p *Client) doRead(ctx context.Context, req *http.Request) ([]byte, error) {
	tctx, wrote := sender.TraceWrites(req.Context())
	resp, err := p.doer.Do(req.WithContext(tctx))
	if err != nil {
		if wrote() {
			return nil, fmt.Errorf("slack http do: %w: %w", sender.ErrMaybeSent, err)
		}
		return nil, fmt.Errorf("slack http do: %w", err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			p.log().WarnContext(ctx, "slackclient: closing response body", "error", cerr)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read slack response body: %w: %w", sender.ErrMaybeSent, err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, &sender.SlackError{Code: "ratelimited", RetryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("slack http %d: %w", resp.StatusCode, sender.ErrMaybeSent)
	}
	return body, nil
}

// retryAfter reads Slack's Retry-After header, a whole number of seconds. A
// missing or malformed value is zero, which leaves the retry ladder in charge.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// errNoBlocks signals an empty blocks string: a valid "no blocks" case distinct
// from a decode failure.
var errNoBlocks = errors.New("slackclient: no blocks")

// parseBlocks decodes a Block Kit JSON array. An empty string returns errNoBlocks.
func parseBlocks(blocksJSON string) ([]any, error) {
	if blocksJSON == "" {
		return nil, errNoBlocks
	}
	var arr []any
	if err := json.Unmarshal([]byte(blocksJSON), &arr); err != nil {
		return nil, fmt.Errorf("parse slack blocks json: %w", err)
	}
	return arr, nil
}

// allowedUploadHost reports whether host is slack.com, a subdomain of it, or the
// configured base URL's host. A bare suffix match would admit evilslack.example.
func (p *Client) allowedUploadHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	return host == "slack.com" || strings.HasSuffix(host, ".slack.com") || host == strings.ToLower(p.baseHost)
}

// fallbackTextMax bounds the derived fallback; Slack truncates long text anyway.
const fallbackTextMax = 300

// fallbackText is the first non-empty "text" string found in the blocks, depth
// first, or "New message" when the blocks carry none.
func fallbackText(blocks []any) string {
	var walk func(v any) string
	walk = func(v any) string {
		switch t := v.(type) {
		case map[string]any:
			if s, ok := t["text"].(string); ok && strings.TrimSpace(s) != "" {
				return s
			}
			for _, k := range []string{"text", "elements", "fields"} {
				if r := walk(t[k]); r != "" {
					return r
				}
			}
		case []any:
			for _, e := range t {
				if r := walk(e); r != "" {
					return r
				}
			}
		}
		return ""
	}
	if s := walk(blocks); s != "" {
		if r := []rune(s); len(r) > fallbackTextMax {
			return string(r[:fallbackTextMax-1]) + "…"
		}
		return s
	}
	return "New message"
}

func (p *Client) log() *slog.Logger {
	if p.logger != nil {
		return p.logger
	}
	return slog.Default()
}
