// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

// fakeSlack is an httptest Slack Web API: it records each call and answers
// from the canned bodies.
type fakeSlack struct {
	mu        sync.Mutex
	calls     []call
	postBody  string
	postCode  int
	retryHdr  string
	getBody   string
	complBody string
}

type call struct {
	path, auth, body string
}

func (f *fakeSlack) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.calls = append(f.calls, call{path: r.URL.Path, auth: r.Header.Get("Authorization"), body: string(b)})
		f.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/chat.postMessage"):
			if f.retryHdr != "" {
				w.Header().Set("Retry-After", f.retryHdr)
			}
			if f.postCode != 0 {
				w.WriteHeader(f.postCode)
			}
			_, _ = io.WriteString(w, f.postBody)
		case strings.HasSuffix(r.URL.Path, "/files.getUploadURLExternal"):
			_, _ = io.WriteString(w, f.getBody)
		case strings.HasSuffix(r.URL.Path, "/files.completeUploadExternal"):
			_, _ = io.WriteString(w, f.complBody)
		default:
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	})
}

// get returns the calls recorded so far, under the lock the handler writes with.
func (f *fakeSlack) get() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func newClient(t *testing.T, f *fakeSlack) (*Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := New(srv.Client(), srv.URL+"/api")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, srv
}

func TestNewRejectsBadBaseURL(t *testing.T) {
	t.Parallel()
	for _, base := range []string{"", "slack.com/api", "ftp://slack.com/api", "://x"} {
		if _, err := New(nil, base); err == nil {
			t.Errorf("New(%q) must fail", base)
		}
	}
}

func TestPostMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		postBody string
		postCode int
		text     string
		blocks   string
		wantText string
		wantTS   string
		wantErr  bool
		wantCode string // expected sender.SlackError.Code, if any
		retryHdr string
		// wantErrText is a substring the error must carry, if any.
		wantErrText string
	}{
		{name: "ok returns ts and sends the given text", postBody: `{"ok":true,"ts":"1700.01"}`, text: "Hello", blocks: `[{"type":"section","text":{"type":"mrkdwn","text":"hi"}}]`, wantTS: "1700.01", wantText: "Hello"},
		{name: "empty text is derived from the blocks", postBody: `{"ok":true,"ts":"1700.03"}`, blocks: `[{"type":"header","text":{"type":"plain_text","text":"Weekly totals"}}]`, wantTS: "1700.03", wantText: "Weekly totals"},
		{name: "no text anywhere falls back to a generic line", postBody: `{"ok":true,"ts":"1700.04"}`, blocks: `[{"type":"divider"}]`, wantTS: "1700.04", wantText: "New message"},
		{name: "empty blocks are omitted", postBody: `{"ok":true,"ts":"1700.02"}`, text: "x", blocks: ``, wantTS: "1700.02", wantText: "x"},
		{name: "slack ok:false is a SlackError with the code", postBody: `{"ok":false,"error":"channel_not_found"}`, blocks: `[]`, wantErr: true, wantCode: "channel_not_found"},
		{name: "HTTP 429 is the ratelimited code", postCode: http.StatusTooManyRequests, postBody: `{}`, blocks: `[]`, wantErr: true, wantCode: "ratelimited"},
		{name: "HTTP 429 carries Retry-After", postCode: http.StatusTooManyRequests, retryHdr: "30", postBody: `{}`, blocks: `[]`, wantErr: true, wantCode: "ratelimited", wantErrText: "retry after 30s"},
		{name: "HTTP 500 is an error", postCode: http.StatusInternalServerError, postBody: `{}`, blocks: `[]`, wantErr: true},
		{name: "malformed blocks json fails before the call", postBody: `{"ok":true}`, blocks: `{not json`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fakeSlack{postBody: tt.postBody, postCode: tt.postCode, retryHdr: tt.retryHdr}
			c, _ := newClient(t, f)
			ts, err := c.PostMessage(context.Background(), "test-token", "C123", tt.text, tt.blocks)
			if tt.wantErrText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrText)) {
				t.Fatalf("error %v must contain %q", err, tt.wantErrText)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantCode != "" {
				var se *sender.SlackError
				if !errors.As(err, &se) || se.Code != tt.wantCode {
					t.Fatalf("want SlackError{%q}, got %v", tt.wantCode, err)
				}
			}
			if tt.wantErr {
				return
			}
			if ts != tt.wantTS {
				t.Fatalf("ts=%q want %q", ts, tt.wantTS)
			}
			if len(f.get()) != 1 || f.get()[0].auth != "Bearer test-token" {
				t.Fatalf("want one call with the bearer token, got %+v", f.get())
			}
			var body map[string]any
			if err := json.Unmarshal([]byte(f.get()[0].body), &body); err != nil {
				t.Fatalf("request body: %v", err)
			}
			if body["channel"] != "C123" {
				t.Errorf("channel not forwarded: %v", body["channel"])
			}
			if body["text"] != tt.wantText {
				t.Errorf("text = %q, want %q", body["text"], tt.wantText)
			}
			if _, has := body["blocks"]; has != (tt.blocks != "") {
				t.Errorf("blocks present=%v, want %v", has, tt.blocks != "")
			}
		})
	}
}

func TestPostMessageTransportError(t *testing.T) {
	t.Parallel()
	c, srv := newClient(t, &fakeSlack{})
	srv.Close()
	if _, err := c.PostMessage(context.Background(), "tok", "C1", "", `[]`); err == nil {
		t.Fatal("a closed server must surface an error")
	}
}

func TestUploadFile(t *testing.T) {
	t.Parallel()
	f := &fakeSlack{complBody: `{"ok":true}`}
	c, srv := newClient(t, f)
	f.getBody = `{"ok":true,"upload_url":"` + srv.URL + `/upload/x","file_id":"F999"}`
	id, err := c.UploadFile(context.Background(), "tok", "C123", []byte("%PDF-1.4 data"), "report.pdf")
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if id != "F999" {
		t.Fatalf("file id = %q, want F999", id)
	}
	want := []string{"/api/files.getUploadURLExternal", "/upload/x", "/api/files.completeUploadExternal"}
	if len(f.get()) != 3 {
		t.Fatalf("call sequence = %+v, want the 3-step flow %v", f.get(), want)
	}
	for i, w := range want {
		if f.get()[i].path != w {
			t.Fatalf("call %d = %s, want %s", i, f.get()[i].path, w)
		}
	}
	if f.get()[1].auth != "" {
		t.Errorf("the bot token must not be sent to the upload URL")
	}
}

func TestUploadFileSlackError(t *testing.T) {
	t.Parallel()
	c, _ := newClient(t, &fakeSlack{getBody: `{"ok":false,"error":"not_in_channel"}`})
	_, err := c.UploadFile(context.Background(), "tok", "C123", []byte("x"), "r.pdf")
	var se *sender.SlackError
	if !errors.As(err, &se) || se.Code != "not_in_channel" {
		t.Fatalf("want SlackError{not_in_channel}, got %v", err)
	}
}

// TestUploadFileRefusesForeignHost proves the bytes go only to slack.com, a
// subdomain of it, or the configured base host: a look-alike suffix is refused
// before any byte is sent.
func TestUploadFileRefusesForeignHost(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"example.net", "evilslack.com", "slack.com.example.net", "notslack.com"} {
		t.Run(host, func(t *testing.T) {
			f := &fakeSlack{getBody: `{"ok":true,"upload_url":"https://` + host + `/upload/x","file_id":"F1"}`}
			c, _ := newClient(t, f)
			_, err := c.UploadFile(context.Background(), "tok", "C123", []byte("x"), "r.pdf")
			if err == nil || !strings.Contains(err.Error(), "non-Slack host") {
				t.Fatalf("want a refusal for %s, got %v", host, err)
			}
			if n := len(f.get()); n != 1 {
				t.Fatalf("only getUploadURLExternal may be called, got %d calls", n)
			}
		})
	}
}

func TestAllowedUploadHost(t *testing.T) {
	t.Parallel()
	c, err := New(nil, "http://stub.local:8080/api")
	if err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string]bool{
		"slack.com": true, "files.slack.com": true, "FILES.SLACK.COM": true, "stub.local": true,
		"evilslack.com": false, "slack.com.evil.example": false, "": false,
	} {
		if got := c.allowedUploadHost(host); got != want {
			t.Errorf("allowedUploadHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestJoinChannel(t *testing.T) {
	t.Parallel()
	f := &fakeSlack{}
	c, _ := newClient(t, f)
	if err := c.JoinChannel(context.Background(), "tok", "C9"); err != nil {
		t.Fatalf("JoinChannel: %v", err)
	}
	if len(f.get()) != 1 || !strings.HasSuffix(f.get()[0].path, "/conversations.join") || !strings.Contains(f.get()[0].body, "channel=C9") {
		t.Fatalf("want one conversations.join for C9, got %+v", f.get())
	}
}
