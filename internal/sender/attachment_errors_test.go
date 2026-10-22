// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/internal/attachments"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
)

const (
	orgA = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"
	orgB = "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"
)

// sendWithKey runs one SMTP send from org with key as attachment_key, against a
// relay that is never reached, and returns the outcome.
func sendWithKey(t *testing.T, store sender.ObjectFetcher, org, key string) dispatch.SendResult {
	t.Helper()
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: "127.0.0.1:1", From: "Example <noreply@example.com>", Timeout: time.Second}, store)
	in := smtpInput()
	in.OrgID = org
	in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: key})
	return s.Send(context.Background(), in)
}

// TestAttachmentKeyErrors pins how a failed attachment fetch ends: a key that
// is invalid, names no file, names a directory, names a file the service may
// not read, or leaves the organization's folder through a symlink can never
// succeed, so it is permanent; any other fetch failure may be a store outage,
// so it is retried.
func TestAttachmentKeyErrors(t *testing.T) {
	root := t.TempDir()
	org := filepath.Join(root, orgA)
	if err := os.MkdirAll(filepath.Join(org, "reports", "folder"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "secret.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "secret.txt"), filepath.Join(org, "reports", "link.pdf")); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(org, "reports", "locked.pdf")
	if err := os.WriteFile(locked, []byte("%PDF"), 0o000); err != nil {
		t.Fatal(err)
	}
	dir := attachments.Dir{Root: root}
	tests := []struct {
		name  string
		store sender.ObjectFetcher
		key   string
		want  dispatch.Outcome
	}{
		{"a key that climbs out of the store", dir, "../etc/passwd", dispatch.OutcomePermanent},
		{"an absolute key", dir, "/etc/passwd", dispatch.OutcomePermanent},
		{"a key with a NUL byte", dir, "reports/r\x00.pdf", dispatch.OutcomePermanent},
		{"a key with no file", dir, "reports/missing.pdf", dispatch.OutcomePermanent},
		{"a key naming a directory", dir, "reports/folder", dispatch.OutcomePermanent},
		{"a symlink out of the organization's folder", dir, "reports/link.pdf", dispatch.OutcomePermanent},
		{"a store outage", fakeFetcher{err: errors.New("connection refused")}, "reports/r.pdf", dispatch.OutcomeTransient},
	}
	if runtime.GOOS != "windows" && os.Geteuid() != 0 {
		tests = append(tests, struct {
			name  string
			store sender.ObjectFetcher
			key   string
			want  dispatch.Outcome
		}{"an unreadable file", dir, "reports/locked.pdf", dispatch.OutcomePermanent})
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if res := sendWithKey(t, tc.store, orgA, tc.key); res.Outcome != tc.want {
				t.Fatalf("outcome %v (%v), want %v", res.Outcome, res.Err, tc.want)
			}
		})
	}
}

// TestAttachmentKeysAreScopedByOrg pins that a key resolves inside the sending
// organization's folder: org A's own key reaches the relay with A's file in
// the message, and the same key from org B finds nothing and fails
// permanently, without reading A's file.
func TestAttachmentKeysAreScopedByOrg(t *testing.T) {
	dir := attachments.Dir{Root: t.TempDir()}
	file := []byte("%PDF-org-A-report")
	if err := dir.Put(context.Background(), orgA, "reports/r.pdf", file); err != nil {
		t.Fatal(err)
	}
	relay := newScriptedSMTP(t, "250 ok", "250 queued")
	s := sender.NewSMTPSender(sender.SMTPConfig{Addr: relay.addr, From: "Example <noreply@example.com>", Timeout: 5 * time.Second}, dir)
	in := smtpInput()
	in.OrgID = orgA
	in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
	if res := s.Send(context.Background(), in); res.Outcome != dispatch.OutcomeSuccess {
		t.Fatalf("org A's own key: outcome %v (%v), want success", res.Outcome, res.Err)
	}
	body := strings.ReplaceAll(<-relay.body, "\r\n", "")
	if !strings.Contains(body, base64.StdEncoding.EncodeToString(file)) {
		t.Fatalf("the relay did not receive org A's file:\n%s", body)
	}
	if res := sendWithKey(t, dir, orgB, "reports/r.pdf"); res.Outcome != dispatch.OutcomePermanent {
		t.Fatalf("org B naming org A's key: outcome %v (%v), want permanent", res.Outcome, res.Err)
	}
	if res := sendWithKey(t, dir, orgB, "../"+orgA+"/reports/r.pdf"); res.Outcome != dispatch.OutcomePermanent {
		t.Fatalf("org B climbing into org A: outcome %v (%v), want permanent", res.Outcome, res.Err)
	}
}
