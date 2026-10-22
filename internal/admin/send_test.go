// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// TestPreviewBannerMarksEveryEmailBody proves reviewers see the preview marking
// whether the email renders the base body or an email override, and that the
// source content is not mutated.
func TestPreviewBannerMarksEveryEmailBody(t *testing.T) {
	tests := []struct {
		name string
		c    *pb.MessageContent
	}{
		{"base only", &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Hello"}}},
		{"email override", &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Hello"}, Email: &pb.RenderableContent{Subject: "Hello by email"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := proto.Clone(tt.c)
			out := withPreviewBanner(tt.c, 3)
			for _, rc := range []*pb.RenderableContent{out.GetBase(), out.GetEmail()} {
				if rc == nil {
					continue
				}
				if !strings.HasPrefix(rc.GetSubject(), "[PREVIEW] ") {
					t.Errorf("subject %q is not marked", rc.GetSubject())
				}
				if !strings.Contains(rc.GetBlocks()[0].GetParagraph().GetText(), "Intended audience: 3") {
					t.Errorf("first block is not the banner: %v", rc.GetBlocks())
				}
			}
			if !proto.Equal(orig, tt.c) {
				t.Fatal("the source content was mutated")
			}
			again := withPreviewBanner(out, 3)
			if strings.Count(again.GetBase().GetSubject(), "[PREVIEW]") != 1 {
				t.Fatalf("subject marked twice: %q", again.GetBase().GetSubject())
			}
		})
	}
}

func TestContentWithGreeting(t *testing.T) {
	c := &pb.MessageContent{Base: &pb.RenderableContent{Subject: "s"}, Email: &pb.RenderableContent{Subject: "e"}}
	out := contentWithGreeting(c, "Jane")
	if out.GetBase().GetGreetingName() != "Jane" || out.GetEmail().GetGreetingName() != "Jane" {
		t.Fatalf("greeting not set on every body: %v", out)
	}
	if c.GetBase().GetGreetingName() != "" {
		t.Fatal("the source content was mutated")
	}
	if contentWithGreeting(c, "") != c {
		t.Fatal("an empty name must return the content unchanged")
	}
}

func write(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReadRecipients(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    []recipient
		wantErr string
	}{
		{"names, comments, case and duplicates", "# audience\nB@Example.com,Bea\n\na@example.com\nb@example.com, Bea again\n", []recipient{{"a@example.com", ""}, {"b@example.com", "Bea"}}, ""},
		{"a bad address names its line", "a@example.com\nnot-an-address\n", nil, "line 2"},
		{"a display-name form is refused", "Jane <jane@example.com>\n", nil, "line 1"},
		{"empty", "# nothing\n", nil, "lists no addresses"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readRecipients(write(t, "r.txt", tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || len(got) != len(tt.want) {
				t.Fatalf("got %v, %v; want %v", got, err, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestChunksAndKeys(t *testing.T) {
	rs := make([]recipient, 2500)
	chunks := chunkRecipients(rs, maxEntries)
	if len(chunks) != 3 || len(chunks[2]) != 500 || chunkCount(2500) != 3 || chunkCount(1000) != 1 {
		t.Fatalf("chunking is off: %d chunks", len(chunks))
	}
	if sendIdempotencyKey("p", 0, 1) != "adhoc-send:p" || sendIdempotencyKey("p", 2, 3) != "adhoc-send:p:2" {
		t.Fatal("idempotency keys changed shape")
	}
	if audienceFingerprint([]string{"b", "a"}) != audienceFingerprint([]string{"a", "b"}) {
		t.Fatal("the audience fingerprint must not depend on order")
	}
}

func TestSanitizeFilename(t *testing.T) {
	for in, want := range map[string]string{
		"report.pdf": "report.pdf", "../../etc/passwd": "passwd", `a\b\c d.pdf`: "c_d.pdf", "...": "attachment",
		strings.Repeat("é", 150) + ".pdf": strings.Repeat("é", 98) + ".pdf",
	} {
		if got := sanitizeFilename(in); got != want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}
