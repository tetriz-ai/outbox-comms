// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"strings"
	"testing"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

var fixedNow = time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)

func body() *pb.RenderedPayload {
	return &pb.RenderedPayload{EmailSubject: "s", EmailHtml: "<p>Hi John,</p><p>body</p>", EmailText: "Hi John,\n\nbody\n"}
}

// TestWrapEmail pins the chrome contract: every option degrades on its own, the
// footer notice follows the category, and nothing unsafe reaches an href.
func TestWrapEmail(t *testing.T) {
	t.Parallel()
	full := EmailChromeOptions{
		Now: fixedNow, BrandName: "Example Corp", SiteURL: "https://example.com", LogoURL: "https://example.com/logo.png",
		PreferencesURL: "https://app.example.com/preferences", PrivacyURL: "https://example.com/privacy", TermsURL: "https://example.com/terms",
	}
	tests := []struct {
		name     string
		category pb.MessageCategory
		opts     EmailChromeOptions
		has      []string
		hasText  []string
		not      []string
	}{
		{
			name: "transactional with every option", category: pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL, opts: full,
			has: []string{"<!DOCTYPE html>", `<a href="https://example.com" target="_blank"`, `src="https://example.com/logo.png"`, `alt="Example Corp"`,
				"&copy; 2026 Example Corp. All rights reserved.", `href="https://example.com/privacy"`, `href="https://example.com/terms"`,
				"<p>Hi John,</p><p>body</p>", "you can safely ignore it"},
			hasText: []string{"© 2026 Example Corp. All rights reserved.", "Privacy Policy: https://example.com/privacy", "you can safely ignore it"},
			not:     []string{"Manage notification preferences"},
		},
		{
			name: "non-transactional links the preferences page", category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, opts: full,
			has:     []string{"notifications from Example Corp are enabled for you", `<a href="https://app.example.com/preferences"`},
			hasText: []string{"Manage notification preferences: https://app.example.com/preferences"},
			not:     []string{"safely ignore"},
		},
		{
			name: "no options: no header, no copyright, no links", category: pb.MessageCategory_MESSAGE_CATEGORY_ALERT, opts: EmailChromeOptions{Now: fixedNow},
			has: []string{"<p>Hi John,</p><p>body</p>", "notifications are enabled for you."},
			not: []string{"<img", "&copy;", "Privacy Policy", "Manage notification preferences", "border-bottom:1px solid #EDEDED"},
		},
		{
			name: "brand without logo renders a text wordmark", category: pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
			opts: EmailChromeOptions{Now: fixedNow, BrandName: "Example Corp"},
			has:  []string{">Example Corp</span>", "&copy; 2026 Example Corp."},
			not:  []string{"<img"},
		},
		{
			name: "unsafe URLs are dropped", category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT,
			opts: EmailChromeOptions{Now: fixedNow, LogoURL: "javascript:alert(1)", PreferencesURL: "javascript:alert(1)", PrivacyURL: "data:text/html,x"},
			not:  []string{"javascript:", "data:text", "<img", "Manage notification preferences"},
		},
		{
			name: "hostile brand name is escaped", category: pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
			opts: EmailChromeOptions{Now: fixedNow, BrandName: `<script>x</script>`},
			has:  []string{"&lt;script&gt;"},
			not:  []string{"<script>"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := WrapEmail(body(), tt.category, tt.opts)
			for _, w := range tt.has {
				if !strings.Contains(p.GetEmailHtml(), w) {
					t.Errorf("html missing %q", w)
				}
			}
			for _, w := range tt.hasText {
				if !strings.Contains(p.GetEmailText(), w) {
					t.Errorf("text missing %q in %q", w, p.GetEmailText())
				}
			}
			for _, w := range tt.not {
				if strings.Contains(p.GetEmailHtml(), w) {
					t.Errorf("html unexpectedly contains %q", w)
				}
			}
			if p.GetEmailSubject() != "s" {
				t.Errorf("subject changed: %q", p.GetEmailSubject())
			}
		})
	}
}

// TestWrapEmailPassesSlackThrough proves a payload with no email body is
// returned unchanged.
func TestWrapEmailPassesSlackThrough(t *testing.T) {
	t.Parallel()
	in := &pb.RenderedPayload{SlackBlocksJson: `[]`}
	if out := WrapEmail(in, pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, EmailChromeOptions{}); out != in {
		t.Fatal("a Slack payload must pass through unchanged")
	}
}

// TestWrapEmailEscapesPrefsURL proves a hostile preferences URL cannot break out
// of the footer anchor.
func TestWrapEmailEscapesPrefsURL(t *testing.T) {
	t.Parallel()
	p := WrapEmail(
		&pb.RenderedPayload{EmailSubject: "s", EmailHtml: "<p>b</p>"},
		pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT,
		EmailChromeOptions{Now: fixedNow, PreferencesURL: `https://x/"><script>alert(1)</script>`},
	)
	if strings.Contains(p.GetEmailHtml(), "<script>") {
		t.Errorf("hostile prefs URL not escaped: %s", p.GetEmailHtml())
	}
}

// TestButtonHTML pins the shared CTA button: a safe URL renders an escaped
// button; a hostile label is escaped.
func TestButtonHTML(t *testing.T) {
	t.Parallel()
	href, ok := safeURL("https://app.example.com/login")
	if !ok {
		t.Fatal("https URL must be safe")
	}
	got := buttonHTML(href, `Sign in <now>`)
	for _, want := range []string{`href="https://app.example.com/login"`, `class="oc-btn"`, "background:" + accentColor, "Sign in &lt;now&gt;"} {
		if !strings.Contains(got, want) {
			t.Errorf("button missing %q: %s", want, got)
		}
	}
	for _, bad := range []string{"javascript:alert(1)", "data:text/html,x", "", "::not a url"} {
		if _, ok := safeURL(bad); ok {
			t.Errorf("safeURL(%q) must be false", bad)
		}
	}
}
