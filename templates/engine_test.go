// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

func card(headline, body, action string, dir pb.Direction) *pb.ContentBlock {
	return &pb.ContentBlock{Block: &pb.ContentBlock_Card{Card: &pb.Card{
		Headline: headline, Body: body, Action: action, Direction: dir,
	}}}
}

func TestRenderContent(t *testing.T) {
	tests := []struct {
		name    string
		channel pb.Channel
		content *pb.RenderableContent
		wantErr bool
		// assert runs on a successful render.
		assert func(t *testing.T, p *pb.RenderedPayload)
	}{
		{
			name:    "email renders all cards, not just the first",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: &pb.RenderableContent{
				Subject:      "Monthly summary",
				GreetingName: "Jane",
				Blocks: []*pb.ContentBlock{
					card("Signups up", "More signups this month.", "Keep the launch email.", pb.Direction_DIRECTION_POSITIVE),
					card("Checkout slowed", "Pages took longer to load.", "Profile the API.", pb.Direction_DIRECTION_NEGATIVE),
					card("Steady traffic", "Same as last month.", "", pb.Direction_DIRECTION_NEUTRAL),
				},
				Cta: &pb.CallToAction{Label: "Open report", Url: "https://app.example.com/r/1"},
			},
			assert: func(t *testing.T, p *pb.RenderedPayload) {
				if p.GetEmailSubject() != "Monthly summary" {
					t.Errorf("subject = %q", p.GetEmailSubject())
				}
				for _, h := range []string{"Signups up", "Checkout slowed", "Steady traffic"} {
					if !strings.Contains(p.GetEmailHtml(), h) {
						t.Errorf("email missing card %q:\n%s", h, p.GetEmailHtml())
					}
				}
				if !strings.Contains(p.GetEmailHtml(), "https://app.example.com/r/1") {
					t.Errorf("email missing CTA url")
				}
			},
		},
		{
			name:    "email escapes hostile values",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: &pb.RenderableContent{
				Subject: "Report",
				Blocks:  []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "<script>alert(1)</script>"}}}},
			},
			assert: func(t *testing.T, p *pb.RenderedPayload) {
				if strings.Contains(p.GetEmailHtml(), "<script>") {
					t.Errorf("hostile value not escaped: %s", p.GetEmailHtml())
				}
				if !strings.Contains(p.GetEmailHtml(), "&lt;script&gt;") {
					t.Errorf("expected escaped script tag: %s", p.GetEmailHtml())
				}
			},
		},
		{
			name:    "an unsafe CTA scheme is dropped from both the HTML button and the text alternative",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: &pb.RenderableContent{
				Subject: "Report",
				Cta:     &pb.CallToAction{Label: "Open", Url: "javascript:alert(document.cookie)"},
			},
			assert: func(t *testing.T, p *pb.RenderedPayload) {
				if strings.Contains(p.GetEmailHtml(), "javascript:") {
					t.Errorf("unsafe scheme reached the HTML part: %s", p.GetEmailHtml())
				}
				if strings.Contains(p.GetEmailText(), "javascript:") {
					t.Errorf("unsafe scheme reached the text part: %q", p.GetEmailText())
				}
			},
		},
		{
			name:    "slack renders all cards as Block Kit",
			channel: pb.Channel_CHANNEL_SLACK,
			content: &pb.RenderableContent{
				Subject: "Monthly summary",
				Blocks: []*pb.ContentBlock{
					card("A", "body a", "act a", pb.Direction_DIRECTION_POSITIVE),
					card("B", "body b", "", pb.Direction_DIRECTION_NEGATIVE),
				},
			},
			assert: func(t *testing.T, p *pb.RenderedPayload) {
				var blocks []map[string]any
				if err := json.Unmarshal([]byte(p.GetSlackBlocksJson()), &blocks); err != nil {
					t.Fatalf("slack blocks not a JSON array: %v", err)
				}
				if len(blocks) < 3 { // header + 2 cards
					t.Errorf("expected >=3 blocks, got %d", len(blocks))
				}
				if p.GetSlackText() != "Monthly summary" {
					t.Errorf("slack fallback text = %q, want the subject", p.GetSlackText())
				}
				if !strings.Contains(p.GetSlackBlocksJson(), "body a") || !strings.Contains(p.GetSlackBlocksJson(), "body b") {
					t.Errorf("slack missing card bodies: %s", p.GetSlackBlocksJson())
				}
			},
		},
		{
			name:    "email subject required",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: &pb.RenderableContent{Blocks: []*pb.ContentBlock{card("x", "", "", 0)}},
			wantErr: true,
		},
		{
			name:    "card without headline is a render error",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: &pb.RenderableContent{Subject: "s", Blocks: []*pb.ContentBlock{card("", "body", "", 0)}},
			wantErr: true,
		},
		{
			name:    "nil content errors",
			channel: pb.Channel_CHANNEL_EMAIL,
			content: nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RenderContent(tt.channel, tt.content)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assert != nil {
				tt.assert(t, got)
			}
		})
	}
}

// TestRenderEmailTextAlternative pins the plain-text part: greeting, block
// prose, stat lines, and the CTA URL all survive without any markup.
func TestRenderEmailTextAlternative(t *testing.T) {
	p, err := RenderContent(pb.Channel_CHANNEL_EMAIL, &pb.RenderableContent{
		Subject:      "Monthly summary",
		GreetingName: "Jane",
		Blocks: []*pb.ContentBlock{
			card("Orders up 22%", "The median basket grew to 4 items.", "Keep the weekend promo.", pb.Direction_DIRECTION_POSITIVE),
			{Block: &pb.ContentBlock_StatList{StatList: &pb.StatList{Stats: []*pb.Stat{
				{Label: "Orders", Value: "12", Delta: "+3"},
			}}}},
		},
		Cta: &pb.CallToAction{Label: "Open the full report", Url: "https://r/x"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{"Hi Jane,", "Orders up 22%", "What next: Keep the weekend promo.", "- Orders: 12 (+3)", "Open the full report: https://r/x"} {
		if !strings.Contains(p.GetEmailText(), want) {
			t.Errorf("text missing %q; got:\n%s", want, p.GetEmailText())
		}
	}
	if strings.Contains(p.GetEmailText(), "<") {
		t.Errorf("text part contains markup: %q", p.GetEmailText())
	}
}

// TestRenderEmailTextGreetingNotEscaped proves the text-part greeting carries a
// name verbatim: HTML-escaping it (as the HTML part must) would show a text-only
// client literal entities like "O&#39;Doe" instead of the real name.
func TestRenderEmailTextGreetingNotEscaped(t *testing.T) {
	p, err := RenderContent(pb.Channel_CHANNEL_EMAIL, &pb.RenderableContent{
		Subject:      "s",
		GreetingName: "O'Doe & Co",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.HasPrefix(p.GetEmailText(), "Hi O'Doe & Co,") {
		t.Errorf("text greeting escaped or wrong: %q", p.GetEmailText())
	}
}

func TestRenderSlackSectionTruncated(t *testing.T) {
	long := strings.Repeat("x", 5000)
	got, err := RenderContent(pb.Channel_CHANNEL_SLACK, &pb.RenderableContent{
		Subject: "s",
		Blocks:  []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: long}}}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var blocks []map[string]any
	if err := json.Unmarshal([]byte(got.GetSlackBlocksJson()), &blocks); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	for _, b := range blocks {
		if txt, ok := b["text"].(map[string]any); ok {
			if s, ok := txt["text"].(string); ok && len([]rune(s)) > slackSectionTextMax {
				t.Errorf("section text %d runes exceeds cap %d", len([]rune(s)), slackSectionTextMax)
			}
		}
	}
}

// TestSlackFallbackText pins the Slack fallback: subject, else the first heading,
// paragraph or card headline, else empty.
func TestSlackFallbackText(t *testing.T) {
	para := func(s string) *pb.ContentBlock {
		return &pb.ContentBlock{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: s}}}
	}
	tests := []struct {
		name string
		c    *pb.RenderableContent
		want string
	}{
		{"subject wins", &pb.RenderableContent{Subject: "S", Blocks: []*pb.ContentBlock{para("p")}}, "S"},
		{"first paragraph", &pb.RenderableContent{Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Divider{Divider: &pb.Divider{}}}, para("p")}}, "p"},
		{"card headline", &pb.RenderableContent{Blocks: []*pb.ContentBlock{card("H", "b", "", 0)}}, "H"},
		{"nothing", &pb.RenderableContent{Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Divider{Divider: &pb.Divider{}}}}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := RenderContent(pb.Channel_CHANNEL_SLACK, tt.c)
			if err != nil {
				t.Fatal(err)
			}
			if p.GetSlackText() != tt.want {
				t.Fatalf("SlackText = %q, want %q", p.GetSlackText(), tt.want)
			}
		})
	}
}

// TestSlackLimitsAtTheBoundary pins Slack's two caps at their exact values: a
// message of exactly 50 blocks and a section of exactly 3000 characters are
// kept whole; one more of either is cut, with the note for blocks.
func TestSlackLimitsAtTheBoundary(t *testing.T) {
	para := func(s string) *pb.ContentBlock {
		return &pb.ContentBlock{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: s}}}
	}
	render := func(t *testing.T, blocks []*pb.ContentBlock) []map[string]any {
		t.Helper()
		got, err := RenderContent(pb.Channel_CHANNEL_SLACK, &pb.RenderableContent{Blocks: blocks})
		if err != nil {
			t.Fatal(err)
		}
		var out []map[string]any
		if err := json.Unmarshal([]byte(got.GetSlackBlocksJson()), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, n := range []int{50, 51} {
		blocks := make([]*pb.ContentBlock, n)
		for i := range blocks {
			blocks[i] = para("p")
		}
		out := render(t, blocks)
		last, _ := json.Marshal(out[len(out)-1])
		cut := strings.Contains(string(last), "Some items were omitted")
		if len(out) != 50 || cut != (n == 51) {
			t.Errorf("%d blocks: got %d blocks, note=%v", n, len(out), cut)
		}
	}
	for _, n := range []int{3000, 3001} {
		out := render(t, []*pb.ContentBlock{para(strings.Repeat("a", n))})
		text := out[0]["text"].(map[string]any)["text"].(string)
		whole := text == strings.Repeat("a", n)
		if whole != (n == 3000) || len([]rune(text)) > 3000 {
			t.Errorf("%d characters: got %d, whole=%v", n, len([]rune(text)), whole)
		}
	}
}

// TestSlackEmptyContentIsARenderError pins that Slack content with no subject
// and no blocks is an error, as it is for email, never a blank post.
func TestSlackEmptyContentIsARenderError(t *testing.T) {
	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_SLACK, pb.Channel_CHANNEL_EMAIL} {
		if _, err := RenderContent(ch, &pb.RenderableContent{}); err == nil {
			t.Errorf("%s: empty content rendered without an error", ch)
		}
	}
	if _, err := RenderContent(pb.Channel_CHANNEL_SLACK, &pb.RenderableContent{Subject: "s"}); err != nil {
		t.Errorf("a subject alone is enough for Slack: %v", err)
	}
}

// TestSlackContentIsInert pins that content cannot become Slack markup: a CTA
// link must be http or https, and <, > and & in any text are escaped, so
// <!channel> is text, not a mention, in the blocks and in the fallback text.
func TestSlackContentIsInert(t *testing.T) {
	render := func(t *testing.T, c *pb.RenderableContent) *pb.RenderedPayload {
		t.Helper()
		got, err := RenderContent(pb.Channel_CHANNEL_SLACK, c)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := render(t, &pb.RenderableContent{Subject: "s", Cta: &pb.CallToAction{Url: "javascript:alert(1)", Label: "Open"}})
	if strings.Contains(got.GetSlackBlocksJson(), "javascript:") {
		t.Errorf("a javascript: CTA reached the blocks: %s", got.GetSlackBlocksJson())
	}
	got = render(t, &pb.RenderableContent{Subject: "s", Cta: &pb.CallToAction{Url: "https://app.example.com/x?a=1&b=2", Label: "Open <now>"}})
	var cta []map[string]any
	if err := json.Unmarshal([]byte(got.GetSlackBlocksJson()), &cta); err != nil {
		t.Fatal(err)
	}
	if text := cta[len(cta)-1]["text"].(map[string]any)["text"]; text != "<https://app.example.com/x?a=1&amp;b=2|Open &lt;now&gt;>" {
		t.Errorf("CTA link = %q, want Slack's escaping", text)
	}
	got = render(t, &pb.RenderableContent{Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "<!channel> deploy & go"}}}}})
	var blocks []map[string]any
	if err := json.Unmarshal([]byte(got.GetSlackBlocksJson()), &blocks); err != nil {
		t.Fatal(err)
	}
	if text := blocks[0]["text"].(map[string]any)["text"]; text != "&lt;!channel&gt; deploy &amp; go" {
		t.Errorf("paragraph text = %q", text)
	}
	if got.GetSlackText() != "&lt;!channel&gt; deploy &amp; go" {
		t.Errorf("fallback text = %q", got.GetSlackText())
	}
}

// TestTruncateKeepsRunes pins truncation by character: text within the cap is
// untouched whatever its bytes, and a cut never splits a UTF-8 sequence.
func TestTruncateKeepsRunes(t *testing.T) {
	short := strings.Repeat("é", 2000)
	if got := truncate(short, 3000); got != short {
		t.Errorf("2000 é cut to %d runes under a 3000 cap", len([]rune(got)))
	}
	got := truncate(strings.Repeat("é", 3001), 3000)
	if n := len([]rune(got)); n != 3000 || !utf8.ValidString(got) || !strings.HasSuffix(got, "…") {
		t.Errorf("3001 é cut to %d runes, valid UTF-8 %v", n, utf8.ValidString(got))
	}
}
