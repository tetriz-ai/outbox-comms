// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"unicode/utf8"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// engine.go is the channel-agnostic rendering engine (the block-based content
// model): one generic walker per channel turns a RenderableContent block stream
// into the channel artifact. All usecases share these two functions — adding a
// template adds producer-side blocks, never a renderer here.

// slackMaxBlocks is Slack's hard cap on blocks per message (chat.postMessage). We
// truncate beyond it with a trailing notice rather than letting the send fail.
const slackMaxBlocks = 50

// RenderContent renders block content for one channel. It escapes every value
// for that channel. Nil or empty content is an error, so a message is never sent
// blank.
func RenderContent(ch pb.Channel, content *pb.RenderableContent) (*pb.RenderedPayload, error) {
	if content == nil {
		return nil, fmt.Errorf("render: nil content for channel %s", ch)
	}
	switch ch {
	case pb.Channel_CHANNEL_EMAIL:
		rendered, err := renderEmailContent(content)
		if err != nil {
			return nil, fmt.Errorf("render email content: %w", err)
		}
		return rendered, nil
	case pb.Channel_CHANNEL_SLACK:
		rendered, err := renderSlackContent(content)
		if err != nil {
			return nil, fmt.Errorf("render slack content: %w", err)
		}
		return rendered, nil
	default:
		return nil, fmt.Errorf("render: unsupported channel %s", ch)
	}
}

// renderEmailContent walks the block stream into the body HTML plus a plain-text
// alternative (WrapEmail adds the shared chrome around both). Every HTML value is
// escaped except a RawHtml block, whose producer owns escaping (the email-only
// escape hatch — it has no text-part equivalent and is skipped there). A
// SLACK-only RawSlackBlocks block is skipped on both.
func renderEmailContent(c *pb.RenderableContent) (*pb.RenderedPayload, error) {
	if c.GetSubject() == "" {
		return nil, fmt.Errorf("render email: subject is required")
	}
	var b strings.Builder
	var t strings.Builder
	b.WriteString(emailGreeting(c.GetGreetingName()))
	t.WriteString(greetingText(c.GetGreetingName()) + ",\n\n")
	for _, blk := range c.GetBlocks() {
		if err := writeEmailBlock(&b, blk); err != nil {
			return nil, fmt.Errorf("render email block: %w", err)
		}
		writeTextBlock(&t, blk)
	}
	if cta := c.GetCta(); cta != nil && cta.GetUrl() != "" {
		// safeURL once, shared by the HTML button and the text line, so an
		// unsafe scheme drops from both and is logged only once.
		if href, ok := safeURL(cta.GetUrl()); ok {
			b.WriteString(buttonHTML(href, cta.GetLabel()))
			fmt.Fprintf(&t, "%s: %s\n", cta.GetLabel(), cta.GetUrl())
		}
	}
	return &pb.RenderedPayload{
		EmailSubject: c.GetSubject(),
		EmailHtml:    b.String(),
		EmailText:    strings.TrimRight(t.String(), "\n") + "\n",
	}, nil
}

// writeTextBlock renders one block into the plain-text alternative. Best-effort by
// design: it never errors (writeEmailBlock already surfaced malformed blocks) and
// skips the channel-specific escape hatches, which have no text mapping.
func writeTextBlock(t *strings.Builder, blk *pb.ContentBlock) {
	switch v := blk.GetBlock().(type) {
	case *pb.ContentBlock_Heading:
		t.WriteString(v.Heading.GetText() + "\n\n")
	case *pb.ContentBlock_Paragraph:
		t.WriteString(v.Paragraph.GetText() + "\n\n")
	case *pb.ContentBlock_Card:
		t.WriteString(v.Card.GetHeadline() + "\n")
		if v.Card.GetBody() != "" {
			t.WriteString(v.Card.GetBody() + "\n")
		}
		if v.Card.GetAction() != "" {
			t.WriteString("What next: " + v.Card.GetAction() + "\n")
		}
		t.WriteString("\n")
	case *pb.ContentBlock_StatList:
		for _, s := range v.StatList.GetStats() {
			t.WriteString("- " + s.GetLabel() + ": " + s.GetValue())
			if s.GetDelta() != "" {
				t.WriteString(" (" + s.GetDelta() + ")")
			}
			t.WriteString("\n")
		}
		t.WriteString("\n")
	case *pb.ContentBlock_Divider:
		t.WriteString("----\n\n")
	default:
	}
}

// writeEmailBlock renders one block to HTML. A malformed block (e.g. a Card with no
// headline) is a render error so the producer bug surfaces, not a blank section.
func writeEmailBlock(b *strings.Builder, blk *pb.ContentBlock) error {
	switch v := blk.GetBlock().(type) {
	case *pb.ContentBlock_Heading:
		tag := "h2"
		if v.Heading.GetLevel() == pb.HeadingLevel_HEADING_LEVEL_TITLE {
			tag = "h1"
		}
		fmt.Fprintf(b, "<%s>%s</%s>", tag, html.EscapeString(v.Heading.GetText()), tag)
	case *pb.ContentBlock_Paragraph:
		fmt.Fprintf(b, "<p>%s</p>", html.EscapeString(v.Paragraph.GetText()))
	case *pb.ContentBlock_Card:
		if v.Card.GetHeadline() == "" {
			return fmt.Errorf("render email: card block missing headline")
		}
		fmt.Fprintf(b,
			`<div style="border:1px solid #e5e7eb;border-left:4px solid %s;border-radius:6px;padding:12px 16px;margin:12px 0;">`,
			directionColor(v.Card.GetDirection()),
		)
		fmt.Fprintf(b, `<p style="margin:0 0 4px;font-weight:700;">%s</p>`, html.EscapeString(v.Card.GetHeadline()))
		if v.Card.GetBody() != "" {
			fmt.Fprintf(b, `<p style="margin:0 0 4px;">%s</p>`, html.EscapeString(v.Card.GetBody()))
		}
		if v.Card.GetAction() != "" {
			fmt.Fprintf(b, `<p style="margin:0;color:%s;"><strong>What next:</strong> %s</p>`, accentColor, html.EscapeString(v.Card.GetAction()))
		}
		b.WriteString("</div>")
	case *pb.ContentBlock_StatList:
		b.WriteString(`<table style="width:100%;border-collapse:collapse;margin:12px 0;">`)
		for _, s := range v.StatList.GetStats() {
			delta := ""
			if s.GetDelta() != "" {
				delta = fmt.Sprintf(`<span style="color:%s;"> (%s)</span>`, directionColor(s.GetDirection()), html.EscapeString(s.GetDelta()))
			}
			fmt.Fprintf(b,
				`<tr><td style="padding:4px 8px;color:#6b7280;">%s</td><td style="padding:4px 8px;font-weight:600;text-align:right;">%s%s</td></tr>`,
				html.EscapeString(s.GetLabel()), html.EscapeString(s.GetValue()), delta,
			)
		}
		b.WriteString("</table>")
	case *pb.ContentBlock_Divider:
		b.WriteString(`<hr style="border:none;border-top:1px solid #e5e7eb;margin:16px 0;"/>`)
	case *pb.ContentBlock_RawHtml:
		// Escape hatch: producer owns escaping; emit verbatim.
		b.WriteString(v.RawHtml.GetHtml())
	case *pb.ContentBlock_RawSlackBlocks:
		// SLACK-only block; ignored on email.
	default:
		return fmt.Errorf("render email: unknown block kind %T", blk.GetBlock())
	}
	return nil
}

// renderSlackContent walks the block stream into a Block Kit blocks array (JSON).
// JSON encoding keeps the array well formed, but Slack still reads mrkdwn inside
// it, so every text goes through slackEscape and a CTA link must pass safeURL.
// The array is capped at slackMaxBlocks with a trailing truncation notice so an
// over-long body degrades instead of failing the send. Content with no subject
// and no blocks is an error, as for email.
func renderSlackContent(c *pb.RenderableContent) (*pb.RenderedPayload, error) {
	if c.GetSubject() == "" && len(c.GetBlocks()) == 0 {
		return nil, fmt.Errorf("render slack: content has no subject and no blocks")
	}
	blocks := make([]map[string]any, 0, len(c.GetBlocks())+2)
	if c.GetSubject() != "" {
		blocks = append(blocks, map[string]any{"type": "header", "text": map[string]any{"type": "plain_text", "text": truncate(c.GetSubject(), 150)}})
	}
	if g := slackGreeting(c.GetGreetingName()); g != "" {
		blocks = append(blocks, sectionBlock(g))
	}
	for _, blk := range c.GetBlocks() {
		bs, err := slackBlocksFor(blk)
		if err != nil {
			return nil, fmt.Errorf("render slack block: %w", err)
		}
		blocks = append(blocks, bs...)
	}
	if cta := c.GetCta(); cta != nil {
		if _, ok := safeURL(cta.GetUrl()); ok {
			link := strings.ReplaceAll(slackEscape(cta.GetUrl()), "|", "%7C")
			blocks = append(blocks, sectionBlock(fmt.Sprintf("<%s|%s>", link, slackEscape(cta.GetLabel()))))
		}
	}
	blocks = capSlackBlocks(blocks)
	out, err := json.Marshal(blocks)
	if err != nil {
		return nil, fmt.Errorf("marshal slack blocks: %w", err)
	}
	return &pb.RenderedPayload{SlackBlocksJson: string(out), SlackText: slackEscape(slackFallback(c))}, nil
}

// slackBlocksFor renders one content block to zero or more Block Kit blocks.
func slackBlocksFor(blk *pb.ContentBlock) ([]map[string]any, error) {
	switch v := blk.GetBlock().(type) {
	case *pb.ContentBlock_Heading:
		if v.Heading.GetLevel() == pb.HeadingLevel_HEADING_LEVEL_TITLE {
			return []map[string]any{{"type": "header", "text": map[string]any{"type": "plain_text", "text": truncate(v.Heading.GetText(), 150)}}}, nil
		}
		return []map[string]any{sectionBlock("*" + slackEscape(v.Heading.GetText()) + "*")}, nil
	case *pb.ContentBlock_Paragraph:
		return []map[string]any{sectionBlock(slackEscape(v.Paragraph.GetText()))}, nil
	case *pb.ContentBlock_Card:
		if v.Card.GetHeadline() == "" {
			return nil, fmt.Errorf("render slack: card block missing headline")
		}
		text := fmt.Sprintf("%s *%s*", directionMarker(v.Card.GetDirection()), slackEscape(v.Card.GetHeadline()))
		if v.Card.GetBody() != "" {
			text += "\n" + slackEscape(v.Card.GetBody())
		}
		if v.Card.GetAction() != "" {
			text += "\n*What next:* " + slackEscape(v.Card.GetAction())
		}
		return []map[string]any{sectionBlock(strings.TrimSpace(text))}, nil
	case *pb.ContentBlock_StatList:
		var lines []string
		for _, s := range v.StatList.GetStats() {
			line := fmt.Sprintf("• *%s:* %s", slackEscape(s.GetLabel()), slackEscape(s.GetValue()))
			if s.GetDelta() != "" {
				line += fmt.Sprintf(" (%s)", slackEscape(s.GetDelta()))
			}
			lines = append(lines, line)
		}
		return []map[string]any{sectionBlock(strings.Join(lines, "\n"))}, nil
	case *pb.ContentBlock_Divider:
		return []map[string]any{{"type": "divider"}}, nil
	case *pb.ContentBlock_RawSlackBlocks:
		// Escape hatch: producer owns the bare Block Kit array; splice it in.
		var raw []map[string]any
		if err := json.Unmarshal([]byte(v.RawSlackBlocks.GetBlocksJson()), &raw); err != nil {
			return nil, fmt.Errorf("render slack: raw_slack_blocks is not a JSON blocks array: %w", err)
		}
		return raw, nil
	case *pb.ContentBlock_RawHtml:
		// EMAIL-only block; ignored on Slack.
		return nil, nil
	default:
		return nil, fmt.Errorf("render slack: unknown block kind %T", blk.GetBlock())
	}
}

// capSlackBlocks trims the block array to slackMaxBlocks, leaving room for a
// trailing context note that flags the truncation (so a clipped digest reads as
// clipped, not complete).
func capSlackBlocks(blocks []map[string]any) []map[string]any {
	if len(blocks) <= slackMaxBlocks {
		return blocks
	}
	kept := blocks[:slackMaxBlocks-1]
	kept = append(kept, map[string]any{"type": "context", "elements": []map[string]any{{"type": "mrkdwn", "text": "_Some items were omitted._"}}})
	return kept
}

// slackSectionTextMax is Slack's hard per-section text cap (chat.postMessage
// rejects a section mrkdwn text element over 3000 chars). We truncate to it so an
// over-long card/paragraph degrades instead of failing the whole send.
const slackSectionTextMax = 3000

func sectionBlock(mrkdwn string) map[string]any {
	return map[string]any{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": truncate(mrkdwn, slackSectionTextMax)}}
}

// emailGreeting / slackGreeting reuse the missing-optional-field degradation: a
// blank name yields a generic greeting.
func emailGreeting(name string) string {
	return fmt.Sprintf("<p>%s,</p>", greeting(name))
}

func slackGreeting(name string) string {
	if name == "" {
		return ""
	}
	return "Hi " + slackEscape(name)
}

// slackEscaper replaces the three characters Slack's mrkdwn reads as markup.
var slackEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// slackEscape makes content text inert in Slack mrkdwn, so it cannot open a link
// or a mention such as <!channel>.
func slackEscape(s string) string { return slackEscaper.Replace(s) }

// truncate cuts s to at most maxRunes characters, ending a cut with "…". It
// counts runes, the unit Slack's limits use, and never splits a UTF-8 sequence.
func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	r := []rune(s)
	if maxRunes <= 1 {
		return string(r[:maxRunes])
	}
	return string(r[:maxRunes-1]) + "…"
}

// directionColor maps a Direction to a hex for email accents (neutral gray when unset).
func directionColor(d pb.Direction) string {
	switch d {
	case pb.Direction_DIRECTION_POSITIVE:
		return "#15803D"
	case pb.Direction_DIRECTION_NEGATIVE:
		return "#dc2626"
	default:
		return "#9ca3af"
	}
}

// directionMarker maps a Direction to a Slack-safe leading marker.
func directionMarker(d pb.Direction) string {
	switch d {
	case pb.Direction_DIRECTION_POSITIVE:
		return "🟢"
	case pb.Direction_DIRECTION_NEGATIVE:
		return "🔴"
	default:
		return "•"
	}
}

// slackFallback is the plain-text fallback Slack shows in notifications: the
// subject, else the first heading or paragraph, else empty (the client then
// derives one from the blocks).
func slackFallback(c *pb.RenderableContent) string {
	if c.GetSubject() != "" {
		return c.GetSubject()
	}
	for _, blk := range c.GetBlocks() {
		switch v := blk.GetBlock().(type) {
		case *pb.ContentBlock_Heading:
			return v.Heading.GetText()
		case *pb.ContentBlock_Paragraph:
			return v.Paragraph.GetText()
		case *pb.ContentBlock_Card:
			return v.Card.GetHeadline()
		default:
		}
	}
	return ""
}
