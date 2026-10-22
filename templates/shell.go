// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"fmt"
	"html"
	"net/url"
	"strings"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// shell.go is the shared email chrome: every outgoing email, whatever its
// usecase or render path, is wrapped in the same document (header, white card on
// a neutral backdrop, accent-coloured footer with the legal links), so no
// producer can ship a bare, unstyled body.

// accentColor is the button and footer colour every element in this package
// shares.
const accentColor = "#2563EB"

// EmailChromeOptions carries the per-send values WrapEmail needs. Now is the
// dispatcher's clock (a deterministic copyright year); a zero Now falls back to
// time.Now(). Every other field is optional and degrades on its own: no LogoURL
// renders BrandName as a text wordmark, no BrandName drops the wordmark and the
// copyright line, and an empty or non-http(s) URL drops its link.
type EmailChromeOptions struct {
	Now time.Time
	// BrandName is the sender's name in the header wordmark and the copyright line.
	BrandName string
	// SiteURL is where the header logo links.
	SiteURL string
	// LogoURL is an absolute http(s) URL of the header logo image.
	LogoURL string
	// PreferencesURL is the manage-notification-preferences page linked from
	// non-transactional footers.
	PreferencesURL string
	// PrivacyURL and TermsURL are the legal links in the footer.
	PrivacyURL string
	TermsURL   string
}

// WrapEmail wraps a rendered email payload in the shared chrome and appends the
// category-appropriate footer notice: transactional emails carry the safety
// line (unexpected email → safe to ignore); everything else states why the
// recipient got it, with a manage-preferences link when PreferencesURL is
// non-empty. Payloads without an email body (Slack) pass through unchanged. Call
// it once per render: a second call nests a second document and repeats the
// footer.
func WrapEmail(p *pb.RenderedPayload, category pb.MessageCategory, opts EmailChromeOptions) *pb.RenderedPayload {
	if p.GetEmailHtml() == "" {
		return p
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	footerHTML, footerText := footerLines(category, opts, now.Year())

	var b strings.Builder
	b.Grow(2048 + len(p.GetEmailHtml()) + len(footerHTML))
	b.WriteString(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">`)
	// mso rules target Outlook desktop (Word rendering engine — no @media support);
	// the max-width media query is for clients that do support it (Apple/Gmail/mobile
	// webmail), collapsing the 40px side padding to keep body copy readable on a
	// ~360px phone viewport.
	b.WriteString(`<style>body,table,td,a{-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;}table,td{mso-table-lspace:0pt;mso-table-rspace:0pt;}img{-ms-interpolation-mode:bicubic;}@media only screen and (max-width:600px){.oc-pad{padding-left:24px !important;padding-right:24px !important;}.oc-btn{display:block !important;width:100% !important;text-align:center !important;box-sizing:border-box !important;}}</style>`)
	b.WriteString(`</head>`)
	b.WriteString(`<body style="margin:0;padding:0;font-family:sans-serif;background:#F9F9F9;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#F9F9F9;padding:40px 20px;"><tr><td align="center">`)
	b.WriteString(`<!--[if mso]><table role="presentation" align="center" width="560" cellpadding="0" cellspacing="0" border="0"><tr><td><![endif]-->`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;background:#ffffff;border-radius:10px;box-sizing:border-box;text-align:left;overflow:hidden;">`)
	if logo := logoHTML(opts); logo != "" {
		b.WriteString(`<tr><td class="oc-pad" style="padding:24px 40px;border-bottom:1px solid #EDEDED;">`)
		b.WriteString(logo)
		b.WriteString(`</td></tr>`)
	}
	fmt.Fprintf(&b, `<tr><td class="oc-pad" style="padding:28px 40px 8px;font-size:15px;color:#404040;line-height:1.6;word-break:break-word;">%s</td></tr>`, p.GetEmailHtml())
	b.WriteString(`<tr><td style="height:12px;line-height:12px;font-size:0;">&nbsp;</td></tr>`)
	fmt.Fprintf(&b, `<tr><td class="oc-pad" style="background:%s;padding:24px 40px;">%s</td></tr>`, accentColor, footerHTML)
	b.WriteString(`</table>`)
	b.WriteString(`<!--[if mso]></td></tr></table><![endif]-->`)
	b.WriteString(`</td></tr></table></body></html>`)

	text := p.GetEmailText()
	if text != "" {
		text = strings.TrimRight(text, "\n") + "\n\n" + footerText + "\n"
	}

	return &pb.RenderedPayload{
		EmailSubject:    p.GetEmailSubject(),
		EmailHtml:       b.String(),
		EmailText:       text,
		SlackBlocksJson: p.GetSlackBlocksJson(),
	}
}

// logoHTML renders the header: the logo image linked to the site when LogoURL is
// a safe absolute URL, else the brand name as a text wordmark, else nothing. A
// relative image src would render broken in every mail client, so it is never
// emitted.
func logoHTML(opts EmailChromeOptions) string {
	name := html.EscapeString(opts.BrandName)
	if src, ok := safeURL(opts.LogoURL); ok {
		alt := name
		if alt == "" {
			alt = "Logo"
		}
		img := fmt.Sprintf(`<img src="%s" width="140" alt="%s" style="display:block;border:0;outline:none;max-width:140px;height:auto;">`, src, alt)
		if href, ok := safeURL(opts.SiteURL); ok {
			return fmt.Sprintf(`<a href="%s" target="_blank" rel="noopener" style="text-decoration:none;">%s</a>`, href, img)
		}
		return img
	}
	if name == "" {
		return ""
	}
	return fmt.Sprintf(`<span style="font-size:20px;font-weight:800;color:%s;">%s</span>`, accentColor, name)
}

// footerLines returns the footer band in both formats: a copyright line with the
// legal links, then the category's notice. Transactional mail covers the
// unexpected-recipient case; everything else says why the recipient got it and
// links their preferences.
func footerLines(category pb.MessageCategory, opts EmailChromeOptions, year int) (htmlBlock, textBlock string) {
	var legalHTML, legalText []string
	if opts.BrandName != "" {
		legalHTML = append(legalHTML, fmt.Sprintf("&copy; %d %s. All rights reserved.", year, html.EscapeString(opts.BrandName)))
		legalText = append(legalText, fmt.Sprintf("© %d %s. All rights reserved.", year, opts.BrandName))
	}
	for _, l := range []struct{ label, url string }{{"Privacy Policy", opts.PrivacyURL}, {"Terms of Use", opts.TermsURL}} {
		if href, ok := safeURL(l.url); ok {
			legalHTML = append(legalHTML, fmt.Sprintf(`<a href="%s" style="color:#ffffff;text-decoration:underline;">%s</a>`, href, l.label))
			legalText = append(legalText, l.label+": "+l.url)
		}
	}

	var noticeHTML, noticeText string
	if category == pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL {
		const line = "If you weren't expecting this email, you can safely ignore it."
		noticeHTML, noticeText = line, line
	} else {
		line := "You're receiving this because email notifications are enabled for you."
		if opts.BrandName != "" {
			line = "You're receiving this because email notifications from " + opts.BrandName + " are enabled for you."
		}
		noticeHTML, noticeText = html.EscapeString(line), line
		if safeHref, ok := safeURL(opts.PreferencesURL); ok {
			noticeHTML = fmt.Sprintf(`%s <a href="%s" style="color:#ffffff;">Manage notification preferences</a>`, noticeHTML, safeHref)
			noticeText = fmt.Sprintf("%s Manage notification preferences: %s", line, opts.PreferencesURL)
		}
	}

	if len(legalHTML) > 0 {
		htmlBlock = `<p style="margin:0 0 10px;font-size:12px;color:#ffffff;line-height:1.6;">` + strings.Join(legalHTML, " &nbsp;&middot;&nbsp; ") + `</p>`
		textBlock = strings.Join(legalText, " · ") + "\n"
	}
	htmlBlock += fmt.Sprintf(`<p style="margin:0;font-size:11px;color:#eef2ff;line-height:1.6;">%s</p>`, noticeHTML)
	textBlock += noticeText
	return htmlBlock, textBlock
}

// buttonHTML renders the shared CTA button from an href that safeURL already
// validated and escaped. label is escaped here: pass it raw.
func buttonHTML(href, label string) string {
	return fmt.Sprintf(
		`<p style="margin:20px 0 4px;"><a href="%s" class="oc-btn" style="display:inline-block;padding:14px 28px;background:%s;color:#ffffff;border-radius:8px;text-decoration:none;font-weight:600;font-size:14px;">%s</a></p>`,
		href, accentColor, html.EscapeString(label),
	)
}

// safeURL validates rawURL is an absolute http(s) link before it is allowed
// into an href — html.EscapeString alone neutralizes markup/attribute breakout
// but does nothing to stop a javascript:/data: URI from executing on click.
// Returns the HTML-escaped, safe-to-emit href and true, or ("", false) for
// anything else (empty, unparseable, or a non-http(s) scheme).
func safeURL(rawURL string) (string, bool) {
	if rawURL == "" {
		return "", false
	}
	// A dropped link is left out of the output and not logged: the package
	// writes to no logger, and a link can carry a token in its query string.
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return "", false
	}
	return html.EscapeString(rawURL), true
}
