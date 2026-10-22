// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"strings"
	"testing"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// TestPreviewGateComparesTheStoredContent pins that send checks the content the
// preview stored, not only its label: a preview row whose label matches but
// whose content differs is refused, and the real preview is accepted.
func TestPreviewGateComparesTheStoredContent(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	content := &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Maintenance tonight",
		Blocks: []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "From 22:00 UTC."}}}}}}
	c := &sendCmd{org: "11111111-1111-1111-1111-111111111111", usecase: "notice", content: content,
		recipients: []recipient{{email: "jane@example.com"}}, previewID: "p1", now: func() time.Time { return now }}
	contentFP, err := contentFingerprint(content)
	if err != nil {
		t.Fatal(err)
	}
	label := previewAnalytics("notice", 1, "audience-fp", contentFP, noAttachmentFP)
	preview := func(stored *pb.MessageContent) *model.CommsRequest {
		return &model.CommsRequest{OrganizationID: c.org, Usecase: "notice", IdempotencyKey: previewKeyPrefix + "x",
			CreatedAt: now.Add(-time.Hour), AnalyticsID: &label, Content: stored}
	}
	if err := c.checkPreview(preview(withPreviewBanner(content, 1)), "audience-fp", noAttachmentFP, false); err != nil {
		t.Fatalf("the real preview: %v", err)
	}
	other := &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Something else"}}
	if err := c.checkPreview(preview(withPreviewBanner(other, 1)), "audience-fp", noAttachmentFP, false); err == nil || !strings.Contains(err.Error(), "content differs") {
		t.Fatalf("a matching label with other content: %v, want refused", err)
	}
}

// TestPreviewGateUsecaseAndAge pins two more refusals: a preview of another
// usecase whose label was forged to match, and a preview past 24 hours on a
// fresh send. A preview exactly 24 hours old is still accepted.
func TestPreviewGateUsecaseAndAge(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	content := &pb.MessageContent{Base: &pb.RenderableContent{Subject: "Maintenance tonight"}}
	c := &sendCmd{org: "11111111-1111-1111-1111-111111111111", usecase: "notice", content: content,
		recipients: []recipient{{email: "jane@example.com"}}, previewID: "p1", now: func() time.Time { return now }}
	contentFP, err := contentFingerprint(content)
	if err != nil {
		t.Fatal(err)
	}
	label := previewAnalytics("notice", 1, "audience-fp", contentFP, noAttachmentFP)
	preview := func(usecase string, age time.Duration) *model.CommsRequest {
		return &model.CommsRequest{OrganizationID: c.org, Usecase: usecase, IdempotencyKey: previewKeyPrefix + "x",
			CreatedAt: now.Add(-age), AnalyticsID: &label, Content: withPreviewBanner(content, 1)}
	}
	for _, tc := range []struct {
		name     string
		usecase  string
		age      time.Duration
		resuming bool
		wantErr  string
	}{
		{"another usecase, forged label", "alert", time.Hour, false, "is usecase alert"},
		{"exactly 24 hours old", "notice", previewTTL, false, ""},
		{"24 hours and a nanosecond", "notice", previewTTL + time.Nanosecond, false, "older than"},
		{"past 24 hours, resuming", "notice", 48 * time.Hour, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := c.checkPreview(preview(tc.usecase, tc.age), "audience-fp", noAttachmentFP, tc.resuming)
			if (tc.wantErr == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("checkPreview: %v, want %q", err, tc.wantErr)
			}
		})
	}
}
