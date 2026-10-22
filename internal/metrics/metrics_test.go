// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestRecordTransition proves the transition counter increments with the bounded
// (usecase, channel, to_status) labels — never an org label.
func TestRecordTransition(t *testing.T) {
	messageTransitionsTotal.Reset()
	RecordTransition(
		templates.UsecaseNotice,
		pb.Channel_CHANNEL_EMAIL,
		pb.MessageStatus_MESSAGE_STATUS_SENT,
	)
	const want = `
		# HELP outbox_comms_message_transitions_total Message status transitions, by usecase, channel, and target status.
		# TYPE outbox_comms_message_transitions_total counter
		outbox_comms_message_transitions_total{channel="CHANNEL_EMAIL",to_status="MESSAGE_STATUS_SENT",usecase="notice"} 1
	`
	if err := testutil.CollectAndCompare(messageTransitionsTotal, strings.NewReader(want), "outbox_comms_message_transitions_total"); err != nil {
		t.Fatalf("transition metric mismatch: %v", err)
	}
}

// TestRecordDead proves the DEAD counter increments by reason class.
func TestRecordDead(t *testing.T) {
	before := testutil.ToFloat64(deadTotal.WithLabelValues(ReasonRenderBug))
	RecordDead(ReasonRenderBug)
	after := testutil.ToFloat64(deadTotal.WithLabelValues(ReasonRenderBug))
	if after != before+1 {
		t.Fatalf("dead_total[render_bug] = %v, want %v", after, before+1)
	}
}
