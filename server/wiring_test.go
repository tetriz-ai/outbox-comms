// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/tetriz-ai/outbox-comms/config"
)

// TestSESClientUsesTheSendTimeout pins that the configured send deadline, even
// one above the 60-second default, is the SES client's HTTP timeout, so the
// client never cuts a call the dispatcher still allows.
func TestSESClientUsesTheSendTimeout(t *testing.T) {
	for _, d := range []time.Duration{90 * time.Second, 10 * time.Minute, 5 * time.Second} {
		c := sesClient(aws.Config{Region: "us-east-1"}, config.Config{SendTimeout: d})
		hc, ok := c.Options().HTTPClient.(interface{ GetTimeout() time.Duration })
		if !ok {
			t.Fatalf("the SES HTTP client %T reports no timeout", c.Options().HTTPClient)
		}
		if got := hc.GetTimeout(); got != d {
			t.Fatalf("send timeout %v: SES HTTP timeout %v, want %v", d, got, d)
		}
	}
}

// TestFeedbackConsumerHonoursTheTombstoneTTL pins that the feedback consumer the
// server builds matches purge tombstones only within
// OUTBOX_COMMS_PURGE_TOMBSTONE_TTL.
func TestFeedbackConsumerHonoursTheTombstoneTTL(t *testing.T) {
	fb := feedbackConsumer(config.Config{PurgeTombstoneTTL: 72 * time.Hour}, nil, nil, nil, nil)
	if fb.TombstoneTTL != 72*time.Hour {
		t.Fatalf("tombstone TTL %v, want 72h", fb.TombstoneTTL)
	}
}

// TestShutdownOutlastsTheSendDeadline pins that a stop waits longer than one
// send may take, at any configured send deadline.
func TestShutdownOutlastsTheSendDeadline(t *testing.T) {
	for _, d := range []time.Duration{0, 5 * time.Second, 60 * time.Second, 10 * time.Minute} {
		want := d
		if want == 0 {
			want = 60 * time.Second
		}
		if got := shutdownTimeout(d); got <= want {
			t.Errorf("send deadline %v: shutdown waits %v, want more than %v", d, got, want)
		}
	}
}

// TestSchedulerConfigOutlastsTheSendDeadline pins the wiring, not only the
// helper: the queue worker's shutdown wait is longer than the send deadline.
func TestSchedulerConfigOutlastsTheSendDeadline(t *testing.T) {
	for _, d := range []time.Duration{5 * time.Second, 60 * time.Second, 10 * time.Minute} {
		got := schedulerConfig(config.Config{SendTimeout: d}, nil)
		if got.ShutdownTimeout <= d {
			t.Errorf("send deadline %v: the worker waits %v on shutdown, want more", d, got.ShutdownTimeout)
		}
	}
}
