// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package metrics holds the service's Prometheus metrics. No label carries an
// organization, so the series count does not grow with the customer count. The
// labels usecase, channel, to_status and reason_class take values from the
// usecase registry and the enums. monitoring/ holds the alert rules and the
// dashboard that read them. Importing the package registers nothing: Register
// adds the metrics to the registry the caller names.
package metrics

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// Dead reason classes. The alert separates a bad address or channel
// (transient_exhausted) from sending-side incidents. crash_exhausted is a
// GUARANTEED message whose worker stopped mid-send on every attempt.
const (
	ReasonTransientExhausted = "transient_exhausted"
	ReasonPermanentConfig    = "permanent_config"
	ReasonRenderBug          = "render_bug"
	ReasonCrashExhausted     = "crash_exhausted"
)

var (
	messageTransitionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_message_transitions_total",
		Help: "Message status transitions, by usecase, channel, and target status.",
	}, []string{"usecase", "channel", "to_status"})

	dispatchDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "outbox_comms_dispatch_duration_seconds",
		Help:    "Per-provider-call dispatch duration, by channel.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms → ~40s
	}, []string{"channel"})

	deliveryLatencySeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "outbox_comms_delivery_latency_seconds",
		Help:    "Time from message creation (fan-out) to the provider's acceptance (SENT, or DELIVERED for Slack), by channel.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 14), // 1s → ~4.5h
	}, []string{"channel"})

	deadTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_dead_total",
		Help: "Messages terminated DEAD, by reason class (transient_exhausted|permanent_config|render_bug).",
	}, []string{"reason_class"})

	unconfirmedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_unconfirmed_total",
		Help: "Messages terminated UNCONFIRMED, by reason.",
	}, []string{"reason"})

	orphanFeedbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_comms_orphan_feedback_total",
		Help: "Email feedback events dropped after their last redelivery with no matching dispatched message.",
	})

	feedbackDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_feedback_dropped_total",
		Help: "Email feedback dropped on arrival because it could not be read, by reason: malformed_json or missing_message_id.",
	}, []string{"reason"})

	schedulerEnqueueFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_scheduler_enqueue_failures_total",
		Help: "Ticks of a periodic task (reconcile, prune) the scheduler leader could not enqueue, by task type.",
	}, []string{"task"})

	reconcileFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "outbox_comms_reconcile_failures_total",
		Help: "Reconcile runs in which an arm failed, by arm.",
	}, []string{"arm"})

	overduePending = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_comms_overdue_pending",
		Help: "FAILED rows past next_retry_at by more than OUTBOX_COMMS_OVERDUE_AFTER (5m by default), set by each reconcile run.",
	})

	bounceRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_comms_bounce_rate",
		Help: "Hard bounces / email sends over the reputation window (24h by default), set by each reconcile run; 0 below the minimum sample.",
	})

	complaintRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_comms_complaint_rate",
		Help: "Complaints / email sends over the reputation window (24h by default), set by each reconcile run; 0 below the minimum sample.",
	})

	reputationSampleSends = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_comms_reputation_sample_sends",
		Help: "Email sends over the reputation window, the sample the bounce and complaint rates rest on.",
	})
)

// all is every metric this package records.
func all() []prometheus.Collector {
	return []prometheus.Collector{
		messageTransitionsTotal,
		dispatchDurationSeconds,
		deliveryLatencySeconds,
		deadTotal,
		unconfirmedTotal,
		orphanFeedbackTotal,
		feedbackDroppedTotal,
		schedulerEnqueueFailuresTotal,
		reconcileFailuresTotal,
		overduePending,
		bounceRate,
		complaintRate,
		reputationSampleSends,
	}
}

// Register adds every metric to reg. A metric reg already holds is not an
// error, so a process that runs the service twice registers once.
func Register(reg prometheus.Registerer) error {
	for _, c := range all() {
		if err := reg.Register(c); err != nil {
			var dup prometheus.AlreadyRegisteredError
			if errors.As(err, &dup) && dup.ExistingCollector == c {
				continue
			}
			return fmt.Errorf("register metrics: %w", err)
		}
	}
	return nil
}

// RecordTransition counts a message status transition.
func RecordTransition(usecase string, channel pb.Channel, to pb.MessageStatus) {
	messageTransitionsTotal.WithLabelValues(usecase, channel.String(), to.String()).Inc()
}

// RecordDispatchDuration records a provider-call duration in seconds.
func RecordDispatchDuration(channel pb.Channel, seconds float64) {
	dispatchDurationSeconds.WithLabelValues(channel.String()).Observe(seconds)
}

// RecordDeliveryLatency records the creation-to-acceptance latency in seconds.
func RecordDeliveryLatency(channel pb.Channel, seconds float64) {
	deliveryLatencySeconds.WithLabelValues(channel.String()).Observe(seconds)
}

// RecordDead counts a DEAD termination by reason class.
func RecordDead(reasonClass string) { deadTotal.WithLabelValues(reasonClass).Inc() }

// RecordUnconfirmed counts an UNCONFIRMED termination by reason.
func RecordUnconfirmed(reason string) { unconfirmedTotal.WithLabelValues(reason).Inc() }

// RecordOrphanFeedback counts a feedback event that matched no message.
func RecordOrphanFeedback() { orphanFeedbackTotal.Inc() }

// Reasons a feedback notification is dropped on arrival.
const (
	FeedbackDroppedMalformedJSON    = "malformed_json"
	FeedbackDroppedMissingMessageID = "missing_message_id"
)

// RecordFeedbackDropped counts a feedback notification dropped because it could
// not be read.
func RecordFeedbackDropped(reason string) { feedbackDroppedTotal.WithLabelValues(reason).Inc() }

// RecordSchedulerEnqueueFailure counts a periodic task tick that was not
// enqueued.
func RecordSchedulerEnqueueFailure(task string) {
	schedulerEnqueueFailuresTotal.WithLabelValues(task).Inc()
}

// RecordReconcileFailure counts a reconcile run in which one arm failed.
func RecordReconcileFailure(arm string) { reconcileFailuresTotal.WithLabelValues(arm).Inc() }

// SetOverduePending sets the overdue-pending gauge (reconcile observes it).
func SetOverduePending(n float64) { overduePending.Set(n) }

// SetReputationRates sets the rolling bounce/complaint reputation gauges.
func SetReputationRates(bounce, complaint float64) {
	bounceRate.Set(bounce)
	complaintRate.Set(complaint)
}

// SetReputationSample sets the number of email sends the reputation rates rest on.
func SetReputationSample(sends float64) { reputationSampleSends.Set(sends) }
