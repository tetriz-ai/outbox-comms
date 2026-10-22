// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package feedback consumes Amazon SES delivery, bounce and complaint
// notifications from SQS, raw or inside an SNS envelope. It finds the message
// by its provider id, records the event, moves the status on, and suppresses
// the address after a hard bounce or a complaint. An event for a message this
// service never sent writes nothing.
package feedback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/metrics"
	"github.com/tetriz-ai/outbox-comms/internal/queue"
)

// ErrNoMatch signals that no dispatched message matched the feedback event: the
// race where feedback arrives before the send recorded its provider id, or a
// true orphan. Handle retries it until DefaultMaxReceives, then counts and drops
// it.
var ErrNoMatch = errors.New("feedback: no matching dispatched message")

// DefaultMaxReceives is how many deliveries of an unmatched event Handle allows
// before it counts the event as an orphan and drops it.
const DefaultMaxReceives = 5

// sesNotification models the fields the consumer needs from an SES feedback
// event. Configuration-set event publishing carries the discriminator in
// `eventType`; the older identity-notification shape uses `notificationType`.
// eventKind() coalesces the two, so either wiring works: the values
// (Delivery/Bounce/Complaint) and the sub-objects are the same in both.
type sesNotification struct {
	EventType        string `json:"eventType"`        // config-set event publishing
	NotificationType string `json:"notificationType"` // legacy identity notifications
	Mail             struct {
		MessageID string    `json:"messageId"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"mail"`
	Delivery *struct {
		Timestamp time.Time `json:"timestamp"`
	} `json:"delivery,omitempty"`
	Bounce *struct {
		BounceType string    `json:"bounceType"` // Permanent | Transient | Undetermined
		FeedbackID string    `json:"feedbackId"`
		Timestamp  time.Time `json:"timestamp"`
	} `json:"bounce,omitempty"`
	Complaint *struct {
		FeedbackID string    `json:"feedbackId"`
		Timestamp  time.Time `json:"timestamp"`
	} `json:"complaint,omitempty"`
}

// snsEnvelope is the wrapper SNS puts around a message when the SQS subscription
// does not use raw message delivery.
type snsEnvelope struct {
	Type    string `json:"Type"`
	Message string `json:"Message"`
}

// Consumer applies SES feedback events.
type Consumer struct {
	messageDAO     dao.IMessageDAO
	eventDAO       dao.IDeliveryEventDAO
	suppressionDAO dao.ISuppressionDAO
	now            func() time.Time
	// MaxReceives is the delivery count at which Handle drops an unmatched event
	// as an orphan. Zero means DefaultMaxReceives.
	MaxReceives int
	// Requests names a bounced message's usecase in the transition metric. Nil
	// records the usecase as "unknown".
	Requests dao.IRequestDAO
	// Logger receives the consumer's log lines. Nil means slog's default.
	Logger *slog.Logger
	// TombstoneTTL is how long a purge tombstone matches late feedback
	// (OUTBOX_COMMS_PURGE_TOMBSTONE_TTL). Zero matches a tombstone of any age.
	TombstoneTTL time.Duration
}

// NewConsumer constructs the SES-feedback consumer.
func NewConsumer(messageDAO dao.IMessageDAO, eventDAO dao.IDeliveryEventDAO, suppressionDAO dao.ISuppressionDAO) *Consumer {
	return &Consumer{messageDAO: messageDAO, eventDAO: eventDAO, suppressionDAO: suppressionDAO, now: time.Now}
}

// Handle is the queue entry point: it applies one notification delivered for the
// receiveCount-th time. An unmatched event is retried (ErrNoMatch) until
// receiveCount reaches MaxReceives; then it is counted in
// outbox_comms_orphan_feedback_total and dropped.
func (c *Consumer) Handle(ctx context.Context, raw []byte, receiveCount int) error {
	err := c.ProcessRaw(ctx, raw)
	if !errors.Is(err, ErrNoMatch) {
		return err
	}
	limit := c.MaxReceives
	if limit <= 0 {
		limit = DefaultMaxReceives
	}
	if receiveCount < limit {
		return err
	}
	metrics.RecordOrphanFeedback()
	c.log().WarnContext(ctx, "feedback: dropping an event that matched no message", "receive_count", receiveCount)
	return nil
}

// ProcessRaw parses a raw SES notification, unwrapping an SNS envelope if there
// is one, and applies it. A body that cannot be parsed, or names no message, is
// permanent (queue.Permanent): redelivery cannot fix it. ErrNoMatch is
// retryable. An event type other than Delivery, Bounce or Complaint is
// acknowledged and ignored.
func (c *Consumer) ProcessRaw(ctx context.Context, raw []byte) error {
	var env snsEnvelope
	if json.Unmarshal(raw, &env) == nil && env.Type == "Notification" && env.Message != "" {
		raw = []byte(env.Message)
	}
	var n sesNotification
	if err := json.Unmarshal(raw, &n); err != nil {
		metrics.RecordFeedbackDropped(metrics.FeedbackDroppedMalformedJSON)
		return queue.Permanent(fmt.Errorf("feedback: parse: %w", err))
	}
	if n.Mail.MessageID == "" {
		metrics.RecordFeedbackDropped(metrics.FeedbackDroppedMissingMessageID)
		return queue.Permanent(errors.New("feedback: missing mail.messageId"))
	}
	if err := c.process(ctx, n, raw); err != nil {
		return fmt.Errorf("feedback: process: %w", err)
	}
	return nil
}

// process applies one parsed SES notification under the late-event rules: a
// Delivery advances a SENT or no-feedback-UNCONFIRMED row to DELIVERED
// (forward-only); a permanent Bounce → BOUNCED + suppression; a Complaint →
// suppression + event, status untouched (it was delivered). The event row dedups
// on provider_event_id and keeps the raw notification.
func (c *Consumer) process(ctx context.Context, n sesNotification, raw []byte) error {
	kind := n.eventKind()
	switch kind {
	case "Delivery", "Bounce", "Complaint":
	default:
		c.log().DebugContext(ctx, "feedback: ignoring an event type this service does not track", "event_type", kind)
		return nil
	}
	// Cross-org lookup by provider id. No matching dispatched row: retry, and
	// never a suppression for an address this service never mailed.
	msg, err := c.messageDAO.GetByProviderMessageID(ctx, n.Mail.MessageID)
	if err != nil {
		return fmt.Errorf("lookup message by provider id: %w", err)
	}
	if msg == nil {
		return c.processTombstone(ctx, n, kind)
	}
	org := msg.OrganizationID
	ev := event{org: org, msgID: msg.ID, raw: raw, at: c.occurredAt(n)}

	switch kind {
	case "Delivery":
		err := c.messageDAO.MarkDelivered(ctx, org, msg.ID, "", "")
		if err != nil && !errors.Is(err, dao.ErrStaleTransition) {
			return fmt.Errorf("mark delivered: %w", err)
		}
		if err == nil {
			metrics.RecordTransition(c.usecase(ctx, msg), msg.Channel, pb.MessageStatus_MESSAGE_STATUS_DELIVERED)
		}
		if err := c.appendEvent(ctx, ev, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_DELIVERED, eventID(n.Mail.MessageID, "delivery")); err != nil {
			return fmt.Errorf("append delivery event: %w", err)
		}
		return nil

	case "Bounce":
		eid := n.Mail.MessageID + ":bounce"
		if n.Bounce != nil && n.Bounce.FeedbackID != "" {
			eid = n.Bounce.FeedbackID
		}
		if err := c.appendEvent(ctx, ev, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_BOUNCE, eid); err != nil {
			return fmt.Errorf("append bounce event: %w", err)
		}
		// Only a permanent (hard) bounce auto-suppresses + flips the row BOUNCED;
		// a transient bounce is recorded but not suppressed.
		if n.Bounce != nil && n.Bounce.BounceType == "Permanent" {
			if err := c.suppress(ctx, msg.Recipient, "hard_bounce"); err != nil {
				return fmt.Errorf("suppress hard bounce: %w", err)
			}
			err := c.messageDAO.MarkBounced(ctx, org, msg.ID)
			if err != nil && !errors.Is(err, dao.ErrStaleTransition) {
				return fmt.Errorf("mark bounced: %w", err)
			}
			if err == nil {
				metrics.RecordTransition(c.usecase(ctx, msg), msg.Channel, pb.MessageStatus_MESSAGE_STATUS_BOUNCED)
			}
		}
		return nil

	default: // Complaint
		eid := n.Mail.MessageID + ":complaint"
		if n.Complaint != nil && n.Complaint.FeedbackID != "" {
			eid = n.Complaint.FeedbackID
		}
		if err := c.appendEvent(ctx, ev, pb.DeliveryEventType_DELIVERY_EVENT_TYPE_COMPLAINT, eid); err != nil {
			return fmt.Errorf("append complaint event: %w", err)
		}
		// A complaint always writes a suppression regardless of status; the row
		// status stays (it was delivered; the complaint is an event fact).
		if err := c.suppress(ctx, msg.Recipient, "complaint"); err != nil {
			return fmt.Errorf("suppress complaint: %w", err)
		}
		return nil
	}
}

// processTombstone applies a notification for a message an organization purge
// removed. The purge kept its provider id and address, so a permanent bounce or
// a complaint still suppresses the address, as it would for a live message.
// There is no row to move and no event to record. With no tombstone either,
// the notification is unmatched (ErrNoMatch).
func (c *Consumer) processTombstone(ctx context.Context, n sesNotification, kind string) error {
	var notBefore time.Time
	if c.TombstoneTTL > 0 {
		notBefore = c.now().Add(-c.TombstoneTTL)
	}
	recipient, ok, err := c.suppressionDAO.TombstoneRecipient(ctx, n.Mail.MessageID, notBefore)
	if err != nil {
		return fmt.Errorf("lookup purge tombstone: %w", err)
	}
	if !ok {
		return ErrNoMatch
	}
	switch {
	case kind == "Bounce" && n.Bounce != nil && n.Bounce.BounceType == "Permanent":
		if err := c.suppress(ctx, recipient, "hard_bounce"); err != nil {
			return fmt.Errorf("suppress hard bounce of a purged message: %w", err)
		}
	case kind == "Complaint":
		if err := c.suppress(ctx, recipient, "complaint"); err != nil {
			return fmt.Errorf("suppress complaint about a purged message: %w", err)
		}
	}
	return nil
}

// occurredAt is when the event happened at the provider: the event's own
// timestamp, else the send's, else the time of receipt.
func (c *Consumer) occurredAt(n sesNotification) time.Time {
	switch {
	case n.Delivery != nil && !n.Delivery.Timestamp.IsZero():
		return n.Delivery.Timestamp
	case n.Bounce != nil && !n.Bounce.Timestamp.IsZero():
		return n.Bounce.Timestamp
	case n.Complaint != nil && !n.Complaint.Timestamp.IsZero():
		return n.Complaint.Timestamp
	case !n.Mail.Timestamp.IsZero():
		return n.Mail.Timestamp
	default:
		return c.now()
	}
}

// eventKind returns the feedback event's discriminator, preferring the
// configuration-set event-publishing field (`eventType`) and falling back to
// the identity-notification field (`notificationType`).
func (n sesNotification) eventKind() string {
	if n.EventType != "" {
		return n.EventType
	}
	return n.NotificationType
}

// suppress records an email suppression with the cause that triggered it.
// reason distinguishes a hard bounce from a complaint: the two mean different
// things (deliverability vs. recipient intent), so they are not collapsed into
// one label in comms_suppressions.
func (c *Consumer) suppress(ctx context.Context, email, reason string) error {
	if err := c.suppressionDAO.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: reason}); err != nil {
		return fmt.Errorf("suppress if absent: %w", err)
	}
	return nil
}

type event struct {
	org, msgID string
	raw        []byte
	at         time.Time
}

func (c *Consumer) appendEvent(ctx context.Context, ev event, t pb.DeliveryEventType, providerEventID string) error {
	if err := c.eventDAO.Append(ctx, &model.CommsDeliveryEvent{
		OrganizationID:  ev.org,
		MessageID:       ev.msgID,
		EventType:       t,
		ProviderEventID: &providerEventID,
		ProviderPayload: ev.raw,
		OccurredAt:      ev.at,
	}); err != nil {
		return fmt.Errorf("append delivery event: %w", err)
	}
	return nil
}

func eventID(messageID, kind string) string { return messageID + ":" + kind }

// usecase returns the message's usecase for a metric label, or "unknown".
func (c *Consumer) usecase(ctx context.Context, msg *model.CommsMessage) string {
	if c.Requests == nil {
		return "unknown"
	}
	req, err := c.Requests.GetByID(ctx, msg.OrganizationID, msg.RequestID)
	if err != nil {
		return "unknown"
	}
	return req.Usecase
}

func (c *Consumer) log() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}
