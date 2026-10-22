// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package fanout turns a stored request into one message row per recipient.
// It looks up the channel for each configured-destination entry, applies the
// preference gate, and enqueues each QUEUED row for dispatch. It never chooses
// an audience: the request names every recipient or organization.
package fanout

import (
	"context"
	"errors"
	"fmt"

	"log/slog"

	"github.com/tetriz-ai/outbox-comms/gate"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/metrics"
	"github.com/tetriz-ai/outbox-comms/internal/queue"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// skip reasons (comms_messages.skip_reason).
const (
	skipNoDestination = "no_destination"
	skipOptedOut      = "opted_out"
	skipOrgFrozen     = "org_frozen"
)

// DispatchPublisher enqueues a QUEUED message id onto the dispatch queue.
type DispatchPublisher interface {
	PublishDispatch(ctx context.Context, orgID, messageID string) error
}

// VendorResolver maps a channel to its backing vendor.
type VendorResolver interface {
	Resolve(ch pb.Channel) pb.Vendor
}

// StaticVendorResolver binds each channel to one configured vendor.
type StaticVendorResolver struct {
	Email pb.Vendor
	Slack pb.Vendor
}

// Resolve returns the configured vendor for a channel, or UNSPECIFIED.
func (r StaticVendorResolver) Resolve(ch pb.Channel) pb.Vendor {
	switch ch {
	case pb.Channel_CHANNEL_EMAIL:
		return r.Email
	case pb.Channel_CHANNEL_SLACK:
		return r.Slack
	default:
		return pb.Vendor_VENDOR_UNSPECIFIED
	}
}

// FanOut orchestrates one request's fan-out pass.
type FanOut struct {
	requestDAO     dao.IRequestDAO
	messageDAO     dao.IMessageDAO
	preferenceDAO  dao.IPreferenceDAO
	destinationDAO dao.IDestinationDAO
	orgGate        gate.OrgGate
	dispatcher     DispatchPublisher
	vendors        VendorResolver
	logger         *slog.Logger
}

// SetLogger sets where the fan-out logs; nil means slog's default.
func (f *FanOut) SetLogger(l *slog.Logger) { f.logger = l }

// NewFanOut constructs a FanOut.
func NewFanOut(requestDAO dao.IRequestDAO, messageDAO dao.IMessageDAO, preferenceDAO dao.IPreferenceDAO, destinationDAO dao.IDestinationDAO, orgGate gate.OrgGate, dispatcher DispatchPublisher, vendors VendorResolver) *FanOut {
	return &FanOut{requestDAO, messageDAO, preferenceDAO, destinationDAO, orgGate, dispatcher, vendors, nil}
}

// Process runs one bounded fan-out pass for a request: org gate, resolve
// CONFIGURED_DESTINATION entries, gate the batch, materialize message
// rows (ON CONFLICT no-op, crash-safe), re-SELECT QUEUED and enqueue each, then
// mark FANNED_OUT. Idempotent: a re-driven pass no-ops materialized rows and
// re-emits the dispatch triggers (the re-SELECT, not RETURNING).
func (f *FanOut) Process(ctx context.Context, orgID, requestID string) error {
	req, err := f.requestDAO.GetByID(ctx, orgID, requestID)
	if errors.Is(err, dao.ErrNotFound) {
		// A trigger for a row that does not exist can never succeed.
		return queue.Permanent(fmt.Errorf("fanout: no request %s for org %s: %w", requestID, orgID, err))
	}
	if err != nil {
		return fmt.Errorf("fanout: load request: %w", err)
	}
	if err := f.requestDAO.UpdateStatus(ctx, orgID, requestID, pb.RequestStatus_REQUEST_STATUS_FANNING_OUT); err != nil {
		return fmt.Errorf("fanout: mark fanning_out: %w", err)
	}

	active, frozen, err := f.orgGate.CheckOrg(ctx, orgID)
	if err != nil {
		return fmt.Errorf("fanout: org gate: %w", err)
	}
	if !active {
		// The org was purged or never existed: nothing is sent.
		if err := f.requestDAO.UpdateStatus(ctx, orgID, requestID, pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED); err != nil {
			return fmt.Errorf("fanout: mark fanout_failed: %w", err)
		}
		f.log().WarnContext(ctx, "fanout skipped: org inactive", "request_id", requestID)
		return nil
	}

	contract, err := templates.Get(req.Usecase)
	if err != nil {
		// The usecase was registered at submit and is gone now: no retry brings it
		// back, so the request fails visibly instead of looping through reconcile.
		if uerr := f.requestDAO.UpdateStatus(ctx, orgID, requestID, pb.RequestStatus_REQUEST_STATUS_FANOUT_FAILED); uerr != nil {
			return fmt.Errorf("fanout: mark fanout_failed: %w", uerr)
		}
		return queue.Permanent(fmt.Errorf("fanout: contract: %w", err))
	}

	transactional := contract.Category == pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL
	var msgs []*model.CommsMessage
	for _, e := range req.Entries.GetEntries() {
		var rows []*model.CommsMessage
		if frozen {
			// A frozen org sends nothing, and says so on every entry: each becomes a
			// SKIPPED(org_frozen) row, so the request rolls up PARTIAL with a reason.
			rows = []*model.CommsMessage{f.skipRow(orgID, requestID, req, contract, e, f.vendors.Resolve(e.GetChannel()), entryRecipient(e), skipOrgFrozen)}
		} else {
			rows, err = f.materializeEntry(ctx, orgID, requestID, req, contract, transactional, e)
			if err != nil {
				return fmt.Errorf("fanout: materialize entry: %w", err)
			}
		}
		msgs = append(msgs, rows...)
	}

	inserted, err := f.messageDAO.MaterializeBatch(ctx, orgID, msgs)
	if err != nil {
		return fmt.Errorf("fanout: materialize: %w", err)
	}
	// Only a row this pass inserted is counted, so a re-driven fan-out does not
	// count its skips again.
	for _, m := range inserted {
		if m.Status == pb.MessageStatus_MESSAGE_STATUS_SKIPPED {
			metrics.RecordTransition(req.Usecase, m.Channel, pb.MessageStatus_MESSAGE_STATUS_SKIPPED)
		}
	}

	// Re-SELECT the QUEUED rows so a re-driven pass re-emits triggers (a conflict
	// would make RETURNING empty), then enqueue each for dispatch.
	queued, err := f.messageDAO.ListQueuedForRequest(ctx, orgID, requestID)
	if err != nil {
		return fmt.Errorf("fanout: list queued: %w", err)
	}
	for _, m := range queued {
		if err := f.dispatcher.PublishDispatch(ctx, orgID, m.ID); err != nil {
			// Non-fatal: the reconcile aged-QUEUED arm re-enqueues the row.
			f.log().ErrorContext(ctx, "fanout: dispatch enqueue failed; reconcile will re-drive",
				"message_id", m.ID, "error", err)
		}
	}

	// Mark FANNED_OUT only after the enqueues complete, so a crash leaves the
	// request FANNING_OUT for the reconcile stuck-request arm to re-drive.
	if err := f.requestDAO.UpdateStatus(ctx, orgID, requestID, pb.RequestStatus_REQUEST_STATUS_FANNED_OUT); err != nil {
		return fmt.Errorf("fanout: mark fanned_out: %w", err)
	}
	return nil
}

// materializeEntry turns one entry into its message row: a CONFIGURED_DESTINATION
// resolves the scope it names to its one configured ref (or, with none, one
// SKIPPED(no_destination) row; a failed lookup is returned, never a skip); a
// concrete entry is one row. Each row is QUEUED unless the gate rejects it (→ SKIPPED).
func (f *FanOut) materializeEntry(ctx context.Context, orgID, requestID string, req *model.CommsRequest, contract templates.Contract, transactional bool, e *pb.CommsEntry) ([]*model.CommsMessage, error) {
	vendor := f.vendors.Resolve(e.GetChannel())
	cat := contract.Category

	if e.GetTargetKind() == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION {
		ds := e.GetDestinationScope()
		refs, err := f.destinationDAO.Resolve(ctx, orgID, ds.GetEntityType(), ds.GetEntityId(), req.Usecase, e.GetChannel())
		if err != nil {
			return nil, fmt.Errorf("fanout: resolve destination: %w", err)
		}
		if len(refs) == 0 {
			// No configured destination: a deliberate SKIPPED row, not a failure.
			// recipient is the scope marker so uq_comms_messages_dedup stays unique.
			return []*model.CommsMessage{f.skipRow(orgID, requestID, req, contract, e, vendor, entryRecipient(e), skipNoDestination)}, nil
		}
		// PERSON-scoped destinations gate on that person; ORG-scoped at ORG tier.
		personID := ""
		if ds.GetEntityType() == pb.EntityType_ENTITY_TYPE_PERSON {
			personID = ds.GetEntityId()
		}
		var rows []*model.CommsMessage
		for _, ref := range refs {
			row, err := f.gateRow(ctx, orgID, requestID, req, contract, transactional, cat, personID, e, vendor, ref)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		return rows, nil
	}

	// Concrete entry: gated on its subject person (PERSON over ORG) when the
	// producer names one, else on the ORG tier.
	row, err := f.gateRow(ctx, orgID, requestID, req, contract, transactional, cat, e.GetSubjectPersonId(), e, vendor, e.GetRecipient())
	if err != nil {
		return nil, err
	}
	return []*model.CommsMessage{row}, nil
}

// entryRecipient is the recipient column for an entry's row when no concrete
// recipient is resolved: the entry's own recipient, or a scope marker for a
// CONFIGURED_DESTINATION entry so uq_comms_messages_dedup stays unique.
func entryRecipient(e *pb.CommsEntry) string {
	if e.GetTargetKind() == pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION {
		return "cfgdest:" + e.GetDestinationScope().GetEntityId()
	}
	return e.GetRecipient()
}

// gateRow builds one message row, applying the preference gate for a
// non-transactional usecase (a rejection → SKIPPED(opted_out); transactional
// bypasses). A failed preference lookup is returned, so the whole pass retries:
// a database blip must not turn into a permanent opt-out. Suppression is not
// gated here: it is a dispatch-time check.
func (f *FanOut) gateRow(ctx context.Context, orgID, requestID string, req *model.CommsRequest, contract templates.Contract, transactional bool, cat pb.MessageCategory, personID string, e *pb.CommsEntry, vendor pb.Vendor, recipient string) (*model.CommsMessage, error) {
	if !transactional {
		enabled, err := f.preferenceDAO.IsEnabled(ctx, orgID, personID, cat, e.GetChannel())
		if err != nil {
			return nil, fmt.Errorf("fanout: preference gate: %w", err)
		}
		if !enabled {
			return f.skipRow(orgID, requestID, req, contract, e, vendor, recipient, skipOptedOut), nil
		}
	}
	m := f.baseRow(orgID, requestID, req, contract, e, vendor, recipient)
	m.Status = pb.MessageStatus_MESSAGE_STATUS_QUEUED
	return m, nil
}

func (f *FanOut) skipRow(orgID, requestID string, req *model.CommsRequest, contract templates.Contract, e *pb.CommsEntry, vendor pb.Vendor, recipient, reason string) *model.CommsMessage {
	m := f.baseRow(orgID, requestID, req, contract, e, vendor, recipient)
	m.Status = pb.MessageStatus_MESSAGE_STATUS_SKIPPED
	m.SkipReason = &reason
	return m
}

// baseRow builds the common message row. contract is the caller's already-resolved
// contract (Process validates the usecase before any row is built), so it is
// threaded in rather than re-fetched.
func (f *FanOut) baseRow(orgID, requestID string, req *model.CommsRequest, contract templates.Contract, e *pb.CommsEntry, vendor pb.Vendor, recipient string) *model.CommsMessage {
	return &model.CommsMessage{
		OrganizationID:    orgID,
		RequestID:         requestID,
		Channel:           e.GetChannel(),
		Vendor:            vendor,
		Recipient:         recipient,
		SlackDelivery:     e.GetSlackDelivery(),
		RecipientContext:  e.GetVars(),
		Content:           req.RenderableContentForEntry(e),
		VerificationLevel: contract.DefaultVerification,
		DeliveryGuarantee: req.DeliveryGuarantee,
	}
}

func (f *FanOut) log() *slog.Logger {
	if f.logger != nil {
		return f.logger
	}
	return slog.Default()
}
