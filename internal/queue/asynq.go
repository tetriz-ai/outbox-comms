// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package queue

import (
	"context"
	"fmt"

	"github.com/hibiken/asynq"
)

// Task types and the asynq queue the triggers ride.
const (
	TaskRequest  = "outbox_comms:request"
	TaskDispatch = "outbox_comms:dispatch"
	AsynqQueue   = "outbox_comms"
	// asynqMaxRetry bounds redelivery of a trigger whose handler keeps failing;
	// the row stays in the database and reconcile picks it up after that.
	asynqMaxRetry = 5
)

// Asynq is the Redis-backed transport.
type Asynq struct {
	client *asynq.Client
}

// NewAsynq builds the publisher over an asynq client.
func NewAsynq(client *asynq.Client) *Asynq { return &Asynq{client: client} }

var _ Publisher = (*Asynq)(nil)

// PublishRequest enqueues a fan-out trigger.
func (a *Asynq) PublishRequest(ctx context.Context, orgID, requestID string) error {
	b, err := encodeRequest(orgID, requestID)
	if err != nil {
		return err
	}
	if _, err := a.client.EnqueueContext(ctx, asynq.NewTask(TaskRequest, b), asynq.Queue(AsynqQueue), asynq.MaxRetry(asynqMaxRetry)); err != nil {
		return fmt.Errorf("publish request trigger (org=%s, req=%s): %w", orgID, requestID, err)
	}
	return nil
}

// PublishDispatch enqueues a dispatch trigger.
func (a *Asynq) PublishDispatch(ctx context.Context, orgID, messageID string) error {
	b, err := encodeDispatch(orgID, messageID)
	if err != nil {
		return err
	}
	if _, err := a.client.EnqueueContext(ctx, asynq.NewTask(TaskDispatch, b), asynq.Queue(AsynqQueue), asynq.MaxRetry(asynqMaxRetry)); err != nil {
		return fmt.Errorf("publish dispatch trigger (org=%s, msg=%s): %w", orgID, messageID, err)
	}
	return nil
}

// RegisterAsynq routes the two trigger task types on mux to h. A permanent
// handler error skips asynq's retries.
func RegisterAsynq(mux *asynq.ServeMux, h Handlers) {
	mux.HandleFunc(TaskRequest, func(ctx context.Context, t *asynq.Task) error {
		trig, err := decodeRequest(t.Payload())
		if err == nil {
			err = h.Request(ctx, trig.GetOrganizationId(), trig.GetRequestId())
		}
		return asynqErr(err)
	})
	mux.HandleFunc(TaskDispatch, func(ctx context.Context, t *asynq.Task) error {
		trig, err := decodeDispatch(t.Payload())
		if err == nil {
			err = h.Dispatch(ctx, trig.GetOrganizationId(), trig.GetMessageId())
		}
		return asynqErr(err)
	})
}

func asynqErr(err error) error {
	if err == nil {
		return nil
	}
	if IsPermanent(err) {
		return fmt.Errorf("%w: %w", err, asynq.SkipRetry)
	}
	return err
}
