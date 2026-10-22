// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package queue carries the pipeline's two internal triggers: a request id to the
// fan-out and a message id to the dispatcher. A trigger is only a pointer; the
// database row is the source of truth, and reconcile re-drives a lost trigger
// from the row. Two transports implement it: asynq over Redis (the default) and
// Amazon SQS.
package queue

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// Publisher enqueues the two triggers.
type Publisher interface {
	PublishRequest(ctx context.Context, orgID, requestID string) error
	PublishDispatch(ctx context.Context, orgID, messageID string) error
}

// Handlers are the consumers a transport delivers triggers to. A handler error is
// retried by the transport; an error wrapped with Permanent is not.
type Handlers struct {
	Request  func(ctx context.Context, orgID, requestID string) error
	Dispatch func(ctx context.Context, orgID, messageID string) error
}

// errPermanent marks an error redelivery cannot fix.
var errPermanent = errors.New("permanent")

// Permanent wraps err so the transport drops the trigger instead of retrying it.
func Permanent(err error) error { return fmt.Errorf("%w: %w", errPermanent, err) }

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool { return errors.Is(err, errPermanent) }

func encodeRequest(orgID, requestID string) ([]byte, error) {
	b, err := protojson.Marshal(&pb.CommsRequestTrigger{OrganizationId: orgID, RequestId: requestID})
	if err != nil {
		return nil, fmt.Errorf("encode request trigger: %w", err)
	}
	return b, nil
}

func encodeDispatch(orgID, messageID string) ([]byte, error) {
	b, err := protojson.Marshal(&pb.CommsDispatchTrigger{OrganizationId: orgID, MessageId: messageID})
	if err != nil {
		return nil, fmt.Errorf("encode dispatch trigger: %w", err)
	}
	return b, nil
}

// decodeRequest decodes a request trigger. A body that does not decode is
// permanent: redelivering it cannot help.
func decodeRequest(b []byte) (*pb.CommsRequestTrigger, error) {
	t := &pb.CommsRequestTrigger{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, t); err != nil {
		return nil, Permanent(fmt.Errorf("decode request trigger: %w", err))
	}
	return t, nil
}

func decodeDispatch(b []byte) (*pb.CommsDispatchTrigger, error) {
	t := &pb.CommsDispatchTrigger{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, t); err != nil {
		return nil, Permanent(fmt.Errorf("decode dispatch trigger: %w", err))
	}
	return t, nil
}
