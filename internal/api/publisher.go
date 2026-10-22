// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api

import "context"

// RequestPublisher enqueues a persisted request's id onto the requests queue so
// the fan-out consumer picks it up.
type RequestPublisher interface {
	// PublishRequest enqueues the request id (the fan-out trigger). The body
	// carries only the id and org: the durable row is the source of truth.
	PublishRequest(ctx context.Context, orgID, requestID string) error
}
