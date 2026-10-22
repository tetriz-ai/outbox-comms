// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package gate holds the two per-organization switches the pipeline consults.
// The defaults allow everything; a deployment that tracks organizations plugs in
// its own implementations. Every orgID a gate receives is a UUID in the
// lowercase hyphenated form (8-4-4-4-12), whatever spelling the caller sent, so
// a gate keyed by that form matches every request for the organization.
package gate

import "context"

// OrgGate reports whether an organization exists and is active, and whether it
// is frozen. The fan-out materializes nothing for an inactive org, and the
// eligibility preflight reports both states.
type OrgGate interface {
	CheckOrg(ctx context.Context, orgID string) (active bool, frozen bool, err error)
}

// CommsGate decides whether SubmitComms accepts work for an organization. A
// disabled organization's submit fails with FAILED_PRECONDITION. server.Run uses
// AllowAll when Options.CommsGate is nil.
type CommsGate interface {
	Enabled(orgID string) bool
}

// AllowAll is the default OrgGate and CommsGate: every organization is active,
// unfrozen and enabled.
type AllowAll struct{}

// CheckOrg reports every organization active and unfrozen.
func (AllowAll) CheckOrg(context.Context, string) (bool, bool, error) { return true, false, nil }

// Enabled reports every organization enabled.
func (AllowAll) Enabled(string) bool { return true }
