// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api

// ValidateEntry exposes SubmitComms's per-entry check to the external tests,
// so the eligibility check can be held to the same verdict.
var ValidateEntry = validateEntry
