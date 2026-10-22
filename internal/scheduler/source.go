// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"time"
)

// ScheduleSource produces schedule definitions from a domain.
// Each job family implements this interface.
type ScheduleSource interface {
	Name() string
	FetchSchedules(ctx context.Context) ([]ScheduleDefinition, error)
}

// ScheduleDefinition is a domain-agnostic schedule entry.
type ScheduleDefinition struct {
	ID             string        // Unique key among the definitions (the last one wins)
	JobType        string        // asynq task type
	CronExpression string        // 5-field cron
	Payload        []byte        // JSON, opaque to scheduler
	Queue          string        // Target asynq queue
	MaxRetry       int           // 0 = use handler default
	Timeout        time.Duration // 0 = use handler default
}
