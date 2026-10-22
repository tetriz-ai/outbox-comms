// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
)

// fetchTimeout is the per-source timeout for FetchSchedules calls.
const fetchTimeout = 30 * time.Second

// defaultMaxRetry is the retry count for a periodic task whose source sets none.
// A periodic task runs again on its own schedule, so retrying a failed run adds
// little; a few retries cover a brief database outage.
const defaultMaxRetry = 3

// ConfigProvider implements asynq.PeriodicTaskConfigProvider over the registered
// ScheduleSources.
type ConfigProvider struct {
	sources     []ScheduleSource
	taskTimeout time.Duration
	logger      *slog.Logger
}

// NewConfigProvider aggregates the given sources. taskTimeout applies to a
// definition that sets no timeout of its own (0 means none). A nil logger means
// slog's default.
func NewConfigProvider(sources []ScheduleSource, taskTimeout time.Duration, logger *slog.Logger) *ConfigProvider {
	if logger == nil {
		logger = slog.Default()
	}
	return &ConfigProvider{sources: sources, taskTimeout: taskTimeout, logger: logger}
}

// uniqueTTL is how long one tick of a schedule keeps later ticks of the same
// task out: half the schedule's interval, rounded down to whole seconds, and at
// least one second (asynq's minimum). Each tick enqueues a task with a fresh id
// and this uniqueness lock, not a fixed task id. A run that finishes releases the
// lock; a run whose process died leaves it behind only until the TTL, so the
// next tick enqueues again instead of being refused until asynq recovers the
// dead run. Two runs can then overlap when one outlasts the TTL, which the
// reconcile and prune jobs allow (see internal/jobs).
func uniqueTTL(cronExpr string, now time.Time) (time.Duration, error) {
	sched, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return 0, fmt.Errorf("parse schedule %q: %w", cronExpr, err)
	}
	next := sched.Next(now)
	ttl := (sched.Next(next).Sub(next) / 2).Truncate(time.Second)
	if ttl < time.Second {
		ttl = time.Second
	}
	return ttl, nil
}

// GetConfigs is called by the periodic task manager at its sync interval. A
// source that fails is logged and skipped, so one bad source cannot stop the
// others.
func (p *ConfigProvider) GetConfigs() ([]*asynq.PeriodicTaskConfig, error) {
	seen := make(map[string]*asynq.PeriodicTaskConfig)
	for _, src := range p.sources {
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defs, err := src.FetchSchedules(ctx)
		cancel()
		if err != nil {
			p.logger.Warn("scheduler: failed to fetch schedules from source", "source", src.Name(), "error", err)
			continue
		}
		for _, def := range defs {
			ttl, err := uniqueTTL(def.CronExpression, time.Now())
			if err != nil {
				p.logger.Error("scheduler: skipping a schedule that does not parse", "id", def.ID, "error", err)
				continue
			}
			opts := []asynq.Option{asynq.Unique(ttl)}
			if def.Queue != "" {
				opts = append(opts, asynq.Queue(def.Queue))
			}
			maxRetry := def.MaxRetry
			if maxRetry <= 0 {
				maxRetry = defaultMaxRetry
			}
			opts = append(opts, asynq.MaxRetry(maxRetry))
			timeout := def.Timeout
			if timeout == 0 {
				timeout = p.taskTimeout
			}
			if timeout > 0 {
				opts = append(opts, asynq.Timeout(timeout))
			}
			seen[def.ID] = &asynq.PeriodicTaskConfig{
				Cronspec: def.CronExpression,
				Task:     asynq.NewTask(def.JobType, def.Payload),
				Opts:     opts,
			}
		}
	}
	configs := make([]*asynq.PeriodicTaskConfig, 0, len(seen))
	for _, cfg := range seen {
		configs = append(configs, cfg)
	}
	return configs, nil
}
