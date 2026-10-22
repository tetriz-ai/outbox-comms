// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/hibiken/asynq"
)

// LoggingMiddleware logs each task's failure at Error and its completion at
// Debug, with its duration, to l (nil means slog's default).
func LoggingMiddleware(l *slog.Logger) asynq.MiddlewareFunc {
	if l == nil {
		l = slog.Default()
	}
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
			start := time.Now()
			err := next.ProcessTask(ctx, t)
			duration := time.Since(start)
			if err != nil {
				l.ErrorContext(ctx, "scheduler: task failed", "task_type", t.Type(), "duration", duration, "error", err)
				return fmt.Errorf("processing task %s: %w", t.Type(), err)
			}
			l.DebugContext(ctx, "scheduler: task completed", "task_type", t.Type(), "duration", duration)
			return nil
		})
	}
}

// RecoveryMiddleware turns a handler panic into a task error with the stack
// logged to l (nil means slog's default), so one bad task cannot take the
// worker down.
func RecoveryMiddleware(l *slog.Logger) asynq.MiddlewareFunc {
	if l == nil {
		l = slog.Default()
	}
	return func(next asynq.Handler) asynq.Handler {
		return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) (err error) {
			defer func() {
				if r := recover(); r != nil {
					l.ErrorContext(ctx, "scheduler: task panicked", "task_type", t.Type(), "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
					err = fmt.Errorf("scheduler: panic in task %s: %v", t.Type(), r)
				}
			}()
			return next.ProcessTask(ctx, t)
		})
	}
}
