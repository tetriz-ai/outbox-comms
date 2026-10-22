// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
)

const staleTestQueue = "outbox_comms_test"

func staleTestScheduler(t *testing.T, mux *asynq.ServeMux, src staticSource) *Scheduler {
	t.Helper()
	rdb := testRedis(t)
	return New(Config{Queues: map[string]int{staleTestQueue: 1}, Concurrency: 4, LockKey: "test:leader", LockExpiry: 3 * time.Second,
		LockRetryInterval: 200 * time.Millisecond, RefreshInterval: time.Second, ShutdownTimeout: 3 * time.Second},
		rdb, []ScheduleSource{src}, mux, NewDistLockFactory(rdb))
}

// TestTickAfterADeadRun pins that a run which never finishes does not hold later
// ticks back. In Redis a run whose process was killed looks the same as one that
// is still running until asynq recovers it, which takes a minute or more. Here
// the first run blocks for the whole test; the next tick must still enqueue and
// start a run within one interval, give or take two seconds of slack.
func TestTickAfterADeadRun(t *testing.T) {
	const interval = 2 * time.Second
	var calls atomic.Int64
	var firstAt, secondAt atomic.Int64
	release := make(chan struct{})
	mux := asynq.NewServeMux()
	mux.HandleFunc("t.stall", func(ctx context.Context, _ *asynq.Task) error {
		switch calls.Add(1) {
		case 1:
			firstAt.Store(time.Now().UnixNano())
			select {
			case <-release:
			case <-ctx.Done():
			}
		case 2:
			secondAt.Store(time.Now().UnixNano())
		}
		return nil
	})
	s := staleTestScheduler(t, mux, staticSource{{ID: "stall", JobType: "t.stall", CronExpression: "@every 2s", Queue: staleTestQueue}})
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		_ = s.Stop(context.Background())
	})
	waitUntil(t, 15*time.Second, func() bool { return calls.Load() >= 1 })
	// Two seconds of slack over the interval: the tick, the enqueue and the
	// worker's poll each take a little, more on a loaded machine.
	const slack = 2 * time.Second
	deadline := time.Unix(0, firstAt.Load()).Add(interval + slack)
	for calls.Load() < 2 && time.Now().Before(deadline.Add(time.Second)) {
		time.Sleep(50 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("no second run within %v of a run that never finished", interval+slack)
	}
	if gap := time.Duration(secondAt.Load() - firstAt.Load()); gap > interval+slack {
		t.Fatalf("the second run started %v after the first; want within one interval (%v) and %v of slack", gap, interval, slack)
	}
}

// recordingHandler keeps every ERROR record slog receives.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelError }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// loggedFailure reports whether an ERROR record names the task as not enqueued.
func (h *recordingHandler) loggedFailure(task string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "task" && a.Value.String() == task {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func enqueueFailures(t *testing.T, task string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != "outbox_comms_scheduler_enqueue_failures_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "task" && l.GetValue() == task {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestEnqueueFailureIsVisible pins that a tick the scheduler cannot enqueue is
// logged at ERROR with its task type and counted. A task planted with the
// schedule's own options, held for an hour, makes every tick fail.
func TestEnqueueFailureIsVisible(t *testing.T) {
	rec := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })

	src := staticSource{{ID: "held", JobType: "t.held", CronExpression: "@every 1s", Queue: staleTestQueue}}
	mux := asynq.NewServeMux()
	mux.HandleFunc("t.held", func(context.Context, *asynq.Task) error { return nil })
	s := staleTestScheduler(t, mux, src)
	cfgs, err := NewConfigProvider([]ScheduleSource{src}, 0, nil).GetConfigs()
	if err != nil || len(cfgs) != 1 {
		t.Fatalf("configs %v, %v", cfgs, err)
	}
	opts := append(append([]asynq.Option{}, cfgs[0].Opts...), asynq.ProcessIn(time.Hour), asynq.Unique(time.Hour))
	if _, err := asynq.NewClientFromRedisClient(s.redisClient).Enqueue(cfgs[0].Task, opts...); err != nil {
		t.Fatalf("plant the held task: %v", err)
	}
	before := enqueueFailures(t, "t.held")
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	waitUntil(t, 15*time.Second, func() bool { return rec.loggedFailure("t.held") })
	if after := enqueueFailures(t, "t.held"); after <= before {
		t.Fatalf("enqueue failures for t.held went %v -> %v; want an increment", before, after)
	}
}
