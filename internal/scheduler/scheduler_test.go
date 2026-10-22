// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type staticSource []ScheduleDefinition

func (staticSource) Name() string { return "static" }
func (s staticSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	return s, nil
}

type failingSource struct{}

func (failingSource) Name() string { return "failing" }
func (failingSource) FetchSchedules(context.Context) ([]ScheduleDefinition, error) {
	return nil, errors.New("down")
}

// TestConfigProvider proves a failing source is skipped, ids dedup last-wins,
// and the retry and timeout defaults apply.
func TestConfigProvider(t *testing.T) {
	p := NewConfigProvider([]ScheduleSource{
		failingSource{},
		staticSource{{ID: "a", JobType: "t.a", CronExpression: "* * * * *", Queue: "q"}, {ID: "a", JobType: "t.a2", CronExpression: "@every 1s"}},
	}, time.Minute, nil)
	cfgs, err := p.GetConfigs()
	if err != nil || len(cfgs) != 1 || cfgs[0].Task.Type() != "t.a2" {
		t.Fatalf("configs = %v, %v", cfgs, err)
	}
	var unique time.Duration
	for _, o := range cfgs[0].Opts {
		switch o.Type() {
		case asynq.TaskIDOpt:
			t.Fatalf("a periodic task carries a fixed id %v; a dead run would block later ticks", o.Value())
		case asynq.UniqueOpt:
			unique = o.Value().(time.Duration)
		}
	}
	if unique != time.Second {
		t.Fatalf("uniqueness TTL %v for @every 1s, want 1s", unique)
	}
}

// TestUniqueTTL pins the uniqueness TTL: half the schedule's interval, whole
// seconds, at least one second.
func TestUniqueTTL(t *testing.T) {
	now := time.Date(2026, 10, 9, 10, 0, 30, 0, time.UTC)
	for expr, want := range map[string]time.Duration{
		"@every 2s":    time.Second,
		"@every 1s":    time.Second,
		"@every 5s":    2 * time.Second,
		"*/10 * * * *": 5 * time.Minute,
		"0 0 * * *":    12 * time.Hour,
	} {
		if got, err := uniqueTTL(expr, now); err != nil || got != want {
			t.Errorf("uniqueTTL(%q) = %v, %v; want %v", expr, got, err, want)
		}
	}
	if _, err := uniqueTTL("not a schedule", now); err == nil {
		t.Error("an unparsable schedule must be an error")
	}
}

func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("OUTBOX_COMMS_TEST_REDIS_ADDR")
	if addr == "" {
		if os.Getenv("OUTBOX_COMMS_REQUIRE_TEST_DB") == "1" {
			t.Fatal("OUTBOX_COMMS_TEST_REDIS_ADDR is unset")
		}
		t.Skip("set OUTBOX_COMMS_TEST_REDIS_ADDR to run the leader-election test")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, DB: 8})
	if err := c.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestLeaderElection runs two schedulers on one Redis: exactly one leads and
// fires the cron; when it stops, the other takes over and the cron keeps firing.
func TestLeaderElection(t *testing.T) {
	rdb := testRedis(t)
	var fired atomic.Int64
	newSched := func() *Scheduler {
		mux := asynq.NewServeMux()
		mux.HandleFunc("t.tick", func(context.Context, *asynq.Task) error { fired.Add(1); return nil })
		src := staticSource{{ID: "tick", JobType: "t.tick", CronExpression: "@every 1s", Queue: "outbox_comms_test"}}
		return New(Config{Queues: map[string]int{"outbox_comms_test": 1}, LockKey: "test:leader", LockExpiry: 3 * time.Second, LockRetryInterval: 200 * time.Millisecond, RefreshInterval: time.Second, ShutdownTimeout: 3 * time.Second},
			rdb, []ScheduleSource{src}, mux, NewDistLockFactory(rdb))
	}
	a, b := newSched(), newSched()
	ctx := context.Background()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 10*time.Second, a.IsLeader)
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if b.IsLeader() {
		t.Fatal("two leaders at once")
	}
	waitUntil(t, 10*time.Second, func() bool { return fired.Load() >= 2 })
	_ = a.Stop(ctx)
	waitUntil(t, 10*time.Second, b.IsLeader)
	before := fired.Load()
	waitUntil(t, 10*time.Second, func() bool { return fired.Load() > before })
	_ = b.Stop(ctx)
	if err := b.Start(ctx); err != nil || b.IsLeader() {
		t.Fatal("Start after Stop must be a no-op")
	}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestStopWithoutStart(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	s := New(Config{Queues: map[string]int{"q": 1}}, rdb, nil, asynq.NewServeMux(), NewDistLockFactory(rdb))
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMiddleware(t *testing.T) {
	h := RecoveryMiddleware(nil)(LoggingMiddleware(nil)(asynq.HandlerFunc(func(context.Context, *asynq.Task) error { panic("boom") })))
	if err := h.ProcessTask(context.Background(), asynq.NewTask("t.x", nil)); err == nil {
		t.Fatal("a panic must become an error")
	}
	h = LoggingMiddleware(nil)(asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return errors.New("x") }))
	if err := h.ProcessTask(context.Background(), asynq.NewTask("t.x", nil)); err == nil {
		t.Fatal("a handler error must propagate")
	}
}
