// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package scheduler runs the asynq worker on every replica and the periodic task
// manager on exactly one: a Redis lock elects the leader, and only the
// leader turns the registered cron definitions into enqueued tasks. If the
// leader dies, its lock expires and another replica takes over.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/tetriz-ai/outbox-comms/internal/metrics"
)

// Config tunes the worker and the leader election.
type Config struct {
	// Queues maps each asynq queue the worker drains to its priority weight.
	Queues map[string]int
	// Concurrency is the worker's parallel task limit (0 means 10).
	Concurrency int
	// ShutdownTimeout bounds the drain of in-flight tasks on Stop (0 means 30 s).
	ShutdownTimeout time.Duration
	// LockKey names the leader lock in Redis.
	LockKey string
	// LockExpiry is the lock's TTL; the leader extends it every LockExpiry/3
	// (0 means 30 s).
	LockExpiry time.Duration
	// LockRetryInterval is how often a follower retries the lock (0 means 10 s).
	LockRetryInterval time.Duration
	// RefreshInterval is how often the leader re-reads the cron definitions
	// (0 means 1 minute).
	RefreshInterval time.Duration
	// TaskTimeout bounds a periodic task when its definition sets none (0 means
	// none).
	TaskTimeout time.Duration
	// Logger receives the scheduler's and asynq's log lines. Nil means slog's
	// default.
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.LockKey == "" {
		c.LockKey = "outbox_comms:scheduler:leader"
	}
	if c.LockExpiry <= 0 {
		c.LockExpiry = 30 * time.Second
	}
	if c.LockRetryInterval <= 0 {
		c.LockRetryInterval = 10 * time.Second
	}
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = time.Minute
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Scheduler manages the asynq worker (all replicas) and the periodic task
// manager (leader only).
type Scheduler struct {
	cfg            Config
	worker         *asynq.Server
	mux            *asynq.ServeMux
	lockFactory    DistLockFactory
	configProvider *ConfigProvider
	redisClient    *redis.Client

	mu          sync.Mutex // protects manager, leaderMutex, started, stopped and cancel
	manager     *asynq.PeriodicTaskManager
	leaderMutex DistLock
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	started     bool
	stopped     bool
}

// New wires a Scheduler. It does not start the worker; call Start.
func New(cfg Config, redisClient *redis.Client, sources []ScheduleSource, mux *asynq.ServeMux, lockFactory DistLockFactory) *Scheduler {
	cfg = cfg.withDefaults()
	worker := asynq.NewServerFromRedisClient(redisClient, asynq.Config{
		Concurrency:     cfg.Concurrency,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Queues:          cfg.Queues,
		Logger:          slogAdapter{cfg.Logger},
	})
	return &Scheduler{
		cfg:            cfg,
		worker:         worker,
		mux:            mux,
		lockFactory:    lockFactory,
		configProvider: NewConfigProvider(sources, cfg.TaskTimeout, cfg.Logger),
		redisClient:    redisClient,
	}
}

// Start binds the asynq worker and starts leader election. Idempotent; a no-op
// after Stop. Cancelling ctx shuts the leader loop down; call Stop for the full
// cleanup.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped || s.started {
		s.mu.Unlock()
		return nil
	}
	// Hold the lock across worker.Start so Stop sees either no worker or a fully
	// started one: asynq panics on Shutdown of a half-started server.
	if err := s.worker.Start(s.mux); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: start worker: %w", err)
	}
	s.started = true
	lifecycleCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	s.mu.Unlock()

	s.cfg.Logger.InfoContext(ctx, "scheduler: worker started")
	go func() {
		defer s.wg.Done()
		s.runLeader(lifecycleCtx)
	}()
	return nil
}

// runLeader loops leader terms until ctx is cancelled.
func (s *Scheduler) runLeader(ctx context.Context) {
	for ctx.Err() == nil {
		s.runLeaderOnce(ctx)
	}
}

// runLeaderOnce performs one leader term: acquire the lock, start the periodic
// manager, extend the lock until it is lost or ctx is cancelled.
func (s *Scheduler) runLeaderOnce(ctx context.Context) {
	mutex := s.lockFactory.NewMutex(s.cfg.LockKey, s.cfg.LockExpiry)
	for {
		if err := mutex.Lock(); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.cfg.LockRetryInterval):
		}
	}
	s.cfg.Logger.InfoContext(ctx, "scheduler: acquired leader lock")

	s.mu.Lock()
	s.leaderMutex = mutex
	s.mu.Unlock()

	manager, err := asynq.NewPeriodicTaskManager(asynq.PeriodicTaskManagerOpts{
		RedisUniversalClient:       s.redisClient,
		PeriodicTaskConfigProvider: s.configProvider,
		SyncInterval:               s.cfg.RefreshInterval,
		SchedulerOpts:              &asynq.SchedulerOpts{Logger: slogAdapter{s.cfg.Logger}, EnqueueErrorHandler: s.enqueueFailed},
	})
	if err == nil {
		err = manager.Start()
	}
	if err != nil {
		s.cfg.Logger.ErrorContext(ctx, "scheduler: periodic task manager failed to start; stepping down", "error", err)
		s.releaseLock()
		select {
		case <-ctx.Done():
		case <-time.After(s.cfg.LockRetryInterval):
		}
		return
	}
	s.mu.Lock()
	s.manager = manager
	s.mu.Unlock()

	s.extendLock(ctx)
}

// enqueueFailed logs and counts a periodic task tick the leader could not
// enqueue. Without it asynq drops the error, and a schedule that never runs
// looks the same as one that runs and finds nothing to do.
func (s *Scheduler) enqueueFailed(task *asynq.Task, _ []asynq.Option, err error) {
	s.cfg.Logger.Error("scheduler: periodic task not enqueued", "task", task.Type(), "error", err)
	metrics.RecordSchedulerEnqueueFailure(task.Type())
}

// Stop shuts down the worker and the manager and releases the leader lock. Safe
// to call even if Start never ran.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(s.cfg.ShutdownTimeout):
		s.cfg.Logger.WarnContext(ctx, "scheduler: timed out waiting for the leader loop")
	}

	s.mu.Lock()
	if s.manager != nil {
		s.manager.Shutdown()
		s.manager = nil
	}
	if s.leaderMutex != nil {
		if _, err := s.leaderMutex.Unlock(); err != nil {
			s.cfg.Logger.WarnContext(ctx, "scheduler: failed to release leader lock", "error", err)
		}
		s.leaderMutex = nil
	}
	wasStarted := s.started
	s.mu.Unlock()

	if wasStarted {
		s.worker.Shutdown()
	}
	return nil
}

// releaseLock releases the leader lock under the mutex.
func (s *Scheduler) releaseLock() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaderMutex != nil {
		if _, err := s.leaderMutex.Unlock(); err != nil {
			s.cfg.Logger.Warn("scheduler: failed to release leader lock", "error", err)
		}
		s.leaderMutex = nil
	}
}

// extendLock extends the leader lock every LockExpiry/3. When an extension fails
// the manager is shut down and the method returns so the caller re-enters
// election.
func (s *Scheduler) extendLock(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.LockExpiry / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			mutex := s.leaderMutex
			s.mu.Unlock()
			if mutex == nil {
				return
			}
			if ok, err := mutex.Extend(); err != nil || !ok {
				s.cfg.Logger.WarnContext(ctx, "scheduler: lost leader lock, re-entering election", "error", err)
				s.mu.Lock()
				if s.manager != nil {
					s.manager.Shutdown()
					s.manager = nil
				}
				if s.leaderMutex != nil {
					_, _ = s.leaderMutex.Unlock()
					s.leaderMutex = nil
				}
				s.mu.Unlock()
				return
			}
		}
	}
}

// IsLeader reports whether this replica holds the leader lock now.
func (s *Scheduler) IsLeader() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manager != nil
}

// slogAdapter routes asynq's internal logging through the scheduler's logger.
type slogAdapter struct{ l *slog.Logger }

func (a slogAdapter) Debug(args ...any) { a.l.Debug(fmt.Sprint(args...)) }
func (a slogAdapter) Info(args ...any)  { a.l.Info(fmt.Sprint(args...)) }
func (a slogAdapter) Warn(args ...any)  { a.l.Warn(fmt.Sprint(args...)) }
func (a slogAdapter) Error(args ...any) { a.l.Error(fmt.Sprint(args...)) }
func (a slogAdapter) Fatal(args ...any) { a.l.Error(fmt.Sprint(args...)) }
