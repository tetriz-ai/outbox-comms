// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newLocks(t *testing.T) (DistLockFactory, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return NewDistLockFactory(c), mr
}

// TestLockContention proves only one holder at a time: a second Lock on a held
// key returns ErrLockHeld, and succeeds once the first holder releases it.
func TestLockContention(t *testing.T) {
	f, _ := newLocks(t)
	a, b := f.NewMutex("leader", 10*time.Second), f.NewMutex("leader", 10*time.Second)
	if err := a.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := b.Lock(); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("second holder: want ErrLockHeld, got %v", err)
	}
	if ok, err := a.Unlock(); !ok || err != nil {
		t.Fatalf("release: %v %v", ok, err)
	}
	if err := b.Lock(); err != nil {
		t.Fatalf("after release the lock must be free: %v", err)
	}
}

// TestLockExpiry proves a lock the holder stops extending expires, after which
// another holder can take it, and the old holder can neither extend nor release
// it.
func TestLockExpiry(t *testing.T) {
	f, mr := newLocks(t)
	a, b := f.NewMutex("leader", 2*time.Second), f.NewMutex("leader", 2*time.Second)
	if err := a.Lock(); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(time.Second)
	if ok, err := a.Extend(); !ok || err != nil {
		t.Fatalf("extend while held: %v %v", ok, err)
	}
	mr.FastForward(1500 * time.Millisecond)
	if err := b.Lock(); !errors.Is(err, ErrLockHeld) {
		t.Fatalf("the extension must keep the lock: %v", err)
	}
	mr.FastForward(time.Second)
	if err := b.Lock(); err != nil {
		t.Fatalf("an expired lock must be free: %v", err)
	}
	if ok, err := a.Extend(); ok || err != nil {
		t.Fatalf("the old holder must not extend a lock it lost: %v %v", ok, err)
	}
}

// TestUnlockChecksToken proves a holder whose lock expired and passed to another
// cannot release the new holder's lock.
func TestUnlockChecksToken(t *testing.T) {
	f, mr := newLocks(t)
	a, b := f.NewMutex("leader", time.Second), f.NewMutex("leader", time.Second)
	if err := a.Lock(); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	if err := b.Lock(); err != nil {
		t.Fatal(err)
	}
	if ok, err := a.Unlock(); ok || err != nil {
		t.Fatalf("a stale holder's Unlock must be a no-op: %v %v", ok, err)
	}
	if !mr.Exists("leader") {
		t.Fatal("the new holder's lock was deleted")
	}
	if ok, _ := b.Unlock(); !ok {
		t.Fatal("the current holder must release it")
	}
}

func TestLockRedisDown(t *testing.T) {
	f, mr := newLocks(t)
	mr.Close()
	l := f.NewMutex("leader", time.Second)
	if err := l.Lock(); err == nil || errors.Is(err, ErrLockHeld) {
		t.Fatalf("an unreachable Redis is an error, not a held lock: %v", err)
	}
}
