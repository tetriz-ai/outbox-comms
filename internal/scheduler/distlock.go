// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrLockHeld is returned by Lock when another holder owns the lock.
var ErrLockHeld = errors.New("scheduler: lock is held by another instance")

// DistLock is a distributed mutex with a TTL.
type DistLock interface {
	// Lock takes the lock if it is free, else returns ErrLockHeld.
	Lock() error
	// Unlock releases the lock if this holder still owns it; false means it had
	// expired or passed to another holder.
	Unlock() (bool, error)
	// Extend resets the TTL if this holder still owns it; false means it was lost.
	Extend() (bool, error)
}

// DistLockFactory creates distributed locks.
type DistLockFactory interface {
	NewMutex(name string, expiry time.Duration) DistLock
}

// redisLockFactory makes single-Redis locks: SET NX PX with a random token, and
// extend and unlock only while the stored token is still this holder's.
type redisLockFactory struct{ client *redis.Client }

// NewDistLockFactory creates a DistLockFactory backed by one Redis.
func NewDistLockFactory(client *redis.Client) DistLockFactory {
	return &redisLockFactory{client: client}
}

func (f *redisLockFactory) NewMutex(name string, expiry time.Duration) DistLock {
	return &redisLock{client: f.client, key: name, expiry: expiry}
}

type redisLock struct {
	client *redis.Client
	key    string
	expiry time.Duration
	token  string
}

// opTimeout bounds each Redis round trip of the lock.
const opTimeout = 5 * time.Second

// unlockScript deletes the key only while it holds this holder's token.
var unlockScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`)

// extendScript resets the TTL only while the key holds this holder's token.
var extendScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`)

func (l *redisLock) Lock() error {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Errorf("lock token: %w", err)
	}
	token := hex.EncodeToString(b)
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	ok, err := l.client.SetNX(ctx, l.key, token, l.expiry).Result()
	if err != nil {
		return fmt.Errorf("take lock %s: %w", l.key, err)
	}
	if !ok {
		return ErrLockHeld
	}
	l.token = token
	return nil
}

func (l *redisLock) Unlock() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	n, err := unlockScript.Run(ctx, l.client, []string{l.key}, l.token).Int()
	if err != nil {
		return false, fmt.Errorf("release lock %s: %w", l.key, err)
	}
	return n == 1, nil
}

func (l *redisLock) Extend() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	n, err := extendScript.Run(ctx, l.client, []string{l.key}, l.token, l.expiry.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("extend lock %s: %w", l.key, err)
	}
	return n == 1, nil
}
