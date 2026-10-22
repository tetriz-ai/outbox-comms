// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package testdb gives a test package its own throwaway Postgres schema with the
// migrations applied. It reads OUTBOX_COMMS_TEST_DATABASE_URL. When that is unset
// the store-backed tests skip, loudly; with OUTBOX_COMMS_REQUIRE_TEST_DB=1 they
// fail instead, so a missing database can never pass for a green run.
// Tests that need Redis read OUTBOX_COMMS_TEST_REDIS_ADDR the same way.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/migrations"
)

// EnvURL names the variable holding the test database URL, and EnvRedis the one
// holding a test Redis address (host:port).
const (
	EnvURL   = "OUTBOX_COMMS_TEST_DATABASE_URL"
	EnvRedis = "OUTBOX_COMMS_TEST_REDIS_ADDR"
)

// schemaURL is the URL of the schema Main created, for tests that open their
// own connection (the server's start-up path).
var schemaURL string

// SchemaURL returns the database URL of this package's test schema.
func SchemaURL() string { return schemaURL }

// RedisAddr returns the test Redis address, skipping the test when it is unset
// (failing it under OUTBOX_COMMS_REQUIRE_TEST_DB=1).
func RedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(EnvRedis)
	if addr == "" {
		if os.Getenv("OUTBOX_COMMS_REQUIRE_TEST_DB") == "1" {
			t.Fatalf("%s is unset and OUTBOX_COMMS_REQUIRE_TEST_DB=1", EnvRedis)
		}
		t.Skipf("set %s to a Redis address to run this test", EnvRedis)
	}
	return addr
}

// Main is a TestMain body: it creates a fresh schema, migrates it, runs the
// tests with *db set, and drops the schema. It returns the exit code.
func Main(m *testing.M, db **gorm.DB) int {
	raw := os.Getenv(EnvURL)
	if raw == "" {
		if os.Getenv("OUTBOX_COMMS_REQUIRE_TEST_DB") == "1" {
			fmt.Fprintf(os.Stderr, "testdb: %s is unset and OUTBOX_COMMS_REQUIRE_TEST_DB=1\n", EnvURL)
			return 1
		}
		fmt.Fprintf(os.Stderr, "testdb: SKIPPING store-backed tests: set %s to a Postgres URL to run them\n", EnvURL)
		return 0
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "t_" + hex.EncodeToString(b)

	model.RegisterSerializers()
	admin, err := gorm.Open(postgres.Open(raw), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: connect: %v\n", err)
		return 1
	}
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		fmt.Fprintf(os.Stderr, "testdb: create schema: %v\n", err)
		return 1
	}
	defer func() {
		admin.Exec("DROP SCHEMA " + schema + " CASCADE")
		if s, err := admin.DB(); err == nil {
			_ = s.Close()
		}
	}()

	u, err := url.Parse(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: parse %s: %v\n", EnvURL, err)
		return 1
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	schemaURL = u.String()
	conn, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: connect to schema: %v\n", err)
		return 1
	}
	if _, err := migrations.Up(context.Background(), conn); err != nil {
		fmt.Fprintf(os.Stderr, "testdb: migrate: %v\n", err)
		return 1
	}
	*db = conn
	code := m.Run()
	if s, err := conn.DB(); err == nil {
		_ = s.Close()
	}
	return code
}

// Tx returns a transaction rolled back when the test ends, for isolation.
func Tx(t *testing.T, db *gorm.DB) *gorm.DB {
	t.Helper()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}
