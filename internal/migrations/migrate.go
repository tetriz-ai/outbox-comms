// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package migrations embeds the SQL schema and applies it in order. Up runs every
// unapplied NNNN_name.up.sql file, each in its own transaction, under a Postgres
// advisory lock so two replicas starting together do not race.
package migrations

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

//go:embed *.sql
var files embed.FS

// advisoryLockKey is an arbitrary constant that identifies this service's
// migration lock among other users of the same database.
const advisoryLockKey = 7461_2026

// Migration is one numbered schema step.
type Migration struct {
	Version int
	Name    string
	UpSQL   string
}

// All returns the embedded migrations in version order.
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	var out []Migration
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		num, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migration %q: name must be NNNN_name.up.sql", name)
		}
		v, err := strconv.Atoi(num)
		if err != nil {
			return nil, fmt.Errorf("migration %q: version: %w", name, err)
		}
		b, err := files.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", name, err)
		}
		out = append(out, Migration{Version: v, Name: name, UpSQL: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	for i := 1; i < len(out); i++ {
		if out[i].Version == out[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].Version)
		}
	}
	return out, nil
}

// Up applies every migration newer than the database's recorded version and
// returns the versions it applied.
func Up(ctx context.Context, db *gorm.DB) ([]int, error) {
	all, err := All()
	if err != nil {
		return nil, err
	}
	var applied []int
	err = db.WithContext(ctx).Connection(func(conn *gorm.DB) error {
		if err := conn.Exec("SELECT pg_advisory_lock(?)", advisoryLockKey).Error; err != nil {
			return fmt.Errorf("acquire migration lock: %w", err)
		}
		defer conn.Exec("SELECT pg_advisory_unlock(?)", advisoryLockKey)

		if err := conn.Exec(`CREATE TABLE IF NOT EXISTS outbox_comms_schema_migrations (
			version    INT PRIMARY KEY,
			applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now())`).Error; err != nil {
			return fmt.Errorf("create migrations table: %w", err)
		}
		var current int
		if err := conn.Raw("SELECT COALESCE(MAX(version), 0) FROM outbox_comms_schema_migrations").Scan(&current).Error; err != nil {
			return fmt.Errorf("read schema version: %w", err)
		}
		for _, m := range all {
			if m.Version <= current {
				continue
			}
			err := conn.Transaction(func(tx *gorm.DB) error {
				if err := tx.Exec(m.UpSQL).Error; err != nil {
					return fmt.Errorf("apply %s: %w", m.Name, err)
				}
				return tx.Exec("INSERT INTO outbox_comms_schema_migrations (version) VALUES (?)", m.Version).Error
			})
			if err != nil {
				return err
			}
			applied = append(applied, m.Version)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return applied, nil
}
