// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package migrations_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"sync"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/tetriz-ai/outbox-comms/internal/migrations"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
)

// TestConcurrentUpMigratesOnce pins the migration lock: replicas that start
// together on an empty schema apply each migration once between them, and none
// of them fails.
func TestConcurrentUpMigratesOnce(t *testing.T) {
	ctx := context.Background()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "mig_" + hex.EncodeToString(b)
	if err := testDB.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testDB.Exec("DROP SCHEMA " + schema + " CASCADE") })
	u, err := url.Parse(testdb.SchemaURL())
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()

	const replicas = 4
	var wg sync.WaitGroup
	applied := make([][]int, replicas)
	errs := make([]error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		db, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{Logger: gormlogger.Discard})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if s, err := db.DB(); err == nil {
				_ = s.Close()
			}
		})
		wg.Add(1)
		go func(i int, db *gorm.DB) {
			defer wg.Done()
			<-start
			applied[i], errs[i] = migrations.Up(ctx, db)
		}(i, db)
	}
	close(start)
	wg.Wait()
	all, err := migrations.All()
	if err != nil {
		t.Fatal(err)
	}
	times := map[int]int{}
	for i := 0; i < replicas; i++ {
		if errs[i] != nil {
			t.Fatalf("replica %d: %v", i, errs[i])
		}
		for _, v := range applied[i] {
			times[v]++
		}
	}
	for _, m := range all {
		if times[m.Version] != 1 {
			t.Fatalf("migration %d applied %d times across %d replicas, want 1", m.Version, times[m.Version], replicas)
		}
	}
	var rows int64
	testDB.Raw("SELECT count(*) FROM " + schema + ".outbox_comms_schema_migrations").Scan(&rows)
	if rows != int64(len(all)) {
		t.Fatalf("%d recorded versions, want %d", rows, len(all))
	}
}
