// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package migrations_test

import (
	"context"
	"os"
	"testing"

	"gorm.io/gorm"

	"github.com/tetriz-ai/outbox-comms/internal/migrations"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
)

var testDB *gorm.DB

// TestMain applies the migrations once through testdb.Main.
func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

func TestAllOrdered(t *testing.T) {
	all, err := migrations.All()
	if err != nil || len(all) == 0 || all[0].Version != 1 {
		t.Fatalf("All = %v, %v", all, err)
	}
	for i := 1; i < len(all); i++ {
		if all[i].Version <= all[i-1].Version {
			t.Fatal("migrations out of order")
		}
	}
}

// TestUpIsIdempotent proves a second Up applies nothing and the schema is in
// place.
func TestUpIsIdempotent(t *testing.T) {
	applied, err := migrations.Up(context.Background(), testDB)
	if err != nil || len(applied) != 0 {
		t.Fatalf("second Up applied %v, %v", applied, err)
	}
	var tables int64
	testDB.Raw("SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE 'comms_%'").Scan(&tables)
	if tables != 7 {
		t.Fatalf("%d comms tables, want 7", tables)
	}
}

// TestUpDownUp pins that 0001_init.down.sql removes every table the up file
// creates, and that Up then rebuilds the same schema, claim_id included.
func TestUpDownUp(t *testing.T) {
	ctx := context.Background()
	count := func() int64 {
		var n int64
		testDB.Raw("SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE 'comms_%'").Scan(&n)
		return n
	}
	if n := count(); n != 7 {
		t.Fatalf("after Up: %d comms tables, want 7", n)
	}
	down, err := os.ReadFile("0001_init.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if err := testDB.Exec(string(down)).Error; err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := count(); n != 0 {
		t.Fatalf("after down: %d comms tables, want 0", n)
	}
	if err := testDB.Exec("DELETE FROM outbox_comms_schema_migrations WHERE version = 1").Error; err != nil {
		t.Fatal(err)
	}
	applied, err := migrations.Up(ctx, testDB)
	if err != nil || len(applied) != 1 || applied[0] != 1 {
		t.Fatalf("Up after down applied %v, %v; want [1]", applied, err)
	}
	var claimCol int64
	testDB.Raw("SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'comms_messages' AND column_name = 'claim_id'").Scan(&claimCol)
	if n := count(); n != 7 || claimCol != 1 {
		t.Fatalf("after Up again: %d comms tables, claim_id columns %d; want 7, 1", n, claimCol)
	}
}
