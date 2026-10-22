// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server_test

import (
	"testing"

	"gorm.io/gorm/schema"

	_ "github.com/tetriz-ai/outbox-comms/server"
)

// TestImportLeavesGORMAlone pins that importing the server registers no GORM
// serializer under a generic name, so a program with its own "protojson" or
// "protoenum" serializer keeps it. The service's own register, namespaced, when
// it opens a database.
func TestImportLeavesGORMAlone(t *testing.T) {
	for _, name := range []string{"protojson", "protoenum"} {
		if _, ok := schema.GetSerializer(name); ok {
			t.Errorf("a %q serializer is registered", name)
		}
	}
}
