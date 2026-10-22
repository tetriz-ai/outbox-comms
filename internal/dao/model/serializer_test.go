// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package model_test

import (
	"testing"

	"gorm.io/gorm/schema"

	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/server"
)

// hostSerializer stands in for a host program's own serializer under a
// generic name.
type hostSerializer struct{ schema.JSONSerializer }

// TestSerializersRegisterOnOpen pins when the service's GORM serializers
// appear: not at import, and once a database is opened, under the service's
// own names only, leaving a host's "protojson" serializer in place. It needs
// no database: OpenDB registers before it dials, and the dial to port 1 fails.
func TestSerializersRegisterOnOpen(t *testing.T) {
	for _, name := range []string{model.SerializerProtoEnum, model.SerializerProtoJSON} {
		if _, ok := schema.GetSerializer(name); ok {
			t.Fatalf("%s is registered at import", name)
		}
	}
	host := hostSerializer{}
	schema.RegisterSerializer("protojson", host)
	_, _ = server.OpenDB("postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	for _, name := range []string{model.SerializerProtoEnum, model.SerializerProtoJSON} {
		if _, ok := schema.GetSerializer(name); !ok {
			t.Errorf("%s is not registered after OpenDB", name)
		}
	}
	if got, ok := schema.GetSerializer("protojson"); !ok || got != (hostSerializer{}) {
		t.Errorf("the host's protojson serializer was replaced: %T", got)
	}
}
