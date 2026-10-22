// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package model

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gorm.io/gorm/schema"
)

// The two GORM serializers below let a model field be a proto enum (stored as its
// value name, e.g. "MESSAGE_STATUS_QUEUED") or a proto message (stored as
// protojson in a jsonb column). Storing enum names, not numbers, keeps rows
// readable in SQL and stable if enum numbers are ever reordered.
//
// GORM keeps serializers in one process-wide table, so they are registered
// under names of this service's own, and only when a database is opened
// (RegisterSerializers), never at import.
const (
	// SerializerProtoEnum is the name model tags use for a proto enum field.
	SerializerProtoEnum = "outboxcomms_protoenum"
	// SerializerProtoJSON is the name model tags use for a proto message field.
	SerializerProtoJSON = "outboxcomms_protojson"
)

var registerOnce sync.Once

// RegisterSerializers adds the two serializers to GORM's table. It is safe to
// call more than once; every handle opened on the models needs it first.
func RegisterSerializers() {
	registerOnce.Do(func() {
		schema.RegisterSerializer(SerializerProtoEnum, protoEnumSerializer{})
		schema.RegisterSerializer(SerializerProtoJSON, protoJSONSerializer{})
	})
}

type protoEnumSerializer struct{}

// Scan parses the stored enum name back into the field's enum type. An unknown
// name is an error, never a silent zero.
func (protoEnumSerializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue any) error {
	if dbValue == nil {
		return nil
	}
	var name string
	switch v := dbValue.(type) {
	case string:
		name = v
	case []byte:
		name = string(v)
	default:
		return fmt.Errorf("protoenum: column %s: unsupported type %T", field.DBName, dbValue)
	}
	enumVal := reflect.New(field.FieldType).Elem()
	e, ok := enumVal.Interface().(protoreflect.Enum)
	if !ok {
		return fmt.Errorf("protoenum: field %s is not a proto enum", field.Name)
	}
	ev := e.Descriptor().Values().ByName(protoreflect.Name(name))
	if ev == nil {
		return fmt.Errorf("protoenum: column %s: unknown %s value %q", field.DBName, e.Descriptor().FullName(), name)
	}
	enumVal.SetInt(int64(ev.Number()))
	field.ReflectValueOf(ctx, dst).Set(enumVal)
	return nil
}

// Value stores the enum's value name.
func (protoEnumSerializer) Value(_ context.Context, field *schema.Field, _ reflect.Value, fieldValue any) (any, error) {
	e, ok := fieldValue.(protoreflect.Enum)
	if !ok {
		return nil, fmt.Errorf("protoenum: field %s is not a proto enum (%T)", field.Name, fieldValue)
	}
	ev := e.Descriptor().Values().ByNumber(e.Number())
	if ev == nil {
		return nil, fmt.Errorf("protoenum: field %s: undefined enum number %d", field.Name, e.Number())
	}
	return string(ev.Name()), nil
}

type protoJSONSerializer struct{}

// Scan decodes jsonb into the field's proto message type. SQL NULL leaves the
// field nil. Unknown fields are discarded so a row written by a newer build still
// reads.
func (protoJSONSerializer) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, dbValue any) error {
	if dbValue == nil {
		return nil
	}
	var b []byte
	switch v := dbValue.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		return fmt.Errorf("protojson: column %s: unsupported type %T", field.DBName, dbValue)
	}
	if field.FieldType.Kind() != reflect.Pointer {
		return fmt.Errorf("protojson: field %s must be a pointer to a proto message", field.Name)
	}
	msgVal := reflect.New(field.FieldType.Elem())
	m, ok := msgVal.Interface().(proto.Message)
	if !ok {
		return fmt.Errorf("protojson: field %s is not a proto message", field.Name)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, m); err != nil {
		return fmt.Errorf("protojson: column %s: %w", field.DBName, err)
	}
	field.ReflectValueOf(ctx, dst).Set(msgVal)
	return nil
}

// Value encodes the message as protojson; a nil message stores SQL NULL, or the
// column's declared default when the tag sets one.
func (protoJSONSerializer) Value(_ context.Context, field *schema.Field, _ reflect.Value, fieldValue any) (any, error) {
	m, ok := fieldValue.(proto.Message)
	if !ok || m == nil || reflect.ValueOf(m).IsNil() {
		if field.HasDefaultValue && field.DefaultValue != "" {
			return trimQuotes(field.DefaultValue), nil
		}
		return nil, nil
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("protojson: field %s: %w", field.Name, err)
	}
	return string(b), nil
}

func trimQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}
