// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/templates"
)

func valid() templates.Contract {
	return templates.Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_ALERT,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}
}

// TestBuiltins pins the three built-in usecases and their contracts.
func TestBuiltins(t *testing.T) {
	tests := []struct {
		key       string
		category  pb.MessageCategory
		guarantee pb.DeliveryGuarantee
	}{
		{templates.UsecaseNotice, pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED},
		{templates.UsecaseAlert, pb.MessageCategory_MESSAGE_CATEGORY_ALERT, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED},
		{templates.UsecaseDigest, pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			c, err := templates.Get(tt.key)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if c.Category != tt.category || c.DefaultGuarantee != tt.guarantee {
				t.Fatalf("contract = %v/%v, want %v/%v", c.Category, c.DefaultGuarantee, tt.category, tt.guarantee)
			}
			if !c.ChannelEnabled(pb.Channel_CHANNEL_EMAIL) || !c.ChannelEnabled(pb.Channel_CHANNEL_SLACK) || c.ChannelEnabled(pb.Channel(4)) {
				t.Fatalf("built-ins enable exactly email and Slack")
			}
		})
	}
}

// TestRegisterRejects proves Register refuses a contract that would fail later.
func TestRegisterRejects(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		mutate func(*templates.Contract)
		want   string
	}{
		{"uppercase key", "Bad", func(*templates.Contract) {}, "must match"},
		{"empty key", "", func(*templates.Contract) {}, "must match"},
		{"key with a space", "a b", func(*templates.Contract) {}, "must match"},
		{"too long key", strings.Repeat("a", 65), func(*templates.Contract) {}, "must match"},
		{"duplicate of a built-in", templates.UsecaseNotice, func(*templates.Contract) {}, "already registered"},
		{"no channels", "t.nochan", func(c *templates.Contract) { c.EnabledChannels = nil }, "no enabled channels"},
		{"no category", "t.nocat", func(c *templates.Contract) { c.Category = 0 }, "category"},
		{"no verification", "t.noverify", func(c *templates.Contract) { c.DefaultVerification = 0 }, "verification"},
		{"no guarantee", "t.noguar", func(c *templates.Contract) { c.DefaultGuarantee = 0 }, "guarantee"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid()
			tt.mutate(&c)
			err := templates.Register(tt.key, c)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Register(%q) = %v, want an error containing %q", tt.key, err, tt.want)
			}
		})
	}
}

// TestGetUnknown proves an unregistered key is an error, never a zero contract.
func TestGetUnknown(t *testing.T) {
	if _, err := templates.Get("no.such.usecase"); err == nil {
		t.Fatal("Get of an unknown usecase must fail")
	}
}

const typedKey = "t.typed"

func init() {
	c := valid()
	// pb.Destination has buf.validate rules (destination_ref and usecase are
	// min_len 1, and channel must be Slack), so it stands in for any typed param
	// schema.
	c.NewParams = func() proto.Message { return &pb.Destination{} }
	c.Renderers = map[templates.ChannelVendor]templates.Renderer{
		{Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP}: func(p proto.Message) (*pb.RenderedPayload, error) {
			d := p.(*pb.Destination)
			if d.GetDestinationRef() == "boom" {
				return nil, errors.New("renderer failed")
			}
			return &pb.RenderedPayload{EmailSubject: "to " + d.GetDestinationRef(), EmailHtml: "<p>x</p>"}, nil
		},
	}
	templates.MustRegister(typedKey, c)
}

func params(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("params: %v", err)
	}
	return s
}

// TestValidateParams proves the schema check: free-form usecases accept any
// object; a typed one rejects unknown fields and rule violations.
func TestValidateParams(t *testing.T) {
	tests := []struct {
		name    string
		usecase string
		params  map[string]any
		wantErr bool
	}{
		{"free-form accepts anything", templates.UsecaseNotice, map[string]any{"anything": 1, "nested": map[string]any{"a": "b"}}, false},
		{"free-form accepts nil", templates.UsecaseNotice, nil, false},
		{"typed accepts a valid object", typedKey, map[string]any{"destination_ref": "C1", "usecase": "x", "channel": "CHANNEL_SLACK"}, false},
		{"typed rejects an unknown field", typedKey, map[string]any{"destination_ref": "C1", "typo": "x"}, true},
		{"typed rejects a rule violation", typedKey, map[string]any{"destination_ref": ""}, true},
		{"typed rejects a wrong type", typedKey, map[string]any{"destination_ref": 7}, true},
		{"unknown usecase", "no.such", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p *structpb.Struct
			if tt.params != nil {
				p = params(t, tt.params)
			}
			if err := templates.ValidateParams(tt.usecase, p); (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestRender proves the typed path: the renderer for (channel, vendor) runs over
// the validated params, and every failure is an error.
func TestRender(t *testing.T) {
	tests := []struct {
		name    string
		usecase string
		vendor  pb.Vendor
		params  map[string]any
		want    string
		wantErr string
	}{
		{"renders", typedKey, pb.Vendor_VENDOR_SMTP, map[string]any{"destination_ref": "C1", "usecase": "x", "channel": "CHANNEL_SLACK"}, "to C1", ""},
		{"no renderer for the vendor", typedKey, pb.Vendor_VENDOR_SES, map[string]any{"destination_ref": "C1", "usecase": "x", "channel": "CHANNEL_SLACK"}, "", "no renderer"},
		{"no renderer at all (content-only usecase)", templates.UsecaseDigest, pb.Vendor_VENDOR_SMTP, map[string]any{}, "", "carries no content"},
		{"params fail the schema", typedKey, pb.Vendor_VENDOR_SMTP, map[string]any{"destination_ref": ""}, "", "validation"},
		{"renderer error", typedKey, pb.Vendor_VENDOR_SMTP, map[string]any{"destination_ref": "boom", "usecase": "x", "channel": "CHANNEL_SLACK"}, "", "renderer failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := templates.Render(tt.usecase, pb.Channel_CHANNEL_EMAIL, tt.vendor, params(t, tt.params))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || out.GetEmailSubject() != tt.want {
				t.Fatalf("Render = %v, %v; want subject %q", out, err, tt.want)
			}
		})
	}
}

// TestKeysSorted proves Keys lists the registry in order.
func TestKeysSorted(t *testing.T) {
	keys := templates.Keys()
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			t.Fatalf("Keys not sorted: %v", keys)
		}
	}
}

func TestRegisterRejectsBadRetryOverride(t *testing.T) {
	for name, o := range map[string]*templates.RetryPolicy{
		"negative cap":  {MaxAttempts: -1},
		"zero step":     {Ladder: []time.Duration{time.Minute, 0}},
		"negative step": {Ladder: []time.Duration{-time.Second}},
	} {
		c := valid()
		c.RetryOverride = o
		if err := templates.Register("t.badretry."+strings.ReplaceAll(name, " ", "-"), c); err == nil {
			t.Errorf("%s: Register must fail", name)
		}
	}
}
