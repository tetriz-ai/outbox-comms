// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package templates registers usecases and renders their content. Each usecase
// key maps to a Contract: its param schema, category, enabled channels, the
// verification and guarantee defaults, and optional renderers. A request renders
// its block content when it carries some, and the usecase's renderer otherwise.
package templates

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// ChannelVendor keys a renderer by its concrete (channel, vendor) pair: some
// vendors mandate a registered format, so the renderer is keyed on both.
type ChannelVendor struct {
	Channel pb.Channel
	Vendor  pb.Vendor
}

// Renderer turns a fully merged, validated typed param message into the channel
// artifact (email subject and HTML, or Slack Block Kit JSON).
type Renderer func(params proto.Message) (*pb.RenderedPayload, error)

// RetryPolicy is a per-usecase override of the global retry ladder. An empty
// Ladder or a zero MaxAttempts keeps the global value for that field.
type RetryPolicy struct {
	Ladder      []time.Duration
	MaxAttempts int
}

// Contract is the code-side contract for one usecase key.
type Contract struct {
	// NewParams returns an empty typed param exemplar for this usecase: the shared
	// schema validated at submit and decoded at render. nil accepts any params
	// object (a *structpb.Struct).
	NewParams func() proto.Message
	// Category drives the preference gate and the transactional bypass.
	Category pb.MessageCategory
	// EnabledChannels are the channels this usecase may target.
	EnabledChannels []pb.Channel
	// DefaultVerification is how far a send is tracked by default.
	DefaultVerification pb.VerificationLevel
	// DefaultGuarantee is the uncertain-send default.
	DefaultGuarantee pb.DeliveryGuarantee
	// RetryOverride optionally overrides the global ladder and attempt cap.
	RetryOverride *RetryPolicy
	// Renderers is the native renderer per (channel, vendor). A usecase whose
	// producers always send block content needs none.
	Renderers map[ChannelVendor]Renderer
}

// keyRE bounds a usecase key: it is a metric label and a column value, so it is
// short, lowercase and free of separators that would need escaping.
var keyRE = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

var (
	registryMu sync.RWMutex
	registry   = map[string]Contract{}
)

// Register adds a usecase to the process-wide registry. It rejects an invalid
// key, a duplicate key, a contract with no channels, and an unset category,
// verification level or guarantee, so a typo fails at start-up rather than at
// the first send.
func Register(key string, c Contract) error {
	if !keyRE.MatchString(key) {
		return fmt.Errorf("usecase key %q must match %s", key, keyRE)
	}
	if len(c.EnabledChannels) == 0 {
		return fmt.Errorf("usecase %q: no enabled channels", key)
	}
	if c.Category == pb.MessageCategory_MESSAGE_CATEGORY_UNSPECIFIED {
		return fmt.Errorf("usecase %q: category is unspecified", key)
	}
	if c.DefaultVerification == pb.VerificationLevel_VERIFICATION_LEVEL_UNSPECIFIED {
		return fmt.Errorf("usecase %q: default verification is unspecified", key)
	}
	if c.DefaultGuarantee == pb.DeliveryGuarantee_DELIVERY_GUARANTEE_UNSPECIFIED {
		return fmt.Errorf("usecase %q: default guarantee is unspecified", key)
	}
	if o := c.RetryOverride; o != nil {
		if o.MaxAttempts < 0 {
			return fmt.Errorf("usecase %q: retry override max attempts is negative", key)
		}
		for _, d := range o.Ladder {
			if d <= 0 {
				return fmt.Errorf("usecase %q: retry override ladder step %v is not positive", key, d)
			}
		}
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[key]; dup {
		return fmt.Errorf("usecase %q is already registered", key)
	}
	registry[key] = c
	return nil
}

// MustRegister is Register for package init and main: it panics on error.
func MustRegister(key string, c Contract) {
	if err := Register(key, c); err != nil {
		panic(err)
	}
}

// Get returns the contract for a usecase, or an error if it is not registered.
func Get(usecase string) (Contract, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := registry[usecase]
	if !ok {
		return Contract{}, fmt.Errorf("no contract for usecase %q", usecase)
	}
	return c, nil
}

// Keys returns the registered usecase keys, sorted.
func Keys() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ChannelEnabled reports whether the usecase may target the channel.
func (c Contract) ChannelEnabled(ch pb.Channel) bool {
	for _, e := range c.EnabledChannels {
		if e == ch {
			return true
		}
	}
	return false
}

func (c Contract) newParams() proto.Message {
	if c.NewParams == nil {
		return &structpb.Struct{}
	}
	return c.NewParams()
}

// ValidateParams checks an incoming params struct against the usecase's typed
// schema: it must protojson-decode into the schema with no unknown fields and
// satisfy the schema's buf.validate constraints.
func ValidateParams(usecase string, params *structpb.Struct) error {
	c, err := Get(usecase)
	if err != nil {
		return fmt.Errorf("get contract: %w", err)
	}
	typed, err := decodeInto(c.newParams(), params)
	if err != nil {
		return fmt.Errorf("decode params: %w", err)
	}
	if err := protovalidate.Validate(typed); err != nil {
		return fmt.Errorf("params validation: %w", err)
	}
	return nil
}

// Render validates the merged params, decodes them into the usecase's typed
// schema and runs the (channel, vendor) renderer. merged is the entry's vars
// shallow-overlaid on the request's shared params. Slack blocks the renderer
// returns must be a JSON array of at most 50 objects, or Render returns an error.
func Render(usecase string, ch pb.Channel, vendor pb.Vendor, merged *structpb.Struct) (*pb.RenderedPayload, error) {
	c, err := Get(usecase)
	if err != nil {
		return nil, fmt.Errorf("get contract: %w", err)
	}
	r, ok := c.Renderers[ChannelVendor{Channel: ch, Vendor: vendor}]
	if !ok {
		return nil, fmt.Errorf("no renderer for usecase %s on (%s,%s), and the request carries no content", usecase, ch, vendor)
	}
	typed, err := decodeInto(c.newParams(), merged)
	if err != nil {
		return nil, fmt.Errorf("decode params: %w", err)
	}
	if err := protovalidate.Validate(typed); err != nil {
		return nil, fmt.Errorf("render params validation: %w", err)
	}
	rendered, err := r(typed)
	if err != nil {
		return nil, fmt.Errorf("run renderer: %w", err)
	}
	// Malformed blocks are the renderer's bug, so they stop here as a render
	// error, before any send, instead of failing at Slack or in the client.
	if err := checkSlackBlocks(rendered.GetSlackBlocksJson()); err != nil {
		return nil, fmt.Errorf("run renderer: %w", err)
	}
	return rendered, nil
}

// checkSlackBlocks accepts an empty string (no blocks) or a JSON array of at
// most slackMaxBlocks objects, the shape chat.postMessage takes.
func checkSlackBlocks(b string) error {
	if b == "" {
		return nil
	}
	if !json.Valid([]byte(b)) {
		return fmt.Errorf("the Slack blocks are not valid JSON")
	}
	var blocks []json.RawMessage
	if t := bytes.TrimSpace([]byte(b)); len(t) == 0 || t[0] != '[' || json.Unmarshal(t, &blocks) != nil {
		return fmt.Errorf("the Slack blocks are not a JSON array")
	}
	if len(blocks) > slackMaxBlocks {
		return fmt.Errorf("the renderer returned %d Slack blocks, over Slack's limit of %d", len(blocks), slackMaxBlocks)
	}
	for i, blk := range blocks {
		if t := bytes.TrimSpace(blk); len(t) == 0 || t[0] != '{' {
			return fmt.Errorf("the Slack block at index %d is not a JSON object", i)
		}
	}
	return nil
}

// decodeInto protojson-decodes a struct into the typed message, rejecting unknown
// fields so a typo'd or stray param fails fast.
func decodeInto(dst proto.Message, params *structpb.Struct) (proto.Message, error) {
	if params == nil {
		params = &structpb.Struct{}
	}
	b, err := protojson.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(b, dst); err != nil {
		return nil, fmt.Errorf("unknown or malformed param for schema: %w", err)
	}
	return dst, nil
}
