// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"html"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
)

// The built-in usecases. Each takes block content from the producer (the request's
// or the entry's MessageContent) and accepts any params object, so a producer can
// send through a stock binary without writing Go. A usecase that needs a typed
// schema or a code-side renderer is registered with Register.
const (
	// UsecaseNotice is a transactional message: it bypasses the preference gate.
	UsecaseNotice = "notice"
	// UsecaseAlert is an alert-category message: on by default, opt-out-able.
	UsecaseAlert = "alert"
	// UsecaseDigest is a product-category message sent best-effort: an uncertain
	// send is not retried, so one request never sends the same digest twice.
	UsecaseDigest = "digest"
)

func init() {
	both := []pb.Channel{pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK}
	MustRegister(UsecaseNotice, Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     both,
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	})
	MustRegister(UsecaseAlert, Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_ALERT,
		EnabledChannels:     both,
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	})
	MustRegister(UsecaseDigest, Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT,
		EnabledChannels:     both,
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT,
	})
}

// greeting returns a personalized HTML greeting, degrading to a generic one when
// the per-recipient name is absent.
func greeting(name string) string {
	if name == "" {
		return "Hi there"
	}
	return "Hi " + html.EscapeString(name)
}

// greetingText is greeting's plain-text counterpart: the name rides unescaped,
// since the text part has no markup to break out of.
func greetingText(name string) string {
	if name == "" {
		return "Hi there"
	}
	return "Hi " + name
}
