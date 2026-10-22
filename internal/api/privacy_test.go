// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package api_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/api"
	"github.com/tetriz-ai/outbox-comms/templates"
)

func TestRejectedRecipientIsNotLogged(t *testing.T) {
	var logs bytes.Buffer
	svc := api.NewService(api.Deps{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	recipient := "Synthetic Person <synthetic@example.com>"
	_, err := svc.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: testOrg, IdempotencyKey: "privacy-test", Usecase: templates.UsecaseNotice,
		Entries: []*pb.CommsEntry{emailEntry(recipient)},
	})
	if err == nil {
		t.Fatal("display-name recipient accepted")
	}
	if !strings.Contains(logs.String(), "bare email address") {
		t.Fatalf("missing rejection: %s", logs.String())
	}
	if strings.Contains(logs.String(), "synthetic@example.com") || strings.Contains(err.Error(), "synthetic@example.com") {
		t.Fatalf("recipient leaked in rejection: %v; logs %s", err, logs.String())
	}
}
