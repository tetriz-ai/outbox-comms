// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/tetriz-ai/outbox-comms/cli"
	"github.com/tetriz-ai/outbox-comms/gate"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/server"
	"github.com/tetriz-ai/outbox-comms/templates"
)

// TestMainRendersACustomUsecase pins the custom-binary path: a usecase
// registered in the program is rendered by cli.Main's admin render, through its
// own renderer, as the stock commands would render a built-in one.
func TestMainRendersACustomUsecase(t *testing.T) {
	templates.MustRegister("test.custom.receipt", templates.Contract{
		NewParams:           func() proto.Message { return &structpb.Struct{} },
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Renderers: map[templates.ChannelVendor]templates.Renderer{
			{Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP}: func(p proto.Message) (*pb.RenderedPayload, error) {
				n := p.(*structpb.Struct).GetFields()["number"].GetStringValue()
				return &pb.RenderedPayload{EmailSubject: "Receipt " + n, EmailHtml: "<p>Receipt " + n + "</p>"}, nil
			},
		},
	})
	out, code := captureStdout(t, func() int {
		return cli.Main(context.Background(), []string{"admin", "render", "--usecase", "test.custom.receipt", "--channel", "email", "--params", `{"number":"R-7"}`})
	})
	if code != 0 || !strings.Contains(out, "subject: Receipt R-7") || !strings.Contains(out, "<p>Receipt R-7</p>") {
		t.Fatalf("exit %d, stdout %q", code, out)
	}
}

// captureStdout runs fn with os.Stdout sent to a pipe and returns what it wrote.
func captureStdout(t *testing.T, fn func() int) (string, int) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	code := fn()
	os.Stdout = orig
	if err := w.Close(); err != nil {
		t.Fatal(fmt.Errorf("close pipe: %w", err))
	}
	return <-done, code
}

// TestMainWithOptions pins the entry point a binary with its own
// server.Options calls: it runs the same commands as Main.
func TestMainWithOptions(t *testing.T) {
	opts := server.Options{OrgGate: gate.AllowAll{}, CommsGate: gate.AllowAll{}}
	if code := cli.MainWithOptions(context.Background(), []string{"config", "-h"}, opts); code != 0 {
		t.Fatalf("config -h: exit %d, want 0", code)
	}
	if code := cli.MainWithOptions(context.Background(), []string{"bogus"}, opts); code != 2 {
		t.Fatalf("bogus: exit %d, want 2", code)
	}
}
