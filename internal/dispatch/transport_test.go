// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dispatch_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/slackclient"
)

// resetAfterRead accepts connections, reads one whole HTTP request from each,
// counts it, and resets the connection without answering: the provider got the
// request, and the sender never learns what it did with it.
func resetAfterRead(t *testing.T) (addr string, calls *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	calls = &atomic.Int32{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			r := bufio.NewReader(conn)
			length := 0
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					break
				}
				if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && strings.EqualFold(k, "Content-Length") {
					length, _ = strconv.Atoi(strings.TrimSpace(v))
				}
				if line == "\r\n" {
					_, _ = io.CopyN(io.Discard, r, int64(length))
					calls.Add(1)
					break
				}
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetLinger(0) // close with a reset
			}
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), calls
}

// closedAddr is an address nothing listens on: a dial there fails before any
// byte of the request is written.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func sesSender(addr string) dispatch.Sender {
	client := sender.NewSESClient(aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://" + addr),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		}),
	})
	return sender.NewEmailSender(client, nil, "noreply@example.com", "")
}

func slackSender(t *testing.T, addr string) dispatch.Sender {
	t.Helper()
	c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, "http://"+addr+"/api")
	if err != nil {
		t.Fatal(err)
	}
	return sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, nil)
}

// TestTransportFailureAfterTheRequestLeft pins at-most-once for BEST_EFFORT
// over a real SES client and a real Slack client: a provider that read the
// request and then reset the connection gets exactly one call, a BEST_EFFORT
// message ends UNCONFIRMED(uncertain_send), and a GUARANTEED one goes FAILED
// for a retry. A dial failure sends nothing, so it is a plain retry either way.
func TestTransportFailureAfterTheRequestLeft(t *testing.T) {
	type sendFn func(t *testing.T, addr string) dispatch.Sender
	for _, ch := range []struct {
		name   string
		ch     pb.Channel
		vendor pb.Vendor
		send   sendFn
	}{
		{"ses", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, func(_ *testing.T, addr string) dispatch.Sender { return sesSender(addr) }},
		{"slack", pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, slackSender},
	} {
		for _, tt := range []struct {
			name       string
			guarantee  pb.DeliveryGuarantee
			reset      bool
			wantStatus pb.MessageStatus
		}{
			{"reset/best_effort", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, true, pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED},
			{"reset/guaranteed", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, true, pb.MessageStatus_MESSAGE_STATUS_FAILED},
			{"dial_failure/best_effort", pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, false, pb.MessageStatus_MESSAGE_STATUS_FAILED},
		} {
			t.Run(ch.name+"/"+tt.name, func(t *testing.T) {
				var addr string
				var calls *atomic.Int32
				if tt.reset {
					addr, calls = resetAfterRead(t)
				} else {
					addr = closedAddr(t)
				}
				f := newFixture(t)
				id := f.seedQueued(t, "k-transport-"+ch.name+"-"+tt.name, ch.ch, ch.vendor, tt.guarantee, typedUsecase)
				f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
				f.tx.Exec(`UPDATE comms_messages SET recipient = 'C0TRANSPORT', slack_delivery = 'SLACK_DELIVERY_LINK' WHERE id = ? AND channel = 'CHANNEL_SLACK'`, id)
				d := f.dispatcher(ch.send(t, addr), ch.ch)
				if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
					t.Fatal(err)
				}
				got := f.status(t, id)
				if got.Status != tt.wantStatus {
					t.Fatalf("status = %v, want %v", got.Status, tt.wantStatus)
				}
				if tt.wantStatus == pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED && (got.UnconfirmedReason == nil || *got.UnconfirmedReason != "uncertain_send") {
					t.Fatalf("unconfirmed_reason = %v, want uncertain_send", got.UnconfirmedReason)
				}
				if calls != nil && calls.Load() != 1 {
					t.Fatalf("the provider got %d calls, want exactly 1", calls.Load())
				}
			})
		}
	}
}
