// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	"github.com/tetriz-ai/outbox-comms/config"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"

	"github.com/tetriz-ai/outbox-comms/server"
)

func scrape(t *testing.T, httpAddr string) string {
	t.Helper()
	resp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// TestMetricsOnAPrivateRegistry pins /metrics with no Metrics option: the
// service's metrics and the Go and process metrics, from a registry of the
// service's own. Prometheus's global default registry gains none of them.
func TestMetricsOnAPrivateRegistry(t *testing.T) {
	e := start(t, nil)
	body := scrape(t, e.httpAddr)
	for _, want := range []string{"outbox_comms_bounce_rate", "outbox_comms_overdue_pending", "go_goroutines"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "outbox_comms_") {
			t.Fatalf("running the server put %s on the default registry", f.GetName())
		}
	}
}

// lockedBuffer is a bytes.Buffer safe for the server's concurrent log writes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestMetricsAndLoggerThroughOptions pins the two options: the metrics are
// registered on the registry given, which /metrics then serves alone, and the
// service and its scheduler log to the logger given.
func TestMetricsAndLoggerThroughOptions(t *testing.T) {
	reg := prometheus.NewRegistry()
	var logs lockedBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	e := startWithOptions(t, nil, server.Options{Metrics: reg, Logger: logger}, nil)
	body := scrape(t, e.httpAddr)
	if !strings.Contains(body, "outbox_comms_bounce_rate") {
		t.Errorf("/metrics on the given registry lacks outbox_comms_bounce_rate:\n%s", body)
	}
	if strings.Contains(body, "go_goroutines") {
		t.Error("/metrics served the default registry, not the one given")
	}
	for _, want := range []string{`"msg":"outbox-comms started"`, `"msg":"scheduler: worker started"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the given logger has no %s line; it has:\n%s", want, logs.String())
		}
	}
}

func TestGRPCServerOptions(t *testing.T) {
	denied := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "synthetic auth gate")
	}
	e := startWithOptions(t, nil, server.Options{GRPCServerOptions: []grpc.ServerOption{grpc.UnaryInterceptor(denied)}}, nil)
	_, err := e.client.GetCommsStatus(context.Background(), &pb.GetCommsStatusRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("host interceptor bypassed: %v", err)
	}
}

func TestReflectionOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			e := start(t, func(c *config.Config) { c.GRPCReflection = enabled })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := reflectionpb.NewServerReflectionClient(e.conn).ServerReflectionInfo(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_ = stream.Send(&reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""}})
			response, err := stream.Recv()
			if enabled {
				if err != nil || len(response.GetListServicesResponse().GetService()) == 0 {
					t.Fatalf("reflection unavailable: %v, %v", response, err)
				}
			} else if status.Code(err) != codes.Unimplemented {
				t.Fatalf("reflection exposed by default: %v, %v", response, err)
			}
		})
	}
}
