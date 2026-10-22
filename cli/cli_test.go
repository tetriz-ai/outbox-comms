// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tetriz-ai/outbox-comms/config"
	"github.com/tetriz-ai/outbox-comms/gate"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/server"
)

// TestCommands pins the top-level commands' output and exit codes. The help
// text is compared whole so it cannot drift from what users see.
func TestCommands(t *testing.T) {
	tests := []struct {
		args    []string
		code    int
		stdout  string
		stderr  string
		exactly bool
	}{
		{nil, 2, usage, "", true},
		{[]string{"--help"}, 0, usage, "", true},
		{[]string{"version"}, 0, "(devel)\n", "", true},
		{[]string{"config"}, 0, "OUTBOX_COMMS_DATABASE_URL=postgres://", "", false},
		{[]string{"bogus"}, 2, "", `unknown command "bogus"`, false},
		{[]string{"admin"}, 2, "Commands:", "", false},
		{[]string{"health", "http://127.0.0.1:1/healthz"}, 1, "", "", false},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := run(context.Background(), tt.args, server.Options{}, &out, &errOut); code != tt.code {
				t.Fatalf("exit %d, want %d (stderr %q)", code, tt.code, errOut.String())
			}
			if tt.exactly && out.String() != tt.stdout {
				t.Fatalf("stdout = %q, want %q", out.String(), tt.stdout)
			}
			if !strings.Contains(out.String(), tt.stdout) || !strings.Contains(errOut.String(), tt.stderr) {
				t.Fatalf("stdout %q / stderr %q", out.String(), errOut.String())
			}
		})
	}
}

func TestBadConfigFails(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_QUEUE", "kafka")
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"serve"}, server.Options{}, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "OUTBOX_COMMS_QUEUE") {
		t.Fatalf("exit %d, stderr %q", code, errOut.String())
	}
}

func TestHealth(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"health", ok.URL}, server.Options{}, &out, &errOut); code != 0 {
		t.Fatalf("healthy: exit %d %s", code, errOut.String())
	}
	if code := run(context.Background(), []string{"health", bad.URL}, server.Options{}, &out, &errOut); code != 1 || !strings.Contains(errOut.String(), "503") {
		t.Fatalf("unhealthy: exit %d %s", code, errOut.String())
	}
}

// TestSubcommandHelp pins that -h and --help after a command print that
// command's usage and exit 0 without connecting anywhere. The config points at
// closed ports, so a command that tried to connect would fail.
func TestSubcommandHelp(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_DATABASE_URL", "postgres://nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	t.Setenv("OUTBOX_COMMS_REDIS_ADDR", "127.0.0.1:1")
	t.Setenv("OUTBOX_COMMS_HTTP_ADDR", "127.0.0.1:1")
	for _, cmd := range []string{"serve", "migrate", "config", "version", "health"} {
		for _, flag := range []string{"-h", "-help", "--help"} {
			t.Run(cmd+" "+flag, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				var out, errOut bytes.Buffer
				code := run(ctx, []string{cmd, flag}, server.Options{}, &out, &errOut)
				if code != 0 || errOut.Len() != 0 {
					t.Fatalf("exit %d, stderr %q", code, errOut.String())
				}
				if !strings.HasPrefix(out.String(), "Usage: outbox-comms "+cmd) {
					t.Fatalf("stdout = %q, want the %s usage", out.String(), cmd)
				}
			})
		}
	}
}

// TestHealthNeedsA200 pins the health check: only a 200 passes. A 404, or a 302
// with nowhere to go, fails with exit 1.
func TestHealthNeedsA200(t *testing.T) {
	for _, tt := range []struct {
		code int
		want int
	}{{http.StatusOK, 0}, {http.StatusNotFound, 1}, {http.StatusFound, 1}} {
		t.Run(http.StatusText(tt.code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(tt.code) }))
			t.Cleanup(srv.Close)
			var out, errOut bytes.Buffer
			if code := run(context.Background(), []string{"health", srv.URL + "/healthz"}, server.Options{}, &out, &errOut); code != tt.want {
				t.Fatalf("exit %d, want %d (stderr %q)", code, tt.want, errOut.String())
			}
		})
	}
}

// TestHelpCommand pins `help`: the usage on stdout, exit 0.
func TestHelpCommand(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"help"}, server.Options{}, &out, &errOut); code != 0 || out.String() != usage {
		t.Fatalf("exit %d, stdout %q", code, out.String())
	}
}

// TestExtraArgumentsAreUsageErrors pins that a command given more arguments
// than it takes exits 2 with a usage error, before reading any configuration.
func TestExtraArgumentsAreUsageErrors(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_SMTP_ADDR", "not-an-address")
	for _, args := range [][]string{
		{"serve", "now"}, {"migrate", "up"}, {"config", "list"}, {"version", "-v"},
		{"health", "http://127.0.0.1:1/healthz", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			code := run(context.Background(), args, server.Options{}, &out, &errOut)
			if code != 2 || !strings.Contains(errOut.String(), "unexpected argument") || out.Len() != 0 {
				t.Fatalf("exit %d, stdout %q, stderr %q; want 2 and a usage error", code, out.String(), errOut.String())
			}
		})
	}
}

// TestAdminHelpNeedsNoConfig pins that admin help, and each admin command's
// -h, print usage and exit 0 even when the configuration does not load.
func TestAdminHelpNeedsNoConfig(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_SMTP_ADDR", "not-an-address")
	for _, args := range [][]string{
		{"admin", "-h"}, {"admin", "--help"},
		{"admin", "redrive-dead", "-h"}, {"admin", "unsuppress", "--help"},
		{"admin", "render", "-h"}, {"admin", "send", "-h"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			code := run(context.Background(), args, server.Options{}, &out, &errOut)
			if code != 0 || !strings.Contains(out.String()+errOut.String(), "Usage: outbox-comms admin") {
				t.Fatalf("exit %d, stdout %q, stderr %q; want 0 and the usage", code, out.String(), errOut.String())
			}
		})
	}
}

// optOrgGate, optCommsGate and optAuth are distinct pointers, so the test can
// tell each Options field arrived as the one passed in.
type optOrgGate struct{ gate.AllowAll }
type optCommsGate struct{ gate.AllowAll }
type optAuth struct{}

func (*optAuth) ReportGrantInvalid(context.Context, string, string) error { return nil }

// TestServeGetsTheOptions pins that MainWithOptions hands every server.Options
// field to serve unchanged.
func TestServeGetsTheOptions(t *testing.T) {
	prevRun, prevLog := runServer, slog.Default()
	t.Cleanup(func() { runServer = prevRun; slog.SetDefault(prevLog) })
	var got server.Options
	runServer = func(_ context.Context, _ config.Config, o server.Options) error {
		got = o
		return nil
	}
	want := server.Options{
		OrgGate:     &optOrgGate{},
		CommsGate:   &optCommsGate{},
		SlackTokens: &sender.StaticTokens{Default: "example-token"},
		AuthFailure: &optAuth{},
	}
	if code := MainWithOptions(context.Background(), []string{"serve"}, want); code != 0 {
		t.Fatalf("serve: exit %d", code)
	}
	if got.OrgGate != want.OrgGate {
		t.Errorf("OrgGate = %v, want the one passed in", got.OrgGate)
	}
	if got.CommsGate != want.CommsGate {
		t.Errorf("CommsGate = %v, want the one passed in", got.CommsGate)
	}
	if got.SlackTokens != want.SlackTokens {
		t.Errorf("SlackTokens = %v, want the one passed in", got.SlackTokens)
	}
	if got.AuthFailure != want.AuthFailure {
		t.Errorf("AuthFailure = %v, want the one passed in", got.AuthFailure)
	}
}

// TestHelpWinsOverWhatFollows pins one help rule at every level: -h, -help and
// --help print usage and exit 0, whatever arguments follow them.
func TestHelpWinsOverWhatFollows(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_SMTP_ADDR", "not-an-address")
	for _, flag := range []string{"-h", "-help", "--help"} {
		for _, args := range [][]string{
			{flag}, {flag, "extra"},
			{"serve", flag, "extra"}, {"health", flag, "a", "b"},
			{"admin", flag}, {"admin", flag, "extra"},
			{"admin", "render", flag, "extra"}, {"admin", "redrive-dead", flag},
		} {
			t.Run(strings.Join(args, " "), func(t *testing.T) {
				var out, errOut bytes.Buffer
				code := run(context.Background(), args, server.Options{}, &out, &errOut)
				if code != 0 || !strings.Contains(out.String()+errOut.String(), "Usage") {
					t.Fatalf("exit %d, stdout %q, stderr %q; want 0 and a usage", code, out.String(), errOut.String())
				}
			})
		}
	}
}

// TestAdminUsageErrorsNeedNoConfig pins that an admin usage error exits 2 even
// when the configuration does not load: an extra positional argument to each
// command, and each command's missing required flag.
func TestAdminUsageErrorsNeedNoConfig(t *testing.T) {
	t.Setenv("OUTBOX_COMMS_SMTP_ADDR", "not-an-address")
	for _, args := range [][]string{
		{"admin", "redrive-dead", "extra"},
		{"admin", "unsuppress", "--email", "a@example.com", "extra"},
		{"admin", "render", "--usecase", "notice", "--channel", "email", "extra"},
		{"admin", "send", "extra"},
		{"admin", "redrive-dead", "--limit", "0"},
		{"admin", "unsuppress"},
		{"admin", "render", "--usecase", "notice"},
		{"admin", "render", "--usecase", "notice", "--channel", "email", "--send"},
		{"admin", "send", "--usecase", "notice"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			code := run(context.Background(), args, server.Options{}, &out, &errOut)
			if code != 2 || strings.Contains(errOut.String(), "OUTBOX_COMMS_SMTP_ADDR") {
				t.Fatalf("exit %d, stderr %q; want 2 and a usage error, not the config error", code, errOut.String())
			}
		})
	}
}
