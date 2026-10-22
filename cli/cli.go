// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package cli is the outbox-comms command line: serve, migrate, config, health,
// version and the admin commands. The stock binary is one call to Main. A
// binary of your own registers its usecases with templates.Register, then calls
// Main, and every command, admin render included, sees those usecases.
package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/tetriz-ai/outbox-comms/config"
	"github.com/tetriz-ai/outbox-comms/internal/admin"
	"github.com/tetriz-ai/outbox-comms/internal/migrations"
	"github.com/tetriz-ai/outbox-comms/server"
)

// Version is printed by the version command. Release builds set it to the git
// tag, such as v0.1.0, with -ldflags "-X github.com/tetriz-ai/outbox-comms/cli.Version=...":
// goreleaser for the binaries and the Dockerfile's VERSION for the image, so
// both print the same string. Left empty, it comes from the build info, whose
// module versions carry the same leading v.
var Version = ""

const usage = `outbox-comms is a gRPC service that stores each email and Slack send in Postgres first,
then sends, retries and tracks it.

Usage:
  outbox-comms serve            run the API, the workers and the scheduler
  outbox-comms migrate          apply database migrations and exit
  outbox-comms config           list the environment variables and their defaults
  outbox-comms health [URL]     exit 0 if the service's /healthz answers 200 (for container health checks)
  outbox-comms admin <command>  operator commands (run "outbox-comms admin" for the list)
  outbox-comms version          print the version

Configuration comes from OUTBOX_COMMS_* environment variables.
`

// commandUsage is each command's -h text. Asking for it runs nothing and
// connects to nothing.
var commandUsage = map[string]string{
	"serve":   "Usage: outbox-comms serve\n\nRuns the API, the workers and the scheduler. Configuration comes from\nOUTBOX_COMMS_* environment variables; \"outbox-comms config\" lists them.\n",
	"migrate": "Usage: outbox-comms migrate\n\nApplies the database migrations to OUTBOX_COMMS_DATABASE_URL and exits.\n",
	"config":  "Usage: outbox-comms config\n\nLists every OUTBOX_COMMS_* environment variable with its default.\n",
	"version": "Usage: outbox-comms version\n\nPrints the version.\n",
	"health":  "Usage: outbox-comms health [URL]\n\nExits 0 if URL answers 200. Without URL it checks /healthz on 127.0.0.1 at the\nport of OUTBOX_COMMS_HTTP_ADDR. It exists for container health checks.\n",
}

// Main runs one command line, args without the program name, and returns the
// exit code: 0 on success, 1 on a failure and 2 on a usage error. It writes to
// standard output and standard error.
func Main(ctx context.Context, args []string) int {
	return MainWithOptions(ctx, args, server.Options{})
}

// MainWithOptions is Main for a binary that passes server.Options to serve:
// its own org gate, comms gate, Slack token provider or auth-failure hook.
// Every other command ignores opts.
func MainWithOptions(ctx context.Context, args []string, opts server.Options) int {
	return run(ctx, args, opts, os.Stdout, os.Stderr)
}

// awsConfigFooter names the AWS SDK's own variables, which the SES, SQS and S3
// adapters read and the list above does not.
const awsConfigFooter = `
SES, SQS and S3 read the AWS SDK's own variables, only when they are configured:
AWS_REGION
    The region (or AWS_DEFAULT_REGION)
AWS_ENDPOINT_URL_SES, AWS_ENDPOINT_URL_SQS, AWS_ENDPOINT_URL_S3
    A different endpoint for one service, such as a local stand-in (AWS_ENDPOINT_URL for all)
Credentials
    The SDK's default chain: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, a profile, web identity, or the container or instance role
`

// runServer is server.Run. A test replaces it to see the options serve passes.
var runServer = server.Run

// isHelp reports a help flag in any of the spellings Go's flag package takes.
// Help wins over whatever follows it, at the top level and after a command.
func isHelp(a string) bool { return a == "-h" || a == "-help" || a == "--help" }

// maxArgs is how many arguments each command takes after its name.
var maxArgs = map[string]int{"serve": 0, "migrate": 0, "config": 0, "version": 0, "health": 1}

func run(ctx context.Context, args []string, opts server.Options, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) || args[0] == "help" {
		fmt.Fprint(stdout, usage)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	if help, ok := commandUsage[args[0]]; ok && len(args) > 1 && isHelp(args[1]) {
		fmt.Fprint(stdout, help)
		return 0
	}
	if n, ok := maxArgs[args[0]]; ok && len(args)-1 > n {
		fmt.Fprintf(stderr, "%s: unexpected argument %q\n\n%s", args[0], args[1+n], commandUsage[args[0]])
		return 2
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, buildVersion())
		return 0
	case "config":
		for _, v := range config.Vars {
			fmt.Fprintf(stdout, "%s=%s\n    %s\n", v.Name, v.Default, v.Help)
		}
		fmt.Fprint(stdout, awsConfigFooter)
		return 0
	case "serve", "migrate":
		cfg, err := config.Load()
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		setupLogging(stderr, cfg.LogLevel)
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		if args[0] == "migrate" {
			return runMigrate(ctx, cfg, stdout, stderr)
		}
		if err := runServer(ctx, cfg, opts); err != nil {
			slog.Error("outbox-comms failed", "error", err)
			return 1
		}
		return 0
	case "health":
		return health(ctx, args[1:], stderr)
	case "admin":
		return admin.Main(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

// health GETs the service's /healthz, at the URL given or at the configured HTTP
// address on localhost, and exits 0 on a 200. It exists for container health
// checks in an image with no shell or curl.
func health(ctx context.Context, args []string, stderr io.Writer) int {
	url := ""
	if len(args) > 0 {
		url = args[0]
	} else {
		cfg, err := config.Load()
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		_, port, err := net.SplitHostPort(cfg.HTTPAddr)
		if err != nil {
			fmt.Fprintln(stderr, "OUTBOX_COMMS_HTTP_ADDR:", err)
			return 1
		}
		url = "http://127.0.0.1:" + port + "/healthz"
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(stderr, "health:", resp.Status)
		return 1
	}
	return 0
}

func runMigrate(ctx context.Context, cfg config.Config, stdout, stderr io.Writer) int {
	db, err := server.OpenDB(cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	applied, err := migrations.Up(ctx, db)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "applied %d migration(s) %v\n", len(applied), applied)
	return 0
}

func setupLogging(w io.Writer, level string) {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	slog.SetDefault(slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})))
}

// modulePath is this module, looked up in the build info of a binary that
// imports it.
const modulePath = "github.com/tetriz-ai/outbox-comms"

func buildVersion() string {
	if Version != "" {
		return Version
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	if bi.Main.Path != modulePath {
		for _, d := range bi.Deps {
			if d.Path == modulePath && d.Version != "" {
				return d.Version
			}
		}
	}
	if bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "(devel)"
}
