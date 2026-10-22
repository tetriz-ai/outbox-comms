// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package admin implements the `outbox-comms admin` operator commands. Each one
// reads by default and changes nothing until it is given --execute (or, for
// send, --mode). They read the same OUTBOX_COMMS_* configuration as the service.
package admin

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/tetriz-ai/outbox-comms/config"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/server"
)

const usage = `Usage: outbox-comms admin <command> [flags]

Commands:
  redrive-dead  list DEAD messages; with --execute, reset them to QUEUED
  unsuppress    show an address's suppression; with --execute, lift it
  render        render a usecase for one channel; with --send, deliver it to --to
  send          send operator-written content to a recipient list, gated on a
                preview that reviewers received

Run "outbox-comms admin <command> -h" for a command's flags.
`

// env is what a command needs from the process: its configuration and, for the
// commands that touch the database, the DAOs. A test supplies a fake.
type env struct {
	cfg config.Config
	// loadCfg, when set, fills cfg when a command calls ready.
	loadCfg func() (config.Config, error)
	stdout  io.Writer
	stderr  io.Writer
	// openDAOs opens the database lazily, so render needs no database.
	openDAOs func() (*daos, func(), error)
}

type daos struct {
	reconcile   dao.IReconcileDAO
	suppression dao.ISuppressionDAO
	request     dao.IRequestDAO
	message     dao.IMessageDAO
}

// Main runs one admin command and returns the process exit code: 0 on success,
// 1 on a failure, 2 on a usage error. The configuration is read only after the
// command's flags pass their checks.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	e := &env{loadCfg: config.Load, stdout: stdout, stderr: stderr}
	e.openDAOs = func() (*daos, func(), error) {
		db, err := server.OpenDB(e.cfg.DatabaseURL)
		if err != nil {
			return nil, func() {}, err
		}
		sqlDB, err := db.DB()
		if err != nil {
			return nil, func() {}, fmt.Errorf("database handle: %w", err)
		}
		return &daos{
			reconcile:   dao.NewReconcileRepo(db),
			suppression: dao.NewSuppressionRepo(db),
			request:     dao.NewRequestRepo(db),
			message:     dao.NewMessageRepo(db),
		}, func() { _ = sqlDB.Close() }, nil
	}
	return e.run(ctx, args)
}

func (e *env) run(ctx context.Context, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "-help" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(e.stdout, usage)
		if len(args) == 0 {
			return 2
		}
		return 0
	}
	var err error
	switch args[0] {
	case "redrive-dead":
		err = e.redriveDead(ctx, args[1:])
	case "unsuppress":
		err = e.unsuppress(ctx, args[1:])
	case "render":
		err = e.render(ctx, args[1:])
	case "send":
		err = e.send(ctx, args[1:])
	default:
		fmt.Fprintf(e.stderr, "unknown admin command %q\n\n%s", args[0], usage)
		return 2
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintln(e.stderr, err)
		return 2
	default:
		fmt.Fprintln(e.stderr, "error:", err)
		return 1
	}
}

// errUsage marks a flag or argument error (exit code 2).
var errUsage = errors.New("usage")

func usageErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, args...))
}

func (e *env) flags(name, synopsis string) *flag.FlagSet {
	fs := flag.NewFlagSet("outbox-comms admin "+name, flag.ContinueOnError)
	fs.SetOutput(e.stderr)
	fs.Usage = func() {
		fmt.Fprintf(e.stderr, "Usage: outbox-comms admin %s\n\nFlags:\n", synopsis)
		fmt.Fprint(e.stderr, doubleDash(fs))
	}
	return fs
}

// doubleDash is fs.PrintDefaults with each flag shown as --name, the form the
// synopsis and the README use. The flag package accepts both.
func doubleDash(fs *flag.FlagSet) string {
	var b strings.Builder
	out := fs.Output()
	fs.SetOutput(&b)
	fs.PrintDefaults()
	fs.SetOutput(out)
	lines := strings.Split(b.String(), "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "  -") {
			lines[i] = "  --" + l[3:]
		}
	}
	return strings.Join(lines, "\n")
}

func (e *env) parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageErr("%v", err)
	}
	if fs.NArg() > 0 {
		return usageErr("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

// ready loads the configuration. Each command calls it after its own flag
// checks, so -h and every usage error exit without a valid configuration.
func (e *env) ready() error {
	if e.loadCfg != nil {
		cfg, err := e.loadCfg()
		if err != nil {
			return err
		}
		e.cfg, e.loadCfg = cfg, nil
	}
	return nil
}

// maskEmail keeps the first character of the local part and the domain, so
// output in a shared terminal does not carry full addresses
// (jane@example.com prints as j***@example.com).
func maskEmail(t string) string {
	at := strings.IndexByte(t, '@')
	if at <= 0 {
		if t == "" {
			return ""
		}
		return t[:1] + "***"
	}
	return t[:1] + "***" + t[at:]
}

// redriveDead lists DEAD rows matching the filters and, with --execute, resets
// them to QUEUED so reconcile re-dispatches them. Run it only after the cause is
// fixed: a render bug or a bad channel re-driven unchanged goes DEAD again.
func (e *env) redriveDead(ctx context.Context, args []string) error {
	fs := e.flags("redrive-dead", "redrive-dead [--org UUID] [--reason REASON] [--limit N] [--execute]")
	org := fs.String("org", "", "only this organization")
	reason := fs.String("reason", "", "only this dead_reason (e.g. transient_exhausted)")
	limit := fs.Int("limit", 100, "at most this many rows")
	execute := fs.Bool("execute", false, "reset the listed rows to QUEUED")
	if err := e.parse(fs, args); err != nil {
		return err
	}
	if *limit <= 0 {
		return usageErr("--limit must be a positive integer, got %d", *limit)
	}
	if err := e.ready(); err != nil {
		return err
	}
	d, closeDB, err := e.openDAOs()
	if err != nil {
		return err
	}
	defer closeDB()
	rows, err := d.reconcile.ListDead(ctx, *org, *reason, *limit)
	if err != nil {
		return fmt.Errorf("list dead rows: %w", err)
	}
	fmt.Fprintf(e.stdout, "%d DEAD message(s) match\n", len(rows))
	for _, m := range rows {
		r := ""
		if m.DeadReason != nil {
			r = *m.DeadReason
		}
		fmt.Fprintf(e.stdout, "  %s  org=%s  to=%s  reason=%s\n", m.ID, m.OrganizationID, maskEmail(m.Recipient), r)
	}
	if !*execute {
		fmt.Fprintln(e.stdout, "read only: pass --execute to reset them to QUEUED (after fixing the cause)")
		return nil
	}
	ids := make([]string, len(rows))
	for i, m := range rows {
		ids[i] = m.ID
	}
	n, err := d.reconcile.RedriveDead(ctx, ids)
	if err != nil {
		return fmt.Errorf("redrive dead rows: %w", err)
	}
	fmt.Fprintf(e.stdout, "reset %d message(s) to QUEUED; reconcile will dispatch them\n", n)
	return nil
}

// unsuppress shows the live suppression for one exact address and, with
// --execute, soft-deletes it so the next send proceeds. It never matches a
// pattern or acts in bulk.
func (e *env) unsuppress(ctx context.Context, args []string) error {
	fs := e.flags("unsuppress", "unsuppress --email ADDRESS [--execute]")
	email := fs.String("email", "", "the exact address (required)")
	execute := fs.Bool("execute", false, "lift the suppression")
	if err := e.parse(fs, args); err != nil {
		return err
	}
	addr := strings.TrimSpace(*email)
	if addr == "" {
		return usageErr("--email is required")
	}
	if err := e.ready(); err != nil {
		return err
	}
	d, closeDB, err := e.openDAOs()
	if err != nil {
		return err
	}
	defer closeDB()
	sup, err := d.suppression.GetActiveByEmail(ctx, addr)
	if err != nil {
		return fmt.Errorf("look up suppression for %s: %w", maskEmail(addr), err)
	}
	if sup == nil {
		fmt.Fprintf(e.stdout, "%s is not suppressed\n", maskEmail(addr))
		return nil
	}
	fmt.Fprintf(e.stdout, "%s is suppressed: reason=%s since=%s\n", maskEmail(addr), sup.Reason, sup.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"))
	if !*execute {
		fmt.Fprintln(e.stdout, "read only: pass --execute to lift it")
		return nil
	}
	if err := d.suppression.Deactivate(ctx, addr); err != nil {
		return fmt.Errorf("lift suppression for %s: %w", maskEmail(addr), err)
	}
	fmt.Fprintf(e.stdout, "lifted; the next send to %s will proceed\n", maskEmail(addr))
	return nil
}
