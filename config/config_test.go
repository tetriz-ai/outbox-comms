// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package config

import (
	"strings"
	"testing"
	"time"
)

func from(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

// TestDefaults pins the defaults a stranger gets with nothing set.
func TestDefaults(t *testing.T) {
	c, err := LoadFrom(from(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Queue != "asynq" || c.Email != "smtp" || c.Attachments != "dir" || c.GRPCAddr != "127.0.0.1:9090" || c.HTTPAddr != "127.0.0.1:8080" || c.GRPCReflection {
		t.Fatalf("transport defaults: %+v", c)
	}
	want := []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour}
	if len(c.RetryLadder) != len(want) || c.RetryMaxAttempts != 6 {
		t.Fatalf("retry defaults: %v %d", c.RetryLadder, c.RetryMaxAttempts)
	}
	for i := range want {
		if c.RetryLadder[i] != want[i] {
			t.Fatalf("ladder[%d] = %v", i, c.RetryLadder[i])
		}
	}
	if c.ReconcileCron != "*/10 * * * *" || c.PruneCron != "0 0 * * *" || c.PrunePayloadAfter != 90*24*time.Hour {
		t.Fatalf("job defaults: %+v", c)
	}
	if len(Vars) == 0 || !strings.HasPrefix(Vars[0].Name, "OUTBOX_COMMS_") {
		t.Fatal("Vars must list the variables")
	}
}

func TestParsesValues(t *testing.T) {
	c, err := LoadFrom(from(map[string]string{
		"OUTBOX_COMMS_QUEUE": "SQS", "OUTBOX_COMMS_SQS_REQUESTS_URL": "r", "OUTBOX_COMMS_SQS_DISPATCH_URL": "d",
		"OUTBOX_COMMS_SLACK_ORG_TOKENS": "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f=tok-a, 2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c=tok-b", "OUTBOX_COMMS_RETRY_LADDER": "10s, 1m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Queue != "sqs" || c.SlackOrgTokens["2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"] != "tok-b" || len(c.RetryLadder) != 2 || c.RetryLadder[0] != 10*time.Second {
		t.Fatalf("%+v", c)
	}
}

// TestRejects proves a malformed value is an error naming the variable, never a
// silent default.
func TestRejects(t *testing.T) {
	tests := map[string]string{
		"OUTBOX_COMMS_QUEUE":                "kafka",
		"OUTBOX_COMMS_EMAIL":                "pigeon",
		"OUTBOX_COMMS_REDIS_DB":             "one",
		"OUTBOX_COMMS_RETRY_LADDER":         "1m,soon",
		"OUTBOX_COMMS_RETRY_MAX_ATTEMPTS":   "0",
		"OUTBOX_COMMS_STUCK_SENDING_AFTER":  "-5m",
		"OUTBOX_COMMS_SLACK_ORG_TOKENS":     "org-without-token",
		"OUTBOX_COMMS_LOG_LEVEL":            "loud",
		"OUTBOX_COMMS_ATTACHMENTS":          "s3",
		"OUTBOX_COMMS_RECONCILE_CRON":       "every ten minutes",
		"OUTBOX_COMMS_PRUNE_CRON":           "61 * * * *",
		"OUTBOX_COMMS_REPUTATION_WINDOW":    "-1h",
		"OUTBOX_COMMS_OVERDUE_AFTER":        "soon",
		"OUTBOX_COMMS_REPUTATION_MIN_SENDS": "-1",
	}
	for k, v := range tests {
		t.Run(k, func(t *testing.T) {
			_, err := LoadFrom(from(map[string]string{k: v}))
			if err == nil || !strings.Contains(err.Error(), k) {
				t.Fatalf("want an error naming %s, got %v", k, err)
			}
		})
	}
	if _, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_QUEUE": "sqs"})); err == nil || !strings.Contains(err.Error(), "SQS_REQUESTS_URL") {
		t.Fatalf("sqs without URLs: %v", err)
	}
}

// TestCronAccepts pins the schedules the scheduler's parser takes: five-field
// cron and the @every and @daily forms.
func TestCronAccepts(t *testing.T) {
	for _, v := range []string{"*/10 * * * *", "@every 5s", "@daily"} {
		if _, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_RECONCILE_CRON": v, "OUTBOX_COMMS_PRUNE_CRON": v})); err != nil {
			t.Errorf("%q: %v", v, err)
		}
	}
}

// TestReputationVars pins the reputation and overdue settings and their
// defaults: a 24-hour window, 100 sends before the rates count, and 5 minutes
// past the retry time before a row is overdue.
func TestReputationVars(t *testing.T) {
	want := map[string]string{
		"OUTBOX_COMMS_REPUTATION_WINDOW":    "24h",
		"OUTBOX_COMMS_REPUTATION_MIN_SENDS": "100",
		"OUTBOX_COMMS_OVERDUE_AFTER":        "5m",
	}
	for _, v := range Vars {
		if d, ok := want[v.Name]; ok {
			if v.Default != d {
				t.Errorf("%s default = %q, want %q", v.Name, v.Default, d)
			}
			delete(want, v.Name)
		}
	}
	for name := range want {
		t.Errorf("%s is not a listed variable", name)
	}
}

// slackOrg is the organization the SLACK_ORG_TOKENS cases key their tokens by.
const slackOrg = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"

// TestRejectsMalformedSenders pins that settings a send would trip over stop the
// service at start, naming the variable: a From that is not an address, an SMTP
// relay without a port, a non-positive worker count, and a per-org Slack token
// keyed by something other than an organization UUID.
func TestRejectsMalformedSenders(t *testing.T) {
	for _, tt := range []struct{ name, value string }{
		{"OUTBOX_COMMS_EMAIL_FROM", "not an address"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:abc"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:99999"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:-1"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:0"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:"},
		{"OUTBOX_COMMS_SMTP_ADDR", ":25"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:65536"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:+25"},
		{"OUTBOX_COMMS_SMTP_ADDR", "mailhost:025"},
		{"OUTBOX_COMMS_SLACK_ORG_TOKENS", "=tok"},
		{"OUTBOX_COMMS_SQS_WORKERS", "-3"},
		{"OUTBOX_COMMS_WORKER_CONCURRENCY", "0"},
		{"OUTBOX_COMMS_SLACK_ORG_TOKENS", "example-org=xoxb-1"},
		{"OUTBOX_COMMS_SQS_WORKERS", "0"},
		{"OUTBOX_COMMS_WORKER_CONCURRENCY", "-3"},
		{"OUTBOX_COMMS_SLACK_ORG_TOKENS", slackOrg + "="},
		{"OUTBOX_COMMS_SLACK_ORG_TOKENS", slackOrg + "=tok-a," + strings.ToUpper(slackOrg) + "=tok-b"},
		{"OUTBOX_COMMS_SLACK_ORG_TOKENS", slackOrg + "=tok-a," + slackOrg + "=tok-b"},
	} {
		t.Run(tt.name+"="+tt.value, func(t *testing.T) {
			_, err := LoadFrom(from(map[string]string{tt.name: tt.value}))
			if err == nil || !strings.Contains(err.Error(), tt.name) {
				t.Fatalf("%s=%q: want an error naming it, got %v", tt.name, tt.value, err)
			}
		})
	}
}

// TestSlackOrgTokenKeysAreCanonical pins that a token keyed by any UUID form
// uuid.Parse takes is stored under the lowercase hyphenated form, with the space
// around the key and the token trimmed.
func TestSlackOrgTokenKeysAreCanonical(t *testing.T) {
	for _, key := range []string{"8C4F6F0E-6A7B-4C1D-9E2F-3A4B5C6D7E8F", "{8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f}", "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f",
		"urn:uuid:8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f", "8c4f6f0e6a7b4c1d9e2f3a4b5c6d7e8f", " 8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f "} {
		t.Run(key, func(t *testing.T) {
			c, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_SLACK_ORG_TOKENS": key + "= tok-a "}))
			if err != nil {
				t.Fatal(err)
			}
			if got := c.SlackOrgTokens["8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"]; got != "tok-a" || len(c.SlackOrgTokens) != 1 {
				t.Fatalf("tokens = %v, want one entry under the lowercase key", c.SlackOrgTokens)
			}
		})
	}
}

// TestAcceptsSMTPAddrs pins the SMTP addresses on the accepted side of the
// port check: a name, an IPv4 and a bracketed IPv6 host, ports 1 and 65535.
func TestAcceptsSMTPAddrs(t *testing.T) {
	for _, addr := range []string{"mailhost:25", "127.0.0.1:1", "[::1]:25", "mailhost:65535"} {
		t.Run(addr, func(t *testing.T) {
			if _, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_SMTP_ADDR": addr})); err != nil {
				t.Fatalf("%s: %v", addr, err)
			}
		})
	}
}

// TestStuckSendingOutlastsTheSendDeadline pins the rule that keeps a slow send
// from being reconciled and sent again while it runs: the stuck-send threshold
// is at least 2.5 times the send deadline.
func TestStuckSendingOutlastsTheSendDeadline(t *testing.T) {
	for _, tc := range []struct {
		stuck, send string
		ok          bool
	}{
		{"2m", "", false},
		{"149s", "60s", false},
		{"150s", "60s", true},
		{"5m", "", true},
		{"15s", "6s", true},
		{"15s", "7s", false},
		// 600000h × 5 overflows int64 nanoseconds; the rule must still hold.
		{"5m", "600000h", false},
		{"2562047h", "1000000h", true},
	} {
		t.Run(tc.stuck+"/"+tc.send, func(t *testing.T) {
			env := map[string]string{"OUTBOX_COMMS_STUCK_SENDING_AFTER": tc.stuck}
			if tc.send != "" {
				env["OUTBOX_COMMS_SEND_TIMEOUT"] = tc.send
			}
			_, err := LoadFrom(from(env))
			if (err == nil) != tc.ok || (err != nil && !strings.Contains(err.Error(), "OUTBOX_COMMS_STUCK_SENDING_AFTER")) {
				t.Fatalf("stuck %s, send timeout %q: %v; want accepted=%v", tc.stuck, tc.send, err, tc.ok)
			}
		})
	}
}

func TestReflectionSetting(t *testing.T) {
	c, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_GRPC_REFLECTION": "true"}))
	if err != nil || !c.GRPCReflection {
		t.Fatalf("reflection opt-in: %v, %v", c.GRPCReflection, err)
	}
	if _, err := LoadFrom(from(map[string]string{"OUTBOX_COMMS_GRPC_REFLECTION": "typo"})); err == nil {
		t.Fatal("invalid boolean accepted")
	}
}
