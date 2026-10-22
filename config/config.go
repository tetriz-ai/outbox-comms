// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package config reads the service's settings from OUTBOX_COMMS_* environment
// variables. Every setting has a local default, so `outbox-comms serve` against
// the compose file needs none set.
package config

import (
	"fmt"
	"net"
	"net/mail"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Config is the whole service configuration.
type Config struct {
	// Env names the deployment. `admin render --send` refuses to run when it is
	// "prod" or "production", including region suffixes, in any casing.
	Env string

	DatabaseURL string
	RedisAddr   string
	RedisDB     int
	RedisPass   string

	GRPCAddr       string
	HTTPAddr       string
	GRPCReflection bool

	// Queue is "asynq" (Redis) or "sqs".
	Queue             string
	SQSRequestsURL    string
	SQSDispatchURL    string
	SQSFeedbackURL    string
	QueueWorkers      int
	WorkerConcurrency int

	// Email is "smtp", "ses" or "none".
	Email               string
	EmailFrom           string
	SMTPAddr            string
	SMTPUsername        string
	SMTPPassword        string
	SESConfigurationSet string

	// Attachments is "dir" or "s3".
	Attachments    string
	AttachmentsDir string
	S3Bucket       string
	S3Prefix       string

	SlackAPIURL    string
	SlackBotToken  string
	SlackOrgTokens map[string]string

	RetryLadder      []time.Duration
	RetryMaxAttempts int

	ReconcileCron       string
	PruneCron           string
	StuckRequestAfter   time.Duration
	StuckSendingAfter   time.Duration
	SendTimeout         time.Duration
	PurgeTombstoneTTL   time.Duration
	MissingConfirmAfter time.Duration
	AgedQueuedAfter     time.Duration
	PrunePayloadAfter   time.Duration
	ReconcileLimit      int
	// OverdueAfter is how long past its retry time a FAILED row counts as overdue.
	OverdueAfter time.Duration
	// ReputationWindow is the trailing window of the bounce and complaint rates.
	ReputationWindow time.Duration
	// ReputationMinSends is the email-send count in the window below which the
	// rates read 0.
	ReputationMinSends int

	BrandName      string
	SiteURL        string
	LogoURL        string
	PreferencesURL string
	PrivacyURL     string
	TermsURL       string

	LogLevel string
}

// Var documents one environment variable.
type Var struct {
	Name, Default, Help string
}

// Vars lists every variable Load reads, with its default and a one-line help.
// `outbox-comms config` prints it.
var Vars = []Var{
	{"OUTBOX_COMMS_ENV", "development", "Deployment name; `admin render --send` refuses prod or production, including region suffixes"},
	{"OUTBOX_COMMS_DATABASE_URL", "postgres://outbox:outbox-local-only@localhost:5432/outbox_comms?sslmode=disable", "Postgres connection URL"},
	{"OUTBOX_COMMS_REDIS_ADDR", "localhost:6379", "Redis host:port (scheduler, retries, and the asynq queue)"},
	{"OUTBOX_COMMS_REDIS_DB", "0", "Redis database number"},
	{"OUTBOX_COMMS_REDIS_PASSWORD", "", "Redis password"},
	{"OUTBOX_COMMS_GRPC_ADDR", "127.0.0.1:9090", "gRPC listen address"},
	{"OUTBOX_COMMS_HTTP_ADDR", "127.0.0.1:8080", "HTTP listen address for /healthz and /metrics"},
	{"OUTBOX_COMMS_GRPC_REFLECTION", "false", "Expose gRPC reflection for development tools"},
	{"OUTBOX_COMMS_QUEUE", "asynq", "Trigger transport: asynq (Redis) or sqs"},
	{"OUTBOX_COMMS_SQS_REQUESTS_URL", "", "SQS queue URL for request triggers (queue=sqs)"},
	{"OUTBOX_COMMS_SQS_DISPATCH_URL", "", "SQS queue URL for dispatch triggers (queue=sqs)"},
	{"OUTBOX_COMMS_SQS_FEEDBACK_URL", "", "SQS queue URL carrying SES feedback; empty disables the consumer"},
	{"OUTBOX_COMMS_SQS_WORKERS", "2", "Pollers per SQS queue"},
	{"OUTBOX_COMMS_WORKER_CONCURRENCY", "10", "asynq worker concurrency"},
	{"OUTBOX_COMMS_EMAIL", "smtp", "Email sender: smtp, ses, or none"},
	{"OUTBOX_COMMS_EMAIL_FROM", "Outbox Comms <noreply@example.com>", "From address"},
	{"OUTBOX_COMMS_SMTP_ADDR", "localhost:1025", "SMTP relay host:port"},
	{"OUTBOX_COMMS_SMTP_USERNAME", "", "SMTP username; empty disables auth"},
	{"OUTBOX_COMMS_SMTP_PASSWORD", "", "SMTP password"},
	{"OUTBOX_COMMS_SES_CONFIGURATION_SET", "", "SES configuration set that publishes feedback events"},
	{"OUTBOX_COMMS_ATTACHMENTS", "dir", "Attachment store: dir or s3"},
	{"OUTBOX_COMMS_ATTACHMENTS_DIR", "./data/attachments", "Attachment directory (attachments=dir)"},
	{"OUTBOX_COMMS_S3_BUCKET", "", "Attachment bucket (attachments=s3)"},
	{"OUTBOX_COMMS_S3_PREFIX", "", "Key prefix inside the bucket"},
	{"OUTBOX_COMMS_SLACK_API_URL", "https://slack.com/api", "Slack Web API base URL"},
	{"OUTBOX_COMMS_SLACK_BOT_TOKEN", "", "Slack bot token used for every org without its own"},
	{"OUTBOX_COMMS_SLACK_ORG_TOKENS", "", "Per-org Slack bot tokens: org-uuid=token,org-uuid=token"},
	{"OUTBOX_COMMS_RETRY_LADDER", "1m,5m,30m,2h,6h", "Backoff after each failed send"},
	{"OUTBOX_COMMS_RETRY_MAX_ATTEMPTS", "6", "Sends before a message is DEAD"},
	{"OUTBOX_COMMS_RECONCILE_CRON", "*/10 * * * *", "Reconcile schedule (cron or @every)"},
	{"OUTBOX_COMMS_PRUNE_CRON", "0 0 * * *", "Prune schedule (cron or @every)"},
	{"OUTBOX_COMMS_STUCK_REQUEST_AFTER", "5m", "Age at which an ACCEPTED/FANNING_OUT request is re-driven"},
	{"OUTBOX_COMMS_STUCK_SENDING_AFTER", "5m", "Age at which a SENDING message is treated as crashed; at least 2.5 × OUTBOX_COMMS_SEND_TIMEOUT"},
	{"OUTBOX_COMMS_SEND_TIMEOUT", "60s", "Deadline of one provider call; a call cut off after its request was written is an uncertain send"},
	{"OUTBOX_COMMS_MISSING_CONFIRM_AFTER", "30m", "Wait for a delivery event before UNCONFIRMED"},
	{"OUTBOX_COMMS_AGED_QUEUED_AFTER", "5m", "Age at which a QUEUED message is re-dispatched"},
	{"OUTBOX_COMMS_PRUNE_PAYLOAD_AFTER", "2160h", "Age at which provider payloads are stripped (90 days)"},
	{"OUTBOX_COMMS_PURGE_TOMBSTONE_TTL", "720h", "How long a purge keeps each sent address and provider id for late bounces and complaints (30 days)"},
	{"OUTBOX_COMMS_RECONCILE_LIMIT", "1000", "Rows per reconcile scan"},
	{"OUTBOX_COMMS_OVERDUE_AFTER", "5m", "Time past next_retry_at before a FAILED row counts as overdue"},
	{"OUTBOX_COMMS_REPUTATION_WINDOW", "24h", "Trailing window of the bounce and complaint rates"},
	{"OUTBOX_COMMS_REPUTATION_MIN_SENDS", "100", "Email sends in the window below which the bounce and complaint rates read 0"},
	{"OUTBOX_COMMS_BRAND_NAME", "", "Sender name in the email header and footer"},
	{"OUTBOX_COMMS_SITE_URL", "", "Where the email header logo links"},
	{"OUTBOX_COMMS_LOGO_URL", "", "Absolute URL of the email header logo"},
	{"OUTBOX_COMMS_PREFERENCES_URL", "", "Manage-preferences link in non-transactional email footers"},
	{"OUTBOX_COMMS_PRIVACY_URL", "", "Privacy Policy link in the email footer"},
	{"OUTBOX_COMMS_TERMS_URL", "", "Terms of Use link in the email footer"},
	{"OUTBOX_COMMS_LOG_LEVEL", "info", "debug, info, warn or error"},
}

// Load reads the configuration from the environment.
func Load() (Config, error) { return LoadFrom(os.Getenv) }

// LoadFrom reads the configuration through getenv, so tests need not touch the
// process environment. It rejects malformed values rather than falling back to
// a default, so a typo is an error at start-up.
func LoadFrom(getenv func(string) string) (Config, error) {
	defaults := map[string]string{}
	for _, v := range Vars {
		defaults[v.Name] = v.Default
	}
	get := func(name string) string {
		if v := strings.TrimSpace(getenv(name)); v != "" {
			return v
		}
		return defaults[name]
	}
	var errs []string
	boolVar := func(name string) bool {
		v, err := strconv.ParseBool(get(name))
		if err != nil {
			errs = append(errs, name+": not a boolean")
		}
		return v
	}
	intVar := func(name string) int {
		n, err := strconv.Atoi(get(name))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: not an integer: %q", name, get(name)))
		}
		return n
	}
	durVar := func(name string) time.Duration {
		d, err := time.ParseDuration(get(name))
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("%s: not a positive duration: %q", name, get(name)))
		}
		return d
	}
	oneOf := func(name string, allowed ...string) string {
		v := strings.ToLower(get(name))
		for _, a := range allowed {
			if v == a {
				return v
			}
		}
		errs = append(errs, fmt.Sprintf("%s: %q is not one of %s", name, v, strings.Join(allowed, ", ")))
		return v
	}

	c := Config{
		Env:                 get("OUTBOX_COMMS_ENV"),
		DatabaseURL:         get("OUTBOX_COMMS_DATABASE_URL"),
		RedisAddr:           get("OUTBOX_COMMS_REDIS_ADDR"),
		RedisDB:             intVar("OUTBOX_COMMS_REDIS_DB"),
		RedisPass:           get("OUTBOX_COMMS_REDIS_PASSWORD"),
		GRPCAddr:            get("OUTBOX_COMMS_GRPC_ADDR"),
		HTTPAddr:            get("OUTBOX_COMMS_HTTP_ADDR"),
		GRPCReflection:      boolVar("OUTBOX_COMMS_GRPC_REFLECTION"),
		Queue:               oneOf("OUTBOX_COMMS_QUEUE", "asynq", "sqs"),
		SQSRequestsURL:      get("OUTBOX_COMMS_SQS_REQUESTS_URL"),
		SQSDispatchURL:      get("OUTBOX_COMMS_SQS_DISPATCH_URL"),
		SQSFeedbackURL:      get("OUTBOX_COMMS_SQS_FEEDBACK_URL"),
		QueueWorkers:        intVar("OUTBOX_COMMS_SQS_WORKERS"),
		WorkerConcurrency:   intVar("OUTBOX_COMMS_WORKER_CONCURRENCY"),
		Email:               oneOf("OUTBOX_COMMS_EMAIL", "smtp", "ses", "none"),
		EmailFrom:           get("OUTBOX_COMMS_EMAIL_FROM"),
		SMTPAddr:            get("OUTBOX_COMMS_SMTP_ADDR"),
		SMTPUsername:        get("OUTBOX_COMMS_SMTP_USERNAME"),
		SMTPPassword:        get("OUTBOX_COMMS_SMTP_PASSWORD"),
		SESConfigurationSet: get("OUTBOX_COMMS_SES_CONFIGURATION_SET"),
		Attachments:         oneOf("OUTBOX_COMMS_ATTACHMENTS", "dir", "s3"),
		AttachmentsDir:      get("OUTBOX_COMMS_ATTACHMENTS_DIR"),
		S3Bucket:            get("OUTBOX_COMMS_S3_BUCKET"),
		S3Prefix:            get("OUTBOX_COMMS_S3_PREFIX"),
		SlackAPIURL:         get("OUTBOX_COMMS_SLACK_API_URL"),
		SlackBotToken:       get("OUTBOX_COMMS_SLACK_BOT_TOKEN"),
		RetryMaxAttempts:    intVar("OUTBOX_COMMS_RETRY_MAX_ATTEMPTS"),
		ReconcileCron:       get("OUTBOX_COMMS_RECONCILE_CRON"),
		PruneCron:           get("OUTBOX_COMMS_PRUNE_CRON"),
		StuckRequestAfter:   durVar("OUTBOX_COMMS_STUCK_REQUEST_AFTER"),
		StuckSendingAfter:   durVar("OUTBOX_COMMS_STUCK_SENDING_AFTER"),
		SendTimeout:         durVar("OUTBOX_COMMS_SEND_TIMEOUT"),
		PurgeTombstoneTTL:   durVar("OUTBOX_COMMS_PURGE_TOMBSTONE_TTL"),
		MissingConfirmAfter: durVar("OUTBOX_COMMS_MISSING_CONFIRM_AFTER"),
		AgedQueuedAfter:     durVar("OUTBOX_COMMS_AGED_QUEUED_AFTER"),
		PrunePayloadAfter:   durVar("OUTBOX_COMMS_PRUNE_PAYLOAD_AFTER"),
		ReconcileLimit:      intVar("OUTBOX_COMMS_RECONCILE_LIMIT"),
		OverdueAfter:        durVar("OUTBOX_COMMS_OVERDUE_AFTER"),
		ReputationWindow:    durVar("OUTBOX_COMMS_REPUTATION_WINDOW"),
		ReputationMinSends:  intVar("OUTBOX_COMMS_REPUTATION_MIN_SENDS"),
		BrandName:           get("OUTBOX_COMMS_BRAND_NAME"),
		SiteURL:             get("OUTBOX_COMMS_SITE_URL"),
		LogoURL:             get("OUTBOX_COMMS_LOGO_URL"),
		PreferencesURL:      get("OUTBOX_COMMS_PREFERENCES_URL"),
		PrivacyURL:          get("OUTBOX_COMMS_PRIVACY_URL"),
		TermsURL:            get("OUTBOX_COMMS_TERMS_URL"),
		LogLevel:            oneOf("OUTBOX_COMMS_LOG_LEVEL", "debug", "info", "warn", "error"),
	}

	for _, part := range strings.Split(get("OUTBOX_COMMS_RETRY_LADDER"), ",") {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_RETRY_LADDER: %q is not a positive duration", part))
			continue
		}
		c.RetryLadder = append(c.RetryLadder, d)
	}
	if raw := get("OUTBOX_COMMS_SLACK_ORG_TOKENS"); raw != "" {
		c.SlackOrgTokens = map[string]string{}
		for _, pair := range strings.Split(raw, ",") {
			org, tok, ok := strings.Cut(pair, "=")
			org, tok = strings.TrimSpace(org), strings.TrimSpace(tok)
			if !ok || org == "" || tok == "" {
				errs = append(errs, "OUTBOX_COMMS_SLACK_ORG_TOKENS: each entry must be org-uuid=token")
				continue
			}
			id, err := uuid.Parse(org)
			if err != nil {
				errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_SLACK_ORG_TOKENS: %q is not an organization UUID", org))
				continue
			}
			// Keys are stored in the canonical lowercase form the API uses, so
			// two spellings of one org are one key, and a second token for it
			// is an error rather than a silent overwrite.
			key := id.String()
			if _, dup := c.SlackOrgTokens[key]; dup {
				errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_SLACK_ORG_TOKENS: organization %s is listed twice", key))
				continue
			}
			c.SlackOrgTokens[key] = tok
		}
	}
	if a, err := mail.ParseAddress(c.EmailFrom); err != nil || a.Address == "" {
		errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_EMAIL_FROM: %q is not an email address", c.EmailFrom))
	}
	if host, port, err := net.SplitHostPort(c.SMTPAddr); err != nil || host == "" || !validPort(port) {
		errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_SMTP_ADDR: %q is not host:port", c.SMTPAddr))
	}
	if c.QueueWorkers <= 0 {
		errs = append(errs, "OUTBOX_COMMS_SQS_WORKERS: must be positive")
	}
	if c.WorkerConcurrency <= 0 {
		errs = append(errs, "OUTBOX_COMMS_WORKER_CONCURRENCY: must be positive")
	}
	if c.RetryMaxAttempts <= 0 {
		errs = append(errs, "OUTBOX_COMMS_RETRY_MAX_ATTEMPTS: must be positive")
	}
	// A send must end well before reconcile could take its row for a crash, or
	// a slow provider call is sent again while it is still running. Twice the
	// deadline plus half of it again covers the claim, the render and the
	// status write around the call. The comparison is in float64: multiplying
	// a long duration would overflow int64 and wave a huge timeout through.
	if c.SendTimeout > 0 && c.StuckSendingAfter > 0 && float64(c.StuckSendingAfter) < 2.5*float64(c.SendTimeout) {
		errs = append(errs, fmt.Sprintf("OUTBOX_COMMS_STUCK_SENDING_AFTER (%s) must be at least 2.5 × OUTBOX_COMMS_SEND_TIMEOUT (%s)",
			c.StuckSendingAfter, c.SendTimeout))
	}
	if c.ReconcileLimit <= 0 {
		errs = append(errs, "OUTBOX_COMMS_RECONCILE_LIMIT: must be positive")
	}
	if c.ReputationMinSends < 0 {
		errs = append(errs, "OUTBOX_COMMS_REPUTATION_MIN_SENDS: must not be negative")
	}
	// The scheduler parses schedules with the standard cron parser, which takes
	// five fields or a descriptor such as @every 10m. A schedule it rejects would
	// otherwise only log an error and never run.
	for _, name := range []string{"OUTBOX_COMMS_RECONCILE_CRON", "OUTBOX_COMMS_PRUNE_CRON"} {
		if _, err := cron.ParseStandard(get(name)); err != nil {
			errs = append(errs, fmt.Sprintf("%s: not a cron schedule: %q (%v)", name, get(name), err))
		}
	}
	if c.Queue == "sqs" && (c.SQSRequestsURL == "" || c.SQSDispatchURL == "") {
		errs = append(errs, "OUTBOX_COMMS_QUEUE=sqs needs OUTBOX_COMMS_SQS_REQUESTS_URL and OUTBOX_COMMS_SQS_DISPATCH_URL")
	}
	if c.Attachments == "s3" && c.S3Bucket == "" {
		errs = append(errs, "OUTBOX_COMMS_ATTACHMENTS=s3 needs OUTBOX_COMMS_S3_BUCKET")
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		return Config{}, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return c, nil
}

// validPort reports whether p is a decimal TCP port, 1 to 65535.
func validPort(p string) bool {
	n, err := strconv.Atoi(p)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == p
}
