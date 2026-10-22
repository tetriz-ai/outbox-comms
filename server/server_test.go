// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package server_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	"github.com/tetriz-ai/outbox-comms/config"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/slackstub"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/server"
)

var testDB *gorm.DB

// TestMain keeps every test away from real credentials: HOME is a scratch
// directory, AWS_* variables are cleared, and any AWS config read panics.
func TestMain(m *testing.M) {
	home, _ := os.MkdirTemp("", "outbox-comms-home")
	_ = os.Setenv("HOME", home)
	_ = os.Setenv("USERPROFILE", home)
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); strings.HasPrefix(k, "AWS_") {
			_ = os.Unsetenv(k)
		}
	}
	server.LoadAWSConfig = func(context.Context) (aws.Config, error) { panic("a test tried to read AWS credentials") }
	code := testdb.Main(m, &testDB)
	_ = os.RemoveAll(home)
	os.Exit(code)
}

const org = "11111111-1111-1111-1111-111111111111"

// smtpSink is a minimal SMTP server that accepts every message.
type smtpSink struct {
	mu   sync.Mutex
	msgs []string
	addr string
}

func newSMTPSink(t *testing.T) *smtpSink {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	s := &smtpSink{addr: l.Addr().String()}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *smtpSink) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	say := func(line string) { _, _ = fmt.Fprintf(c, "%s\r\n", line) }
	say("220 sink")
	var data strings.Builder
	inData := false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		if inData {
			if line == ".\r\n" {
				inData = false
				s.mu.Lock()
				s.msgs = append(s.msgs, data.String())
				s.mu.Unlock()
				data.Reset()
				say("250 queued")
				continue
			}
			data.WriteString(line)
			continue
		}
		switch cmd := strings.ToUpper(strings.TrimSpace(line)); {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			say("250 sink")
		case cmd == "DATA":
			inData = true
			say("354 go ahead")
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("250 ok")
		}
	}
}

func (s *smtpSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

type env struct {
	client   pb.CommsServiceClient
	conn     *grpc.ClientConn
	slack    *slackstub.Stub
	smtp     *smtpSink
	httpAddr string
}

// start runs the whole service against the test schema, a scratch Redis
// database, a Slack stub and an SMTP sink, with fast reconcile timings.
func start(t *testing.T, tweak func(*config.Config)) *env {
	t.Helper()
	return startWith(t, nil, tweak)
}

// startWith is start with extra environment variables over the test defaults.
func startWith(t *testing.T, extra map[string]string, tweak func(*config.Config)) *env {
	t.Helper()
	return startWithOptions(t, extra, server.Options{}, tweak)
}

// startWithOptions is startWith with the server's options.
func startWithOptions(t *testing.T, extra map[string]string, opts server.Options, tweak func(*config.Config)) *env {
	t.Helper()
	redisAddr := testdb.RedisAddr(t)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: 9})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	_ = rdb.Close()
	testDB.Exec("TRUNCATE comms_delivery_events, comms_messages, comms_requests, comms_suppressions, comms_preferences, comms_channel_destinations")

	stub := &slackstub.Stub{}
	slackSrv := httptest.NewServer(stub.Handler())
	t.Cleanup(slackSrv.Close)
	sink := newSMTPSink(t)

	cfg, err := config.LoadFrom(func(k string) string {
		if v, ok := extra[k]; ok {
			return v
		}
		return map[string]string{
			"OUTBOX_COMMS_DATABASE_URL":        testdb.SchemaURL(),
			"OUTBOX_COMMS_REDIS_ADDR":          redisAddr,
			"OUTBOX_COMMS_REDIS_DB":            "9",
			"OUTBOX_COMMS_GRPC_ADDR":           freeAddr(t),
			"OUTBOX_COMMS_HTTP_ADDR":           freeAddr(t),
			"OUTBOX_COMMS_SMTP_ADDR":           sink.addr,
			"OUTBOX_COMMS_SLACK_API_URL":       slackSrv.URL + "/api",
			"OUTBOX_COMMS_SLACK_BOT_TOKEN":     "test-token",
			"OUTBOX_COMMS_ATTACHMENTS_DIR":     t.TempDir(),
			"OUTBOX_COMMS_RETRY_LADDER":        "1s",
			"OUTBOX_COMMS_RECONCILE_CRON":      "@every 1s",
			"OUTBOX_COMMS_STUCK_SENDING_AFTER": "2s",
			"OUTBOX_COMMS_SEND_TIMEOUT":        "800ms",
			"OUTBOX_COMMS_STUCK_REQUEST_AFTER": "2s",
			"OUTBOX_COMMS_AGED_QUEUED_AFTER":   "2s",
			"OUTBOX_COMMS_LOG_LEVEL":           "error",
		}[k]
	})
	if err != nil {
		t.Fatal(err)
	}
	if tweak != nil {
		tweak(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, cfg, opts) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server.Run: %v", err)
			}
		case <-time.After(40 * time.Second):
			t.Error("server did not stop")
		}
	})
	waitFor(t, 20*time.Second, func() bool {
		resp, err := http.Get("http://" + cfg.HTTPAddr + "/healthz")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	conn, err := grpc.NewClient(cfg.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &env{conn: conn, client: pb.NewCommsServiceClient(conn), slack: stub, smtp: sink, httpAddr: cfg.HTTPAddr}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func content(subject string) *pb.MessageContent {
	return &pb.MessageContent{Base: &pb.RenderableContent{
		Subject: subject,
		Blocks:  []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "Your export is ready."}}}},
	}}
}

func statuses(t *testing.T, e *env, handle string) map[pb.Channel]pb.MessageStatus {
	t.Helper()
	resp, err := e.client.GetCommsStatus(context.Background(), &pb.GetCommsStatusRequest{OrganizationId: org, Handle: handle})
	if err != nil {
		return nil
	}
	out := map[pb.Channel]pb.MessageStatus{}
	for _, m := range resp.GetMessages() {
		out[m.GetChannel()] = m.GetStatus()
	}
	return out
}

// TestEndToEnd submits one notice to an email address and a Slack channel and
// follows it through fan-out and dispatch on the real queue: the email is SENT
// over SMTP and the Slack post DELIVERED, once each; a re-submit with the same
// key sends nothing more.
func TestEndToEnd(t *testing.T) {
	e := start(t, nil)
	req := &pb.SubmitCommsRequest{
		OrganizationId: org, IdempotencyKey: "e2e-1", Usecase: "notice", Content: content("Export ready"),
		Entries: []*pb.CommsEntry{
			{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "jane@example.com"},
			{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, Recipient: "C0123", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK},
		},
	}
	resp, err := e.client.SubmitComms(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		s := statuses(t, e, resp.GetHandle())
		return s[pb.Channel_CHANNEL_EMAIL] == pb.MessageStatus_MESSAGE_STATUS_SENT && s[pb.Channel_CHANNEL_SLACK] == pb.MessageStatus_MESSAGE_STATUS_DELIVERED
	})
	if _, err := e.client.SubmitComms(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if n := len(e.slack.Messages()); n != 1 {
		t.Fatalf("Slack received %d posts, want 1", n)
	}
	if got := e.slack.Messages()[0]; got.Text != "Export ready" || got.Channel != "C0123" || got.Token != "test-token" {
		t.Fatalf("Slack post = %+v", got)
	}
	if n := e.smtp.count(); n != 1 {
		t.Fatalf("SMTP received %d messages, want 1", n)
	}
}

// TestRetryAfterFailure proves a transient Slack failure is retried on the
// ladder and ends DELIVERED with exactly one post.
func TestRetryAfterFailure(t *testing.T) {
	e := start(t, nil)
	e.slack.FailNext(1)
	resp, err := e.client.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: org, IdempotencyKey: "e2e-retry", Usecase: "alert", Content: content("Disk almost full"),
		Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, Recipient: "C0456", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		return statuses(t, e, resp.GetHandle())[pb.Channel_CHANNEL_SLACK] == pb.MessageStatus_MESSAGE_STATUS_DELIVERED
	})
	if n := len(e.slack.Messages()); n != 1 {
		t.Fatalf("Slack received %d posts, want 1", n)
	}
	var attempts int
	testDB.Raw("SELECT attempts FROM comms_messages WHERE recipient = 'C0456'").Scan(&attempts)
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 failed attempt before the success", attempts)
	}
}

// TestCrashRecovery proves a message left SENDING by a worker that died is
// re-driven by reconcile and delivered once.
func TestCrashRecovery(t *testing.T) {
	e := start(t, nil)
	resp, err := e.client.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: org, IdempotencyKey: "e2e-crash", Usecase: "notice", Content: content("Recovered"),
		Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, Recipient: "C0789", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return statuses(t, e, resp.GetHandle())[pb.Channel_CHANNEL_SLACK] == pb.MessageStatus_MESSAGE_STATUS_DELIVERED
	})
	// Rewind the row to what a killed worker leaves: claimed, never finished.
	e.slack.Reset()
	testDB.Exec("UPDATE comms_messages SET status = 'MESSAGE_STATUS_SENDING', updated_at = now() - interval '1 minute' WHERE recipient = 'C0789'")
	waitFor(t, 20*time.Second, func() bool {
		return statuses(t, e, resp.GetHandle())[pb.Channel_CHANNEL_SLACK] == pb.MessageStatus_MESSAGE_STATUS_DELIVERED
	})
	if n := len(e.slack.Messages()); n != 1 {
		t.Fatalf("Slack received %d posts after recovery, want 1", n)
	}
}

// TestValidationInterceptor proves the gRPC server applies the proto's
// buf.validate rules before the handler.
func TestValidationInterceptor(t *testing.T) {
	e := start(t, nil)
	_, err := e.client.SubmitComms(context.Background(), &pb.SubmitCommsRequest{OrganizationId: "not-a-uuid", IdempotencyKey: "k", Usecase: "notice",
		Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "a@example.com"}}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "organization_id") {
		t.Fatalf("want InvalidArgument naming organization_id, got %v", err)
	}
}

// gauge reads one unlabelled gauge from the service's /metrics, or -1.
func gauge(t *testing.T, httpAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil {
				return f
			}
		}
	}
	return -1
}

// TestReputationGauges proves the running service sets the sender-reputation
// gauges from its configured window: 10 email sends an hour ago, one of which
// hard-bounced, read as a 0.1 bounce rate.
func TestReputationGauges(t *testing.T) {
	e := startWith(t, map[string]string{"OUTBOX_COMMS_REPUTATION_MIN_SENDS": "10"}, nil)
	var reqID string
	testDB.Raw(`INSERT INTO comms_requests (organization_id, idempotency_key, usecase, delivery_guarantee, status)
		VALUES (?, 'rep-1', 'notice', 'DELIVERY_GUARANTEE_GUARANTEED', 'REQUEST_STATUS_FANNED_OUT') RETURNING id`, org).Scan(&reqID)
	for i := 0; i < 10; i++ {
		status := "MESSAGE_STATUS_SENT"
		if i == 0 {
			status = "MESSAGE_STATUS_BOUNCED"
		}
		var msgID string
		testDB.Raw(`INSERT INTO comms_messages (organization_id, request_id, channel, vendor, recipient, status, verification_level, delivery_guarantee, updated_at)
			VALUES (?, ?, 'CHANNEL_EMAIL', 'VENDOR_SES', ?, ?, 'VERIFICATION_LEVEL_PROVIDER_ACK', 'DELIVERY_GUARANTEE_GUARANTEED', now() - interval '1 hour') RETURNING id`,
			org, reqID, fmt.Sprintf("rep%d@example.com", i), status).Scan(&msgID)
		testDB.Exec(`INSERT INTO comms_delivery_events (organization_id, message_id, event_type, occurred_at)
			VALUES (?, ?, 'DELIVERY_EVENT_TYPE_SENT', now() - interval '1 hour')`, org, msgID)
	}
	waitFor(t, 20*time.Second, func() bool { return gauge(t, e.httpAddr, "outbox_comms_bounce_rate") == 0.1 })
}

// TestReputationMinSends proves the default minimum sample: with fewer than 100
// email sends in the window the rates read 0, and the sample gauge shows why.
func TestReputationMinSends(t *testing.T) {
	e := start(t, nil)
	var reqID string
	testDB.Raw(`INSERT INTO comms_requests (organization_id, idempotency_key, usecase, delivery_guarantee, status)
		VALUES (?, 'rep-min', 'notice', 'DELIVERY_GUARANTEE_GUARANTEED', 'REQUEST_STATUS_FANNED_OUT') RETURNING id`, org).Scan(&reqID)
	// 7 sends: a sample size no other test leaves on the process-wide gauge.
	for i := 0; i < 7; i++ {
		status := "MESSAGE_STATUS_SENT"
		if i < 3 {
			status = "MESSAGE_STATUS_BOUNCED"
		}
		var msgID string
		testDB.Raw(`INSERT INTO comms_messages (organization_id, request_id, channel, vendor, recipient, status, verification_level, delivery_guarantee)
			VALUES (?, ?, 'CHANNEL_EMAIL', 'VENDOR_SES', ?, ?, 'VERIFICATION_LEVEL_PROVIDER_ACK', 'DELIVERY_GUARANTEE_GUARANTEED') RETURNING id`,
			org, reqID, fmt.Sprintf("min%d@example.com", i), status).Scan(&msgID)
		testDB.Exec(`INSERT INTO comms_delivery_events (organization_id, message_id, event_type, occurred_at)
			VALUES (?, ?, 'DELIVERY_EVENT_TYPE_SENT', now())`, org, msgID)
	}
	waitFor(t, 20*time.Second, func() bool { return gauge(t, e.httpAddr, "outbox_comms_reputation_sample_sends") == 7 })
	if got := gauge(t, e.httpAddr, "outbox_comms_bounce_rate"); got != 0 {
		t.Fatalf("bounce rate over 7 sends = %v, want 0 below the 100-send minimum", got)
	}
}

// TestServerSESClientMakesOneCall pins the SES client server.Run builds: the
// SDK's own retries are off, so a provider that answers 503 sees exactly one
// SendEmail for a best-effort message, which then ends UNCONFIRMED.
func TestServerSESClientMakesOneCall(t *testing.T) {
	var calls atomic.Int32
	ses := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(ses.Close)
	orig := server.LoadAWSConfig
	server.LoadAWSConfig = func(context.Context) (aws.Config, error) {
		return aws.Config{Region: "us-east-1", BaseEndpoint: aws.String(ses.URL),
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
			})}, nil
	}
	t.Cleanup(func() { server.LoadAWSConfig = orig })
	e := startWith(t, map[string]string{"OUTBOX_COMMS_EMAIL": "ses"}, nil)
	resp, err := e.client.SubmitComms(context.Background(), &pb.SubmitCommsRequest{
		OrganizationId: org, IdempotencyKey: "ses-one-call", Usecase: "digest", Content: content("Status digest"),
		Entries: []*pb.CommsEntry{{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: "jane@example.com"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return statuses(t, e, resp.GetHandle())[pb.Channel_CHANNEL_EMAIL] == pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("SES got %d SendEmail calls, want 1", n)
	}
}
