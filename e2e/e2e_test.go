// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

//go:build e2e

// Package e2e runs the service end to end against local stand-ins: Postgres,
// Redis, Mailpit, the Slack stub, the fake SES and ElasticMQ, which
// scripts/e2e.sh starts. The service runs on the host, built from e2e/server,
// so the test can kill and restart it. Phase one uses the asynq queue and SMTP;
// phase two uses SQS and SES. Every case asserts the row's status, how many
// calls the provider stand-in took, and the metric that counts the outcome.
package e2e

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/fakeses"
	"github.com/tetriz-ai/outbox-comms/internal/slackstub"
)

// The timings the service runs with. The stuck threshold is 2.5 times the send
// deadline, the least the service accepts.
const (
	sendTimeout  = 6 * time.Second
	stuckSending = 15 * time.Second
	maxAttempts  = 3
)

const (
	orgA     = "3f1e2d4c-5b6a-4978-8a1b-2c3d4e5f6a7b"
	orgPurge = "9a8b7c6d-5e4f-4a3b-9c2d-1e0f2a3b4c5d"
)

type harness struct {
	t         *testing.T
	ports     map[string]string
	bin       string
	logPath   string
	attachDir string
	db        *sql.DB
	comms     pb.CommsServiceClient
	sqs       *sqs.Client
	svc       *exec.Cmd
	svcLog    *os.File
	waitExit  chan struct{}
	env       []string
	keySeq    int
}

func port(name, def string) string { return cmp.Or(os.Getenv(name), def) }

func newHarness(t *testing.T) *harness {
	bin := os.Getenv("E2E_SERVER_BIN")
	if bin == "" {
		t.Fatal("E2E_SERVER_BIN is not set; run scripts/e2e.sh")
	}
	h := &harness{t: t, bin: bin, ports: map[string]string{
		"pg":      port("OUTBOX_COMMS_PG_PORT", "55432"),
		"redis":   port("OUTBOX_COMMS_REDIS_PORT", "56379"),
		"smtp":    port("OUTBOX_COMMS_SMTP_PORT", "51025"),
		"mailpit": port("OUTBOX_COMMS_MAILPIT_PORT", "58025"),
		"stub":    port("OUTBOX_COMMS_STUB_PORT", "58090"),
		"ses":     port("OUTBOX_COMMS_SES_PORT", "58095"),
		"sqs":     port("OUTBOX_COMMS_SQS_PORT", "59324"),
		"grpc":    port("OUTBOX_COMMS_GRPC_PORT", "59090"),
		"http":    port("OUTBOX_COMMS_HTTP_PORT", "58080"),
	}}
	dir := cmp.Or(os.Getenv("E2E_LOG_DIR"), t.TempDir())
	h.logPath = filepath.Join(dir, "service.log")
	h.attachDir = t.TempDir()
	// The store reads each organization's keys from its own folder.
	if err := os.MkdirAll(filepath.Join(h.attachDir, orgA), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.attachDir, orgA, "report.csv"), []byte("day,sends\n2026-10-01,42\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	h.db = db
	conn, err := grpc.NewClient("127.0.0.1:"+h.ports["grpc"], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h.comms = pb.NewCommsServiceClient(conn)
	h.sqs = sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://127.0.0.1:" + h.ports["sqs"]),
		Credentials:  credentials.NewStaticCredentialsProvider("e2e-local-only", "e2e-local-only", ""),
	})
	t.Cleanup(func() {
		h.stop()
		_ = conn.Close()
		_ = db.Close()
		if t.Failed() {
			h.dumpLog()
		}
	})
	return h
}

func (h *harness) dsn() string {
	return "postgres://outbox:outbox-local-only@127.0.0.1:" + h.ports["pg"] + "/outbox_comms?sslmode=disable"
}

func (h *harness) queueURL(name string) string {
	return "http://127.0.0.1:" + h.ports["sqs"] + "/000000000000/" + name
}

// serviceEnv is the service's environment: the caller's, minus every AWS_ and
// OUTBOX_COMMS_ variable, plus the e2e settings. The AWS variables point every
// call at the local stand-ins with throwaway credentials.
func (h *harness) serviceEnv(phase map[string]string) []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "AWS_") || strings.HasPrefix(k, "OUTBOX_COMMS_") {
			continue
		}
		env = append(env, kv)
	}
	set := map[string]string{
		"OUTBOX_COMMS_DATABASE_URL":          h.dsn(),
		"OUTBOX_COMMS_REDIS_ADDR":            "127.0.0.1:" + h.ports["redis"],
		"OUTBOX_COMMS_GRPC_ADDR":             "127.0.0.1:" + h.ports["grpc"],
		"OUTBOX_COMMS_HTTP_ADDR":             "127.0.0.1:" + h.ports["http"],
		"OUTBOX_COMMS_SLACK_API_URL":         "http://127.0.0.1:" + h.ports["stub"] + "/api",
		"OUTBOX_COMMS_SLACK_BOT_TOKEN":       "e2e-local-only",
		"OUTBOX_COMMS_ATTACHMENTS_DIR":       h.attachDir,
		"OUTBOX_COMMS_EMAIL_FROM":            "E2E Corp <noreply@example.com>",
		"OUTBOX_COMMS_BRAND_NAME":            "E2E Corp",
		"OUTBOX_COMMS_PRIVACY_URL":           "https://example.com/privacy",
		"OUTBOX_COMMS_SEND_TIMEOUT":          sendTimeout.String(),
		"OUTBOX_COMMS_STUCK_SENDING_AFTER":   stuckSending.String(),
		"OUTBOX_COMMS_STUCK_REQUEST_AFTER":   "5s",
		"OUTBOX_COMMS_AGED_QUEUED_AFTER":     "5s",
		"OUTBOX_COMMS_MISSING_CONFIRM_AFTER": "6s",
		"OUTBOX_COMMS_RECONCILE_CRON":        "@every 2s",
		"OUTBOX_COMMS_RETRY_LADDER":          "1s",
		"OUTBOX_COMMS_RETRY_MAX_ATTEMPTS":    strconv.Itoa(maxAttempts),
		"AWS_ACCESS_KEY_ID":                  "e2e-local-only",
		"AWS_SECRET_ACCESS_KEY":              "e2e-local-only",
		"AWS_REGION":                         "us-east-1",
		"AWS_ENDPOINT_URL_SES":               "http://127.0.0.1:" + h.ports["ses"],
		"AWS_ENDPOINT_URL_SQS":               "http://127.0.0.1:" + h.ports["sqs"],
		"AWS_EC2_METADATA_DISABLED":          "true",
		"AWS_CONFIG_FILE":                    os.DevNull,
		"AWS_SHARED_CREDENTIALS_FILE":        os.DevNull,
	}
	for k, v := range phase {
		set[k] = v
	}
	for k, v := range set {
		env = append(env, k+"="+v)
	}
	return env
}

// start runs the service and waits for its health endpoint.
func (h *harness) start(env []string) {
	h.t.Helper()
	h.env = env
	f, err := os.OpenFile(h.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		h.t.Fatal(err)
	}
	cmd := exec.Command(h.bin, "serve")
	cmd.Env, cmd.Stdout, cmd.Stderr = env, f, f
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.svc, h.svcLog = cmd, f
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-exited:
			h.t.Fatal("the service exited while starting; see its log")
		default:
		}
		if resp, err := http.Get("http://127.0.0.1:" + h.ports["http"] + "/healthz"); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				h.waitExit = exited
				return
			}
		}
		if time.Now().After(deadline) {
			h.t.Fatal("the service did not become healthy within 60s")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// kill ends the service with SIGKILL, as a crash would.
func (h *harness) kill() {
	h.t.Helper()
	_ = h.svc.Process.Signal(syscall.SIGKILL)
	<-h.waitExit
	_ = h.svcLog.Close()
	h.svc = nil
}

// stop ends the service with SIGTERM, or SIGKILL after 20 seconds.
func (h *harness) stop() {
	if h.svc == nil {
		return
	}
	_ = h.svc.Process.Signal(syscall.SIGTERM)
	select {
	case <-h.waitExit:
	case <-time.After(20 * time.Second):
		_ = h.svc.Process.Signal(syscall.SIGKILL)
		<-h.waitExit
	}
	_ = h.svcLog.Close()
	h.svc = nil
}

func (h *harness) dumpLog() {
	b, err := os.ReadFile(h.logPath)
	if err != nil {
		return
	}
	lines := strings.Split(string(b), "\n")
	if len(lines) > 80 {
		lines = lines[len(lines)-80:]
	}
	h.t.Logf("last lines of the service log %s:\n%s", h.logPath, strings.Join(lines, "\n"))
}

// admin runs `outbox-comms admin` with the service's environment.
func (h *harness) admin(args ...string) string {
	h.t.Helper()
	cmd := exec.Command(h.bin, append([]string{"admin"}, args...)...)
	cmd.Env = h.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		h.t.Fatalf("admin %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// compose runs a compose command against the e2e stack.
func (h *harness) compose(args string) {
	h.t.Helper()
	c := os.Getenv("E2E_COMPOSE")
	if c == "" {
		h.t.Fatal("E2E_COMPOSE is not set; run scripts/e2e.sh")
	}
	cmd := exec.Command("sh", "-c", c+" "+args)
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		h.t.Fatalf("compose %s: %v\n%s", args, err, out)
	}
}

func (h *harness) do(method, url string, body any) []byte {
	h.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		h.t.Fatalf("%s %s: %d %s", method, url, resp.StatusCode, out)
	}
	return out
}

// reset empties the database tables, Redis, the queues and every stand-in, so
// a run on a stack an earlier run left up starts from nothing.
func (h *harness) reset() {
	h.t.Helper()
	ctx := context.Background()
	if _, err := h.db.ExecContext(ctx, `DO $$ BEGIN
		IF to_regclass('comms_messages') IS NOT NULL THEN
			TRUNCATE comms_delivery_events, comms_messages, comms_requests, comms_suppressions,
				comms_purge_tombstones, comms_preferences, comms_channel_destinations;
		END IF; END $$`); err != nil {
		h.t.Fatalf("truncate: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + h.ports["redis"]})
	defer func() { _ = rdb.Close() }()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		h.t.Fatalf("flush redis: %v", err)
	}
	h.waitSQS()
	for _, q := range []string{"e2e-requests", "e2e-dispatch", "e2e-feedback"} {
		if _, err := h.sqs.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: aws.String(h.queueURL(q))}); err != nil {
			h.t.Fatalf("purge %s: %v", q, err)
		}
	}
	stub := "http://127.0.0.1:" + h.ports["stub"]
	h.do(http.MethodDelete, stub+"/messages", nil)
	h.do(http.MethodDelete, stub+"/control/faults", nil)
	h.do(http.MethodDelete, "http://127.0.0.1:"+h.ports["ses"]+"/calls", nil)
	h.do(http.MethodDelete, "http://127.0.0.1:"+h.ports["mailpit"]+"/api/v1/messages", nil)
}

func (h *harness) waitSQS() {
	h.t.Helper()
	h.waitFor("ElasticMQ", 60*time.Second, func() bool {
		_, err := h.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: aws.String(h.queueURL("e2e-feedback"))})
		return err == nil
	})
}

func (h *harness) waitFor(what string, d time.Duration, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// metric sums the samples of a metric whose labels contain every given pair.
func (h *harness) metric(name string, labels ...string) float64 {
	h.t.Helper()
	body := string(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["http"]+"/metrics", nil))
	sum := 0.0
	for _, line := range strings.Split(body, "\n") {
		rest, ok := strings.CutPrefix(line, name)
		if !ok || rest == "" || (rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		match := true
		for _, l := range labels {
			match = match && strings.Contains(rest, l)
		}
		if !match {
			continue
		}
		f := strings.Fields(line)
		v, _ := strconv.ParseFloat(f[len(f)-1], 64)
		sum += v
	}
	return sum
}

func (h *harness) wantDelta(what string, before, after, want float64) {
	h.t.Helper()
	if after-before != want {
		h.t.Errorf("%s went %v -> %v, want +%v", what, before, after, want)
	}
}

var content = &pb.MessageContent{Base: &pb.RenderableContent{
	Subject: "Your export is ready",
	Blocks:  []*pb.ContentBlock{{Block: &pb.ContentBlock_Paragraph{Paragraph: &pb.Paragraph{Text: "The October export finished."}}}},
}}

func email(addr string) *pb.CommsEntry {
	return &pb.CommsEntry{Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: addr}
}

func slack(channel string, mode pb.SlackDelivery) *pb.CommsEntry {
	return &pb.CommsEntry{Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_SLACK_CHANNEL, Recipient: channel, SlackDelivery: mode}
}

// submit sends one request and returns its idempotency key.
func (h *harness) submit(org, usecase string, params map[string]any, entry *pb.CommsEntry) string {
	h.t.Helper()
	h.keySeq++
	key := fmt.Sprintf("e2e-%d-%s", h.keySeq, entry.GetRecipient())
	req := &pb.SubmitCommsRequest{OrganizationId: org, IdempotencyKey: key, Usecase: usecase, Content: content, Entries: []*pb.CommsEntry{entry}}
	if params != nil {
		p, err := structpb.NewStruct(params)
		if err != nil {
			h.t.Fatal(err)
		}
		req.Params = p
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := h.comms.SubmitComms(ctx, req); err != nil {
		h.t.Fatalf("submit %s: %v", key, err)
	}
	return key
}

type row struct {
	found                                 bool
	status, dead, unconfirmed, providerID string
	attempts                              int
}

func (h *harness) row(org, key string) row {
	h.t.Helper()
	var r row
	err := h.db.QueryRow(`SELECT m.status, coalesce(m.dead_reason, ''), coalesce(m.unconfirmed_reason, ''),
			coalesce(m.provider_message_id, ''), m.attempts
		FROM comms_messages m JOIN comms_requests q ON q.id = m.request_id
		WHERE q.organization_id = $1 AND q.idempotency_key = $2`, org, key).
		Scan(&r.status, &r.dead, &r.unconfirmed, &r.providerID, &r.attempts)
	if err == sql.ErrNoRows {
		return r
	}
	if err != nil {
		h.t.Fatalf("read row %s: %v", key, err)
	}
	r.found = true
	return r
}

func short(s string) string { return strings.TrimPrefix(s, "MESSAGE_STATUS_") }

// waitStatus waits for a request's message to reach a status, and returns it.
func (h *harness) waitStatus(org, key, status string, d time.Duration) row {
	h.t.Helper()
	var r row
	deadline := time.Now().Add(d)
	for {
		r = h.row(org, key)
		if short(r.status) == status {
			return r
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("%s: status %s (dead %q, unconfirmed %q, attempts %d) after %v, want %s",
				key, short(r.status), r.dead, r.unconfirmed, r.attempts, d, status)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (h *harness) stubPosts(channel string) int {
	h.t.Helper()
	var ms []slackstub.Message
	if err := json.Unmarshal(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["stub"]+"/messages", nil), &ms); err != nil {
		h.t.Fatal(err)
	}
	n := 0
	for _, m := range ms {
		if m.Channel == channel {
			n++
		}
	}
	return n
}

func (h *harness) stubFault(f slackstub.Fault) {
	h.t.Helper()
	h.do(http.MethodPost, "http://127.0.0.1:"+h.ports["stub"]+"/control/faults", f)
}

// faultUses is how many calls the stub answered with the fault for a channel.
func (h *harness) faultUses(channel string) int {
	h.t.Helper()
	var fs []slackstub.FaultUse
	if err := json.Unmarshal(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["stub"]+"/control/faults", nil), &fs); err != nil {
		h.t.Fatal(err)
	}
	n := 0
	for _, f := range fs {
		if f.Channel == channel {
			n += f.Used
		}
	}
	return n
}

func (h *harness) sesFault(f fakeses.Fault) {
	h.t.Helper()
	h.do(http.MethodPost, "http://127.0.0.1:"+h.ports["ses"]+"/control/faults", f)
}

func (h *harness) sesCalls(to string) []fakeses.Call {
	h.t.Helper()
	var calls []fakeses.Call
	if err := json.Unmarshal(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["ses"]+"/calls", nil), &calls); err != nil {
		h.t.Fatal(err)
	}
	var out []fakeses.Call
	for _, c := range calls {
		if strings.EqualFold(c.To, to) {
			out = append(out, c)
		}
	}
	return out
}

// feedback puts one SES notification on the feedback queue, wrapped in an SNS
// envelope when sns is set.
func (h *harness) feedback(body string, sns bool) {
	h.t.Helper()
	if sns {
		b, _ := json.Marshal(map[string]string{"Type": "Notification", "MessageId": "e2e-sns", "Message": body})
		body = string(b)
	}
	if _, err := h.sqs.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(h.queueURL("e2e-feedback")), MessageBody: aws.String(body),
	}); err != nil {
		h.t.Fatalf("send feedback: %v", err)
	}
}

func sesEvent(kind, messageID, extra string) string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	s := fmt.Sprintf(`{"eventType":%q,"mail":{"messageId":%q,"timestamp":%q}`, kind, messageID, now)
	if extra != "" {
		s += "," + extra
	}
	return s + "}"
}

func bounce(kind, recipient string) string {
	return fmt.Sprintf(`"bounce":{"bounceType":%q,"feedbackId":"fb-%d","timestamp":%q,"bouncedRecipients":[{"emailAddress":%q}]}`,
		kind, time.Now().UnixNano(), time.Now().UTC().Format(time.RFC3339Nano), recipient)
}

func (h *harness) suppressed(addr string) bool {
	h.t.Helper()
	var n int
	if err := h.db.QueryRow(`SELECT count(*) FROM comms_suppressions WHERE lower(email) = lower($1) AND deleted_at IS NULL`, addr).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n > 0
}

// queueEmpty reports whether the feedback queue holds no message, visible or in flight.
func (h *harness) queueEmpty(name string) bool {
	h.t.Helper()
	out, err := h.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(h.queueURL(name)),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return out.Attributes["ApproximateNumberOfMessages"] == "0" && out.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
}

// run runs a subtest with the harness reporting to it, then hands the harness
// back to the parent.
func (h *harness) run(t *testing.T, name string, f func(t *testing.T)) {
	t.Run(name, func(st *testing.T) {
		h.t = st
		f(st)
	})
	h.t = t
}

func TestLifecycle(t *testing.T) {
	h := newHarness(t)
	h.reset()
	h.run(t, "asynq and smtp", h.phaseAsynqSMTP)
	if t.Failed() {
		return
	}
	h.run(t, "sqs and ses", h.phaseSQSSES)
}

func (h *harness) phaseAsynqSMTP(t *testing.T) {
	h.start(h.serviceEnv(map[string]string{
		"OUTBOX_COMMS_QUEUE":     "asynq",
		"OUTBOX_COMMS_EMAIL":     "smtp",
		"OUTBOX_COMMS_SMTP_ADDR": "127.0.0.1:" + h.ports["smtp"],
	}))
	link, file := pb.SlackDelivery_SLACK_DELIVERY_LINK, pb.SlackDelivery_SLACK_DELIVERY_FILE
	post := "chat.postMessage"
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0RETRY", Status: 500, Times: 1})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0RATE", Status: 429, RetryAfter: 3, Times: 1})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0AUTH", Error: "invalid_auth"})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0GONE", Error: "channel_not_found"})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0DOWN", Status: 503})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0BEST", Status: 500, Times: 1})

	delivered := `to_status="MESSAGE_STATUS_DELIVERED"`
	m0 := map[string]float64{
		"delivered":  h.metric("outbox_comms_message_transitions_total", delivered, `channel="CHANNEL_SLACK"`),
		"permanent":  h.metric("outbox_comms_dead_total", `reason_class="permanent_config"`),
		"exhausted":  h.metric("outbox_comms_dead_total", `reason_class="transient_exhausted"`),
		"uncertain":  h.metric("outbox_comms_unconfirmed_total", `reason="uncertain_send"`),
		"emailsSent": h.metric("outbox_comms_message_transitions_total", `to_status="MESSAGE_STATUS_SENT"`, `channel="CHANNEL_EMAIL"`),
	}
	start := time.Now()
	kEmail := h.submit(orgA, "notice", nil, email("jane@example.com"))
	kLink := h.submit(orgA, "notice", nil, slack("C0LINK", link))
	kFile := h.submit(orgA, "notice", map[string]any{"attachment_key": "report.csv", "attachment_filename": "report.csv"}, slack("C0FILE", file))
	kRetry := h.submit(orgA, "notice", nil, slack("C0RETRY", link))
	kRate := h.submit(orgA, "notice", nil, slack("C0RATE", link))
	kAuth := h.submit(orgA, "notice", nil, slack("C0AUTH", link))
	kGone := h.submit(orgA, "notice", nil, slack("C0GONE", link))
	kDown := h.submit(orgA, "notice", nil, slack("C0DOWN", link))
	kBest := h.submit(orgA, "digest", nil, slack("C0BEST", link))

	h.run(t, "smtp send with the chrome", func(t *testing.T) {
		h.waitStatus(orgA, kEmail, "SENT", 30*time.Second)
		var list struct {
			Messages []struct {
				ID string
				To []struct{ Address string }
			}
		}
		if err := json.Unmarshal(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["mailpit"]+"/api/v1/messages", nil), &list); err != nil {
			t.Fatal(err)
		}
		if len(list.Messages) != 1 || len(list.Messages[0].To) != 1 || list.Messages[0].To[0].Address != "jane@example.com" {
			t.Fatalf("mailpit holds %+v, want one email to jane@example.com", list.Messages)
		}
		var msg struct{ HTML string }
		_ = json.Unmarshal(h.do(http.MethodGet, "http://127.0.0.1:"+h.ports["mailpit"]+"/api/v1/message/"+list.Messages[0].ID, nil), &msg)
		for _, want := range []string{"E2E Corp", "https://example.com/privacy", "The October export finished."} {
			if !strings.Contains(msg.HTML, want) {
				t.Errorf("the email HTML lacks %q", want)
			}
		}
		st, err := h.comms.GetCommsStatus(context.Background(), &pb.GetCommsStatusRequest{OrganizationId: orgA, Handle: kEmail})
		if err != nil || !st.GetDone() || st.GetIsPartial() {
			t.Fatalf("status done=%v partial=%v err=%v, want done and not partial", st.GetDone(), st.GetIsPartial(), err)
		}
	})
	h.run(t, "slack link and file", func(t *testing.T) {
		h.waitStatus(orgA, kLink, "DELIVERED", 30*time.Second)
		h.waitStatus(orgA, kFile, "DELIVERED", 30*time.Second)
		if a, b := h.stubPosts("C0LINK"), h.stubPosts("C0FILE"); a != 1 || b != 1 {
			t.Fatalf("stub posts: C0LINK %d, C0FILE %d; want 1 each", a, b)
		}
	})
	h.run(t, "500 then ok", func(t *testing.T) {
		h.waitStatus(orgA, kRetry, "DELIVERED", 30*time.Second)
		if u, p := h.faultUses("C0RETRY"), h.stubPosts("C0RETRY"); u != 1 || p != 1 {
			t.Fatalf("C0RETRY: %d failed call(s), %d post(s); want 1 and 1", u, p)
		}
	})
	h.run(t, "429 with retry-after", func(t *testing.T) {
		h.waitStatus(orgA, kRate, "DELIVERED", 30*time.Second)
		if took := time.Since(start); took < 3*time.Second {
			t.Fatalf("delivered %v after submit; the 3s Retry-After was not honoured", took)
		}
		if u, p := h.faultUses("C0RATE"), h.stubPosts("C0RATE"); u != 1 || p != 1 {
			t.Fatalf("C0RATE: %d limited call(s), %d post(s); want 1 and 1", u, p)
		}
	})
	h.run(t, "invalid_auth and channel_not_found are dead", func(t *testing.T) {
		if r := h.waitStatus(orgA, kAuth, "DEAD", 30*time.Second); r.dead != "invalid_auth" || r.attempts > 1 {
			t.Errorf("C0AUTH: dead %q after %d attempt(s), want invalid_auth after one", r.dead, r.attempts)
		}
		if r := h.waitStatus(orgA, kGone, "DEAD", 30*time.Second); r.dead != "permanent_config" {
			t.Errorf("C0GONE: dead %q, want permanent_config", r.dead)
		}
		if a, b := h.faultUses("C0AUTH"), h.faultUses("C0GONE"); a != 1 || b != 1 {
			t.Errorf("calls: C0AUTH %d, C0GONE %d; want 1 each", a, b)
		}
	})
	h.run(t, "retries exhausted", func(t *testing.T) {
		if r := h.waitStatus(orgA, kDown, "DEAD", 40*time.Second); r.dead != "transient_exhausted" {
			t.Errorf("C0DOWN: dead %q, want transient_exhausted", r.dead)
		}
		if u := h.faultUses("C0DOWN"); u != maxAttempts {
			t.Errorf("C0DOWN: %d call(s), want %d", u, maxAttempts)
		}
	})
	h.run(t, "best effort 500 is unconfirmed", func(t *testing.T) {
		if r := h.waitStatus(orgA, kBest, "UNCONFIRMED", 30*time.Second); r.unconfirmed != "uncertain_send" {
			t.Errorf("C0BEST: unconfirmed %q, want uncertain_send", r.unconfirmed)
		}
		if u, p := h.faultUses("C0BEST"), h.stubPosts("C0BEST"); u != 1 || p != 0 {
			t.Errorf("C0BEST: %d call(s), %d post(s); want 1 and 0 (never retried)", u, p)
		}
	})
	h.run(t, "metrics", func(_ *testing.T) {
		h.wantDelta("slack DELIVERED transitions", m0["delivered"], h.metric("outbox_comms_message_transitions_total", delivered, `channel="CHANNEL_SLACK"`), 4)
		h.wantDelta("dead_total permanent_config", m0["permanent"], h.metric("outbox_comms_dead_total", `reason_class="permanent_config"`), 2)
		h.wantDelta("dead_total transient_exhausted", m0["exhausted"], h.metric("outbox_comms_dead_total", `reason_class="transient_exhausted"`), 1)
		h.wantDelta("unconfirmed_total uncertain_send", m0["uncertain"], h.metric("outbox_comms_unconfirmed_total", `reason="uncertain_send"`), 1)
		h.wantDelta("email SENT transitions", m0["emailsSent"], h.metric("outbox_comms_message_transitions_total", `to_status="MESSAGE_STATUS_SENT"`, `channel="CHANNEL_EMAIL"`), 1)
	})
	h.run(t, "admin redrive-dead", func(t *testing.T) {
		h.do(http.MethodDelete, "http://127.0.0.1:"+h.ports["stub"]+"/control/faults", nil)
		out := h.admin("redrive-dead", "--org", orgA, "--reason", "permanent_config", "--execute")
		if !strings.Contains(out, "reset 1 message(s) to QUEUED") {
			t.Fatalf("redrive-dead said:\n%s", out)
		}
		h.waitStatus(orgA, kGone, "DELIVERED", 30*time.Second)
		if p := h.stubPosts("C0GONE"); p != 1 {
			t.Fatalf("C0GONE: %d post(s) after the redrive, want 1", p)
		}
	})
	if t.Failed() {
		return
	}
	h.run(t, "kill mid-post and repeated crashes", h.crashes)
	h.stop()
}

// crashes kills the service while three posts are held by the stub: a
// GUARANTEED one is sent again after the restart, a BEST_EFFORT one ends
// UNCONFIRMED without a second post, and one whose every post is held is killed
// on each attempt until it is DEAD(crash_exhausted).
func (h *harness) crashes(t *testing.T) {
	link := pb.SlackDelivery_SLACK_DELIVERY_LINK
	post := "chat.postMessage"
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0KILLG", Delay: "60s", Times: 1})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0KILLB", Delay: "60s", Times: 1})
	h.stubFault(slackstub.Fault{Method: post, Channel: "C0CRASH", Delay: "60s"})
	kG := h.submit(orgA, "notice", nil, slack("C0KILLG", link))
	kB := h.submit(orgA, "digest", nil, slack("C0KILLB", link))
	kC := h.submit(orgA, "notice", nil, slack("C0CRASH", link))
	h.waitFor("all three posts in flight", sendTimeout-time.Second, func() bool {
		return short(h.row(orgA, kG).status) == "SENDING" && short(h.row(orgA, kB).status) == "SENDING" && short(h.row(orgA, kC).status) == "SENDING"
	})
	h.kill()
	time.Sleep(time.Second)
	if n := h.stubPosts("C0KILLG") + h.stubPosts("C0KILLB") + h.stubPosts("C0CRASH"); n != 0 {
		t.Fatalf("%d killed post(s) reached the stub", n)
	}
	kills := 1
	h.start(h.env)
	for {
		// The killed attempt stays SENDING until reconcile counts the crash as
		// an attempt and requeues it, or ends it DEAD at the cap. Reconcile runs
		// on the scheduler leader, and the killed process held the leader lock
		// until its 30-second expiry; a follower retries every 10 seconds.
		before := h.row(orgA, kC).attempts
		h.waitFor("reconcile to take the crashed C0CRASH", stuckSending+60*time.Second, func() bool {
			r := h.row(orgA, kC)
			return r.attempts > before || short(r.status) == "DEAD"
		})
		h.waitFor("C0CRASH to be sent again", 15*time.Second, func() bool {
			s := short(h.row(orgA, kC).status)
			return s == "SENDING" || s == "DEAD"
		})
		if short(h.row(orgA, kC).status) == "DEAD" {
			break
		}
		if kills > 2*maxAttempts {
			t.Fatalf("C0CRASH is still being sent after %d kill(s)", kills)
		}
		// Let a re-sent C0KILLG post finish first, so this kill does not cut it.
		// The kill lands just after the reconcile pass that requeued this row,
		// often while that pass is still running: the next leader's reconcile
		// must not be held back by it.
		h.waitFor("C0KILLG to settle", 2*time.Second, func() bool { return short(h.row(orgA, kG).status) != "SENDING" })
		h.kill()
		kills++
		h.start(h.env)
	}
	if r := h.row(orgA, kC); r.dead != "crash_exhausted" || h.stubPosts("C0CRASH") != 0 {
		t.Errorf("C0CRASH: dead %q, %d post(s); want crash_exhausted and none", r.dead, h.stubPosts("C0CRASH"))
	}
	if got := h.metric("outbox_comms_dead_total", `reason_class="crash_exhausted"`); got != 1 {
		t.Errorf("dead_total crash_exhausted in the last process: %v, want 1", got)
	}
	h.waitStatus(orgA, kG, "DELIVERED", 40*time.Second)
	if p := h.stubPosts("C0KILLG"); p != 1 {
		t.Errorf("C0KILLG: %d post(s), want exactly 1", p)
	}
	if r := h.waitStatus(orgA, kB, "UNCONFIRMED", 40*time.Second); r.unconfirmed != "uncertain_send" || h.stubPosts("C0KILLB") != 0 {
		t.Errorf("C0KILLB: unconfirmed %q, %d post(s); want uncertain_send and none", r.unconfirmed, h.stubPosts("C0KILLB"))
	}
	t.Logf("C0CRASH went DEAD(crash_exhausted) after %d kill(s)", kills)
}

func (h *harness) phaseSQSSES(t *testing.T) {
	h.do(http.MethodDelete, "http://127.0.0.1:"+h.ports["stub"]+"/control/faults", nil)
	env := h.serviceEnv(map[string]string{
		"OUTBOX_COMMS_QUEUE":                 "sqs",
		"OUTBOX_COMMS_EMAIL":                 "ses",
		"OUTBOX_COMMS_SQS_REQUESTS_URL":      h.queueURL("e2e-requests"),
		"OUTBOX_COMMS_SQS_DISPATCH_URL":      h.queueURL("e2e-dispatch"),
		"OUTBOX_COMMS_SQS_FEEDBACK_URL":      h.queueURL("e2e-feedback"),
		"OUTBOX_COMMS_SES_CONFIGURATION_SET": "e2e",
	})
	h.start(env)
	orphan0 := h.metric("outbox_comms_orphan_feedback_total")

	var kOK string
	h.run(t, "ses send through sqs", func(t *testing.T) {
		kOK = h.submit(orgA, "notice", nil, email("ses.ok@example.com"))
		r := h.waitStatus(orgA, kOK, "SENT", 30*time.Second)
		calls := h.sesCalls("ses.ok@example.com")
		if len(calls) != 1 || calls[0].MessageID != r.providerID {
			t.Fatalf("fake SES calls %+v, provider id %q; want one call whose id the row keeps", calls, r.providerID)
		}
	})
	h.run(t, "a lost trigger is re-published by reconcile", func(t *testing.T) {
		h.compose("stop elasticmq")
		k := h.submit(orgA, "notice", nil, email("lost.trigger@example.com"))
		if r := h.row(orgA, k); r.found {
			t.Fatalf("a message row exists before the trigger was delivered: %+v", r)
		}
		h.compose("up -d elasticmq")
		h.waitSQS()
		h.waitStatus(orgA, k, "SENT", 60*time.Second)
		if n := len(h.sesCalls("lost.trigger@example.com")); n != 1 {
			t.Fatalf("%d SES call(s), want 1", n)
		}
	})
	// The orphan needs five receives, so it goes out now and is checked last.
	h.feedback(sesEvent("Delivery", "0100-fake-never-sent", `"delivery":{"timestamp":"2026-10-01T00:00:00Z"}`), false)
	h.run(t, "ses MessageRejected is dead", func(t *testing.T) {
		h.sesFault(fakeses.Fault{Status: 400, Code: "MessageRejected", Times: 1})
		k := h.submit(orgA, "notice", nil, email("rejected@example.com"))
		if r := h.waitStatus(orgA, k, "DEAD", 30*time.Second); r.dead != "permanent_config" {
			t.Fatalf("dead %q, want permanent_config", r.dead)
		}
	})
	h.run(t, "confirmed usecase: unconfirmed, then a late delivery", func(t *testing.T) {
		u0 := h.metric("outbox_comms_unconfirmed_total", `reason="no_delivery_feedback"`)
		k := h.submit(orgA, "e2e.receipt", nil, email("confirm.me@example.com"))
		h.waitStatus(orgA, k, "SENT", 30*time.Second)
		r := h.waitStatus(orgA, k, "UNCONFIRMED", 30*time.Second)
		if r.unconfirmed != "no_delivery_feedback" {
			t.Fatalf("unconfirmed %q, want no_delivery_feedback", r.unconfirmed)
		}
		h.wantDelta("unconfirmed_total no_delivery_feedback", u0, h.metric("outbox_comms_unconfirmed_total", `reason="no_delivery_feedback"`), 1)
		h.feedback(sesEvent("Delivery", r.providerID, `"delivery":{"timestamp":"2026-10-01T00:00:00Z"}`), false)
		h.waitStatus(orgA, k, "DELIVERED", 30*time.Second)
	})
	h.run(t, "feedback: delivery", func(_ *testing.T) {
		h.feedback(sesEvent("Delivery", h.row(orgA, kOK).providerID, `"delivery":{"timestamp":"2026-10-01T00:00:00Z"}`), true)
		h.waitStatus(orgA, kOK, "DELIVERED", 30*time.Second)
	})
	h.run(t, "feedback: permanent bounce in an sns envelope, then unsuppress", func(t *testing.T) {
		k := h.submit(orgA, "notice", nil, email("Bounce.Me@Example.COM"))
		r := h.waitStatus(orgA, k, "SENT", 30*time.Second)
		h.feedback(sesEvent("Bounce", r.providerID, bounce("Permanent", "Bounce.Me@Example.COM")), true)
		h.waitStatus(orgA, k, "BOUNCED", 30*time.Second)
		if !h.suppressed("bounce.me@example.com") {
			t.Fatal("no suppression for the bounced address")
		}
		h.waitStatus(orgA, h.submit(orgA, "notice", nil, email("bounce.me@example.com")), "SUPPRESSED", 30*time.Second)
		if out := h.admin("unsuppress", "--email", "BOUNCE.ME@example.com", "--execute"); !strings.Contains(out, "lifted") {
			t.Fatalf("unsuppress said:\n%s", out)
		}
		h.waitStatus(orgA, h.submit(orgA, "notice", nil, email("bounce.me@example.com")), "SENT", 30*time.Second)
	})
	h.run(t, "feedback: complaint", func(t *testing.T) {
		k := h.submit(orgA, "notice", nil, email("complain@example.com"))
		r := h.waitStatus(orgA, k, "SENT", 30*time.Second)
		h.feedback(sesEvent("Complaint", r.providerID, `"complaint":{"feedbackId":"fb-complaint","timestamp":"2026-10-01T00:00:00Z"}`), true)
		h.waitFor("the complaint suppression", 30*time.Second, func() bool { return h.suppressed("complain@example.com") })
		if s := short(h.row(orgA, k).status); s != "SENT" {
			t.Errorf("status %s after a complaint, want SENT (it was delivered)", s)
		}
		h.waitStatus(orgA, h.submit(orgA, "notice", nil, email("complain@example.com")), "SUPPRESSED", 30*time.Second)
	})
	h.run(t, "feedback: transient bounce", func(t *testing.T) {
		k := h.submit(orgA, "notice", nil, email("transient@example.com"))
		r := h.waitStatus(orgA, k, "SENT", 30*time.Second)
		h.feedback(sesEvent("Bounce", r.providerID, bounce("Transient", "transient@example.com")), true)
		h.waitFor("the bounce event", 30*time.Second, func() bool {
			var n int
			_ = h.db.QueryRow(`SELECT count(*) FROM comms_delivery_events e JOIN comms_messages m ON m.id = e.message_id
				WHERE m.provider_message_id = $1 AND e.event_type = 'DELIVERY_EVENT_TYPE_BOUNCE'`, r.providerID).Scan(&n)
			return n == 1
		})
		if s := short(h.row(orgA, k).status); s != "SENT" || h.suppressed("transient@example.com") {
			t.Fatalf("status %s, suppressed %v; a transient bounce changes neither", s, h.suppressed("transient@example.com"))
		}
	})
	h.run(t, "feedback: malformed", func(_ *testing.T) {
		j0 := h.metric("outbox_comms_feedback_dropped_total", `reason="malformed_json"`)
		i0 := h.metric("outbox_comms_feedback_dropped_total", `reason="missing_message_id"`)
		h.feedback("{not json", false)
		h.feedback(`{"eventType":"Delivery","mail":{}}`, true)
		h.waitFor("both dropped", 30*time.Second, func() bool {
			return h.metric("outbox_comms_feedback_dropped_total", `reason="malformed_json"`) == j0+1 &&
				h.metric("outbox_comms_feedback_dropped_total", `reason="missing_message_id"`) == i0+1
		})
	})
	h.run(t, "purge, then a late bounce meets the tombstone", func(t *testing.T) {
		k := h.submit(orgPurge, "notice", nil, email("purge.me@example.com"))
		r := h.waitStatus(orgPurge, k, "SENT", 30*time.Second)
		resp, err := h.comms.PurgeOrgComms(context.Background(), &pb.PurgeOrgCommsRequest{OrganizationId: orgPurge})
		if err != nil || resp.GetDeletedMessages() != 1 {
			t.Fatalf("purge: %v, %d message(s)", err, resp.GetDeletedMessages())
		}
		h.feedback(sesEvent("Bounce", r.providerID, bounce("Permanent", "Purge.Me@example.com")), true)
		h.waitFor("the tombstone suppression", 30*time.Second, func() bool { return h.suppressed("purge.me@example.com") })
		h.waitStatus(orgA, h.submit(orgA, "notice", nil, email("purge.me@example.com")), "SUPPRESSED", 30*time.Second)
	})
	h.run(t, "a slow provider: one call at a time, ended by the deadline", func(t *testing.T) {
		h.sesFault(fakeses.Fault{Delay: "60s", Times: 1})
		begin := time.Now()
		k := h.submit(orgA, "notice", nil, email("slow@example.com"))
		h.waitFor("the slow call to end", sendTimeout+5*time.Second, func() bool {
			s := short(h.row(orgA, k).status)
			return s != "" && s != "QUEUED" && s != "SENDING"
		})
		h.waitStatus(orgA, k, "SENT", 30*time.Second)
		calls := h.sesCalls("slow@example.com")
		if len(calls) != 2 || calls[0].Overlapped || calls[1].Overlapped {
			t.Fatalf("calls %+v; want two (the cut-off one and its retry), never overlapping", calls)
		}
		if took := time.Since(begin); took >= stuckSending {
			t.Errorf("SENT after %v; the retry, not reconcile, should have sent it", took)
		}
	})
	h.run(t, "orphan feedback", func(_ *testing.T) {
		h.waitFor("the orphan to be dropped", 90*time.Second, func() bool {
			return h.metric("outbox_comms_orphan_feedback_total") >= orphan0+1 && h.queueEmpty("e2e-feedback")
		})
		h.wantDelta("orphan_feedback_total (the tombstone bounce is not an orphan)", orphan0, h.metric("outbox_comms_orphan_feedback_total"), 1)
	})
}
