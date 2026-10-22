// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"gorm.io/gorm"

	"github.com/tetriz-ai/outbox-comms/config"
	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/attachments"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/server"
)

var testDB *gorm.DB

// TestMain keeps the commands away from real credentials: HOME is a scratch
// directory and any AWS config read panics.
func TestMain(m *testing.M) {
	home, _ := os.MkdirTemp("", "outbox-comms-home")
	_ = os.Setenv("HOME", home)
	server.LoadAWSConfig = func(context.Context) (aws.Config, error) { panic("a test tried to read AWS credentials") }
	code := testdb.Main(m, &testDB)
	_ = os.RemoveAll(home)
	os.Exit(code)
}

const org = "11111111-1111-1111-1111-111111111111"

func newEnv(t *testing.T, cfg config.Config) (*env, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	var out, errOut bytes.Buffer
	e := &env{cfg: cfg, stdout: &out, stderr: &errOut, openDAOs: func() (*daos, func(), error) {
		if testDB == nil {
			t.Skip("needs the test database")
		}
		return &daos{
			reconcile: dao.NewReconcileRepo(testDB), suppression: dao.NewSuppressionRepo(testDB),
			request: dao.NewRequestRepo(testDB), message: dao.NewMessageRepo(testDB),
		}, func() {}, nil
	}}
	return e, &out, &errOut
}

func defaults(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.LoadFrom(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestUsage pins the exit codes: 2 for a usage error, 0 for help, 1 for a
// failed command.
func TestUsage(t *testing.T) {
	tests := []struct {
		args []string
		code int
		out  string
	}{
		{nil, 2, "Commands:"},
		{[]string{"help"}, 0, "Commands:"},
		{[]string{"nope"}, 2, `unknown admin command "nope"`},
		{[]string{"unsuppress"}, 2, "--email is required"},
		{[]string{"redrive-dead", "--limit", "0"}, 2, "--limit must be a positive integer"},
		{[]string{"render", "--usecase", "notice"}, 2, "--channel is required"},
		{[]string{"render", "--usecase", "nope", "--channel", "email"}, 2, `--usecase "nope" is not registered`},
		{[]string{"render", "--usecase", "notice", "--channel", "email", "--send"}, 2, "--to is required"},
		{[]string{"send", "--org", "x"}, 2, "--org must be a UUID"},
		{[]string{"unsuppress", "-h"}, 0, "--email"},
		{[]string{"redrive-dead", "--help"}, 0, "\n  --limit int"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			e, out, errOut := newEnv(t, defaults(t))
			if code := e.run(context.Background(), tt.args); code != tt.code {
				t.Fatalf("exit %d, want %d; stderr %q", code, tt.code, errOut.String())
			}
			if !strings.Contains(out.String()+errOut.String(), tt.out) {
				t.Fatalf("output %q lacks %q", out.String()+errOut.String(), tt.out)
			}
		})
	}
}

func seedDead(t *testing.T, recipient, reason string) string {
	t.Helper()
	ctx := context.Background()
	_, req, err := dao.NewRequestRepo(testDB).CreateIfAbsent(ctx, &model.CommsRequest{OrganizationID: org, IdempotencyKey: "dead-" + recipient, Usecase: "notice", DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT})
	if err != nil {
		t.Fatal(err)
	}
	m := &model.CommsMessage{OrganizationID: org, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: recipient, Status: pb.MessageStatus_MESSAGE_STATUS_DEAD, DeadReason: &reason, Attempts: 6, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED}
	if err := testDB.Create(m).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE id = ?", m.ID)
		testDB.Exec("DELETE FROM comms_requests WHERE id = ?", req.ID)
	})
	return m.ID
}

// TestRedriveDead proves the command reads by default, masks addresses, and
// with --execute resets only the matching rows.
func TestRedriveDead(t *testing.T) {
	a := seedDead(t, "alice@example.com", "transient_exhausted")
	b := seedDead(t, "bob@example.com", "render_bug")
	e, out, _ := newEnv(t, defaults(t))
	if code := e.run(context.Background(), []string{"redrive-dead", "--reason", "transient_exhausted"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if !strings.Contains(out.String(), "1 DEAD message(s) match") || !strings.Contains(out.String(), "a***@example.com") || strings.Contains(out.String(), "alice@") {
		t.Fatalf("read output: %q", out.String())
	}
	var status string
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", a).Scan(&status)
	if status != "MESSAGE_STATUS_DEAD" {
		t.Fatal("read mode changed a row")
	}
	out.Reset()
	if code := e.run(context.Background(), []string{"redrive-dead", "--reason", "transient_exhausted", "--execute"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", a).Scan(&status)
	if status != "MESSAGE_STATUS_QUEUED" || !strings.Contains(out.String(), "reset 1 message(s)") {
		t.Fatalf("execute: status %s, output %q", status, out.String())
	}
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", b).Scan(&status)
	if status != "MESSAGE_STATUS_DEAD" {
		t.Fatal("a row with another reason was reset")
	}
}

// TestUnsuppress proves the command shows, then with --execute lifts, the
// suppression for one exact address.
func TestUnsuppress(t *testing.T) {
	ctx := context.Background()
	repo := dao.NewSuppressionRepo(testDB)
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = 'carol@example.com'") })
	_ = repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: "carol@example.com", Reason: "hard_bounce"})
	e, out, _ := newEnv(t, defaults(t))
	if code := e.run(ctx, []string{"unsuppress", "--email", "Carol@Example.com"}); code != 0 || !strings.Contains(out.String(), "is suppressed: reason=hard_bounce") {
		t.Fatalf("read: %d %q", code, out.String())
	}
	if got, _ := repo.GetActiveByEmail(ctx, "carol@example.com"); got == nil {
		t.Fatal("read mode lifted the suppression")
	}
	if code := e.run(ctx, []string{"unsuppress", "--email", "carol@example.com", "--execute"}); code != 0 {
		t.Fatal("execute failed")
	}
	if got, _ := repo.GetActiveByEmail(ctx, "carol@example.com"); got != nil {
		t.Fatal("execute did not lift it")
	}
	out.Reset()
	if code := e.run(ctx, []string{"unsuppress", "--email", "carol@example.com"}); code != 0 || !strings.Contains(out.String(), "is not suppressed") {
		t.Fatalf("after lift: %d %q", code, out.String())
	}
}

// TestRenderContent proves render prints the wrapped email and the Slack blocks
// from a content file, and refuses --send in prod.
func TestRenderContent(t *testing.T) {
	file := write(t, "c.json", `{"base":{"subject":"Quarterly numbers","blocks":[{"paragraph":{"text":"Up 4%"}}]},"slack":{"subject":"Quarterly (Slack)"}}`)
	e, out, _ := newEnv(t, defaults(t))
	if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file}); code != 0 {
		t.Fatal("render email failed")
	}
	if !strings.Contains(out.String(), "subject: Quarterly numbers") || !strings.Contains(out.String(), "<p>Up 4%</p>") {
		t.Fatalf("email output: %q", out.String())
	}
	out.Reset()
	if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "slack", "--content", file}); code != 0 || !strings.Contains(out.String(), "Quarterly (Slack)") {
		t.Fatalf("slack output: %q", out.String())
	}
	cfg := defaults(t)
	cfg.Env = "prod"
	e, _, errOut := newEnv(t, cfg)
	if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file, "--send", "--to", "x@example.com"}); code != 1 || !strings.Contains(errOut.String(), "refused") {
		t.Fatalf("prod send: %q", errOut.String())
	}
	bad := write(t, "bad.json", `{"base":{"subject":"s"},"typo":1}`)
	e, _, errOut = newEnv(t, defaults(t))
	if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", bad}); code != 1 || !strings.Contains(errOut.String(), "not valid MessageContent") {
		t.Fatalf("unknown field: %q", errOut.String())
	}
}

// TestSendDryRun proves the default mode prints the masked audience and submits
// nothing.
func TestSendDryRun(t *testing.T) {
	content := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	recipients := write(t, "r.txt", "jane@example.com,Jane\njohn@example.com\n")
	e, out, errOut := newEnv(t, defaults(t))
	code := e.run(context.Background(), []string{"send", "--org", org, "--usecase", "notice", "--content", content, "--recipients", recipients})
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	for _, want := range []string{"audience: 2 recipient(s) in 1 chunk(s)", "j***@example.com Jane", "dry run: nothing submitted"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q: %q", want, out.String())
		}
	}
	if strings.Contains(out.String(), "jane@") {
		t.Error("full address printed")
	}
}

// fakeSubmitter persists the request as SubmitComms would, without a server.
type fakeSubmitter struct {
	calls []*pb.SubmitCommsRequest
}

func (f *fakeSubmitter) SubmitComms(ctx context.Context, req *pb.SubmitCommsRequest, _ ...grpc.CallOption) (*pb.SubmitCommsResponse, error) {
	f.calls = append(f.calls, req)
	label := req.GetAnalyticsId()
	_, _, err := dao.NewRequestRepo(testDB).CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: req.GetOrganizationId(), IdempotencyKey: req.GetIdempotencyKey(), Usecase: req.GetUsecase(),
		Params: req.GetParams(), Content: req.GetContent(), Entries: &pb.CommsEntries{Entries: req.GetEntries()},
		AnalyticsID: &label, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
	})
	if err != nil {
		return nil, err
	}
	return &pb.SubmitCommsResponse{Handle: req.GetIdempotencyKey()}, nil
}

type memStore map[string][]byte

func (m memStore) Get(_ context.Context, o, k string) ([]byte, error) { return m[o+"/"+k], nil }
func (m memStore) Put(_ context.Context, o, k string, b []byte) error {
	m[o+"/"+k] = b
	return nil
}

// TestSendPreviewGate walks the maker-checker flow: the send is refused until a
// reviewer received the preview, refused when the audience changed, then
// submitted once, and a re-run submits nothing.
func TestSendPreviewGate(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	recipients := write(t, "r.txt", "jane@example.com,Jane\njohn@example.com\n")
	sub := &fakeSubmitter{}
	store := memStore{}
	e, out, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	mk := func(mode, previewID, recPath string) *sendCmd {
		c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: mode, previewID: previewID, d: d, comms: sub, store: store, now: time.Now, newUUID: uuid.NewString}
		if err := c.validate(recPath, []string{"Reviewer@Example.com"}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if err := mk(sendModePreview, "", recipients).run(ctx); err != nil {
		t.Fatal(err)
	}
	pv := sub.calls[0]
	if len(pv.GetEntries()) != 1 || pv.GetEntries()[0].GetRecipient() != "reviewer@example.com" || !strings.HasPrefix(pv.GetContent().GetBase().GetSubject(), "[PREVIEW]") {
		t.Fatalf("preview went to %v with subject %q", pv.GetEntries(), pv.GetContent().GetBase().GetSubject())
	}
	row, _ := d.request.GetByHandle(ctx, org, pv.GetIdempotencyKey())

	err := mk(sendModeSend, row.ID, recipients).run(ctx)
	if err == nil || !strings.Contains(err.Error(), "no messages yet") {
		t.Fatalf("send before the preview fanned out: %v", err)
	}
	testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	if err := mk(sendModeSend, row.ID, recipients).run(ctx); err == nil || !strings.Contains(err.Error(), "not reached any reviewer") {
		t.Fatalf("send before delivery: %v", err)
	}
	testDB.Exec("UPDATE comms_messages SET status = 'MESSAGE_STATUS_SENT' WHERE request_id = ?", row.ID)
	other := write(t, "r2.txt", "jane@example.com\n")
	if err := mk(sendModeSend, row.ID, other).run(ctx); err == nil || !strings.Contains(err.Error(), "changed since the preview") {
		t.Fatalf("send with a changed audience: %v", err)
	}
	if err := mk(sendModeSend, row.ID, recipients).run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(sub.calls) != 2 || len(sub.calls[1].GetEntries()) != 2 || sub.calls[1].GetEntries()[0].GetContent().GetBase().GetGreetingName() != "Jane" {
		t.Fatalf("send submitted %d calls: %v", len(sub.calls), sub.calls)
	}
	if err := mk(sendModeSend, row.ID, recipients).run(ctx); err != nil || len(sub.calls) != 2 || !strings.Contains(out.String(), "already submitted") {
		t.Fatalf("a re-run must submit nothing: %v, %d calls", err, len(sub.calls))
	}
	testDB.Exec("UPDATE comms_requests SET created_at = now() - interval '25 hours' WHERE id = ?", row.ID)
	if err := mk(sendModeSend, row.ID, recipients).run(ctx); err != nil || len(sub.calls) != 2 {
		t.Fatalf("a re-run of a finished send past 24 hours must succeed and submit nothing: %v, %d calls", err, len(sub.calls))
	}

	// A fresh send still needs a fresh preview.
	if err := mk(sendModePreview, "", recipients).run(ctx); err != nil {
		t.Fatal(err)
	}
	stale, _ := d.request.GetByHandle(ctx, org, sub.calls[2].GetIdempotencyKey())
	testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: stale.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	testDB.Exec("UPDATE comms_requests SET created_at = now() - interval '25 hours' WHERE id = ?", stale.ID)
	if err := mk(sendModeSend, stale.ID, recipients).run(ctx); err == nil || !strings.Contains(err.Error(), "older than") {
		t.Fatalf("a fresh send on a stale preview must be refused: %v", err)
	}
}

// failingAfter submits through the inner fake until n calls have been made,
// then fails, as a send cut off midway would.
type failingAfter struct {
	inner *fakeSubmitter
	n     int
}

func (f *failingAfter) SubmitComms(ctx context.Context, req *pb.SubmitCommsRequest, opts ...grpc.CallOption) (*pb.SubmitCommsResponse, error) {
	if len(f.inner.calls) >= f.n {
		return nil, errors.New("connection refused")
	}
	return f.inner.SubmitComms(ctx, req, opts...)
}

// TestSendResumesPastThePreviewAge pins that a send cut off after its first
// chunk can be finished more than 24 hours after the preview: the re-run
// submits only the missing chunk, under the same preview's keys.
func TestSendResumesPastThePreviewAge(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	var lines strings.Builder
	for i := range maxEntries + 1 {
		fmt.Fprintf(&lines, "user%d@example.com\n", i)
	}
	recipients := write(t, "r.txt", lines.String())
	sub := &fakeSubmitter{}
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	mk := func(mode, previewID string, comms submitter) *sendCmd {
		c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: mode, previewID: previewID, d: d, comms: comms, store: memStore{}, now: time.Now, newUUID: uuid.NewString}
		if err := c.validate(recipients, []string{"reviewer@example.com"}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if err := mk(sendModePreview, "", sub).run(ctx); err != nil {
		t.Fatal(err)
	}
	row, _ := d.request.GetByHandle(ctx, org, sub.calls[0].GetIdempotencyKey())
	testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	if err := mk(sendModeSend, row.ID, &failingAfter{inner: sub, n: 2}).run(ctx); err == nil {
		t.Fatal("the cut-off send reported success")
	}
	if len(sub.calls) != 2 {
		t.Fatalf("%d calls after the cut-off send, want the preview and chunk 1", len(sub.calls))
	}
	testDB.Exec("UPDATE comms_requests SET created_at = now() - interval '25 hours' WHERE id = ?", row.ID)
	if err := mk(sendModeSend, row.ID, sub).run(ctx); err != nil {
		t.Fatalf("resuming past 24 hours: %v", err)
	}
	if len(sub.calls) != 3 || len(sub.calls[2].GetEntries()) != 1 || sub.calls[2].GetIdempotencyKey() != sendIdempotencyKey(row.ID, 1, 2) {
		t.Fatalf("the resume submitted %d calls; want only chunk 2 of 2", len(sub.calls)-2)
	}
}

// TestPreviewReachedStatuses pins which preview statuses count as "a reviewer
// received it": SENT, DELIVERED, and UNCONFIRMED only for want of delivery
// feedback. An uncertain send may never have arrived, and DEAD, SKIPPED,
// SUPPRESSED, BOUNCED and the in-flight statuses did not arrive.
func TestPreviewReachedStatuses(t *testing.T) {
	ctx := context.Background()
	str := func(s string) *string { return &s }
	tests := []struct {
		status      pb.MessageStatus
		unconfirmed *string
		reached     bool
	}{
		{pb.MessageStatus_MESSAGE_STATUS_SENT, nil, true},
		{pb.MessageStatus_MESSAGE_STATUS_DELIVERED, nil, true},
		{pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, str("no_delivery_feedback"), true},
		{pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, str("uncertain_send"), false},
		{pb.MessageStatus_MESSAGE_STATUS_DEAD, nil, false},
		{pb.MessageStatus_MESSAGE_STATUS_SKIPPED, nil, false},
		{pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED, nil, false},
		{pb.MessageStatus_MESSAGE_STATUS_BOUNCED, nil, false},
		{pb.MessageStatus_MESSAGE_STATUS_QUEUED, nil, false},
		{pb.MessageStatus_MESSAGE_STATUS_FAILED, nil, false},
	}
	for _, tt := range tests {
		name := tt.status.String()
		if tt.unconfirmed != nil {
			name += "/" + *tt.unconfirmed
		}
		t.Run(name, func(t *testing.T) {
			tx := testDB.Begin()
			t.Cleanup(func() { tx.Rollback() })
			_, req, err := dao.NewRequestRepo(tx).CreateIfAbsent(ctx, &model.CommsRequest{
				OrganizationID: org, IdempotencyKey: "preview-status", Usecase: "notice",
				DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Create(&model.CommsMessage{OrganizationID: org, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP,
				Recipient: "reviewer@example.com", Status: tt.status, UnconfirmedReason: tt.unconfirmed,
				VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED}).Error; err != nil {
				t.Fatal(err)
			}
			c := &sendCmd{org: org, previewID: req.ID, d: &daos{message: dao.NewMessageRepo(tx)}}
			err = c.checkPreviewDelivered(ctx)
			if (err == nil) != tt.reached {
				t.Fatalf("reached = %v, want %v", err == nil, tt.reached)
			}
		})
	}
}

// TestRenderSendRefusedInProduction pins the prod guard on render --send: any
// casing of "prod" or "production" refuses the send before anything goes out.
func TestRenderSendRefusedInProduction(t *testing.T) {
	file := write(t, "c.json", `{"base":{"subject":"Quarterly numbers"}}`)
	for _, envName := range []string{"prod", "PROD", "Prod", "production", "PRODUCTION", " production ", "production-eu", "prod_us", "prod.eu", "production/eu"} {
		t.Run(envName, func(t *testing.T) {
			cfg := defaults(t)
			cfg.Env = envName
			e, _, errOut := newEnv(t, cfg)
			if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file, "--send", "--to", "x@example.com"}); code != 1 || !strings.Contains(errOut.String(), "render --send is refused when OUTBOX_COMMS_ENV") {
				t.Fatalf("OUTBOX_COMMS_ENV=%q: exit %d, stderr %q", envName, code, errOut.String())
			}
		})
	}
}

// TestSendRefusesRenamedAudience pins that the preview's fingerprint covers
// recipient names as well as addresses: renaming a recipient after the preview
// refuses the send.
func TestSendRefusesRenamedAudience(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	before := write(t, "r.txt", "jane@example.com,Jane\n")
	renamed := write(t, "r2.txt", "jane@example.com,Janet\n")
	sub := &fakeSubmitter{}
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	mk := func(mode, previewID, recPath string) *sendCmd {
		c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: mode, previewID: previewID, d: d, comms: sub, store: memStore{}, now: time.Now, newUUID: uuid.NewString}
		if err := c.validate(recPath, []string{"reviewer@example.com"}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if err := mk(sendModePreview, "", before).run(ctx); err != nil {
		t.Fatal(err)
	}
	row, _ := d.request.GetByHandle(ctx, org, sub.calls[0].GetIdempotencyKey())
	testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	if err := mk(sendModeSend, row.ID, renamed).run(ctx); err == nil || !strings.Contains(err.Error(), "changed since the preview") {
		t.Fatalf("a renamed recipient must refuse the send, got %v (calls %d)", err, len(sub.calls))
	}
}

// smtpRelay is a local SMTP server that accepts every message and counts it.
func smtpRelay(t *testing.T) (addr string, received *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	received = &atomic.Int32{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				say := func(s string) { _, _ = fmt.Fprintf(c, "%s\r\n", s) }
				say("220 relay.example.com")
				inData := false
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					cmd := strings.ToUpper(strings.TrimSpace(line))
					switch {
					case inData && line == ".\r\n":
						inData = false
						received.Add(1)
						say("250 queued")
					case inData:
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
			}(conn)
		}
	}()
	return ln.Addr().String(), received
}

// TestRenderSendOutsideProduction pins the other side of the prod guard: in any
// environment that is not prod or production, render --send delivers once.
func TestRenderSendOutsideProduction(t *testing.T) {
	file := write(t, "c.json", `{"base":{"subject":"Quarterly numbers"}}`)
	for _, envName := range []string{"staging", "", "product-demo", "nonproduction"} {
		t.Run("env="+envName, func(t *testing.T) {
			addr, received := smtpRelay(t)
			cfg := defaults(t)
			cfg.Env = envName
			cfg.SMTPAddr = addr
			e, out, errOut := newEnv(t, cfg)
			if code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file, "--send", "--to", "x@example.com"}); code != 0 {
				t.Fatalf("exit %d, stderr %q", code, errOut.String())
			}
			if !strings.Contains(out.String(), "sent to x***@example.com") || received.Load() != 1 {
				t.Fatalf("stdout %q, relay got %d messages", out.String(), received.Load())
			}
		})
	}
}

// TestSendAudienceFingerprint pins what the preview's fingerprint compares: a
// swapped address with the same count and names refuses the send, and the same
// recipients in another order are the same audience.
func TestSendAudienceFingerprint(t *testing.T) {
	for _, tt := range []struct {
		name, send string
		refused    bool
	}{
		{"swapped address", "jane@example.com,Jane\njoan@example.com,John\n", true},
		{"reordered lines", "john@example.com,John\njane@example.com,Jane\n", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			t.Cleanup(func() {
				testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
				testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
			})
			contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
			preview := write(t, "r.txt", "jane@example.com,Jane\njohn@example.com,John\n")
			sendList := write(t, "r2.txt", tt.send)
			sub := &fakeSubmitter{}
			e, _, _ := newEnv(t, defaults(t))
			d, _, _ := e.openDAOs()
			mk := func(mode, previewID, recPath string) *sendCmd {
				c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: mode, previewID: previewID, d: d, comms: sub, store: memStore{}, now: time.Now, newUUID: uuid.NewString}
				if err := c.validate(recPath, []string{"reviewer@example.com"}); err != nil {
					t.Fatal(err)
				}
				return c
			}
			if err := mk(sendModePreview, "", preview).run(ctx); err != nil {
				t.Fatal(err)
			}
			row, _ := d.request.GetByHandle(ctx, org, sub.calls[0].GetIdempotencyKey())
			testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
			err := mk(sendModeSend, row.ID, sendList).run(ctx)
			if tt.refused && (err == nil || !strings.Contains(err.Error(), "changed since the preview")) {
				t.Fatalf("want a refusal, got %v", err)
			}
			if !tt.refused && (err != nil || len(sub.calls) != 2) {
				t.Fatalf("want the send submitted, got %v (%d calls)", err, len(sub.calls))
			}
		})
	}
}

// fakeSES is a fake SES endpoint that reads each request whole, counts it, and
// answers it as mode says: "ok" answers 200, "503" answers 503 every time, and
// "close-first" closes the first connection without answering and answers 200
// after that.
func fakeSES(t *testing.T, mode string) (endpoint string, calls *atomic.Int32) {
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
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				length := 0
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok && strings.EqualFold(k, "Content-Length") {
						length, _ = strconv.Atoi(strings.TrimSpace(v))
					}
					if line == "\r\n" {
						break
					}
				}
				if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
					return
				}
				n := calls.Add(1)
				code, body := 200, `<SendEmailResponse><SendEmailResult><MessageId>0100-ses-`+strconv.Itoa(int(n))+`</MessageId></SendEmailResult></SendEmailResponse>`
				switch {
				case mode == "close-first" && n == 1:
					return
				case mode == "503":
					code, body = 503, ""
				}
				_, _ = fmt.Fprintf(c, "HTTP/1.1 %d X\r\nContent-Type: text/xml\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", code, len(body), body)
			}(conn)
		}
	}()
	return "http://" + ln.Addr().String(), calls
}

// TestRenderSendMakesOneSESCall pins that admin render --send uses the same SES
// client as the service, with the SDK's retries off, so the address receives
// at most one message. SES answering at once is a success. A first request
// that gets no answer, or a 503 every time, is an uncertain send that fails the
// command after exactly one request.
func TestRenderSendMakesOneSESCall(t *testing.T) {
	file := write(t, "c.json", `{"base":{"subject":"Quarterly numbers"}}`)
	for _, tc := range []struct {
		mode    string
		code    int
		output  string
		inError bool
	}{
		{"ok", 0, "sent to x***@example.com, provider id 0100-ses-1", false},
		{"close-first", 1, "outcome=uncertain", true},
		{"503", 1, "outcome=uncertain", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			endpoint, calls := fakeSES(t, tc.mode)
			prev := server.LoadAWSConfig
			t.Cleanup(func() { server.LoadAWSConfig = prev })
			server.LoadAWSConfig = func(context.Context) (aws.Config, error) {
				return aws.Config{Region: "us-east-1", BaseEndpoint: aws.String(endpoint),
					Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
						return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
					})}, nil
			}
			cfg := defaults(t)
			cfg.Env = "staging"
			cfg.Email = "ses"
			e, out, errOut := newEnv(t, cfg)
			code := e.run(context.Background(), []string{"render", "--usecase", "notice", "--channel", "email", "--content", file, "--send", "--to", "x@example.com"})
			got := out.String()
			if tc.inError {
				got = errOut.String()
			}
			if code != tc.code || !strings.Contains(got, tc.output) || calls.Load() != 1 {
				t.Fatalf("exit %d, stdout %q, stderr %q, SES read %d; want exit %d, %q, 1 request", code, out.String(), errOut.String(), calls.Load(), tc.code, tc.output)
			}
		})
	}
}

// TestSendOrgInAnyCase pins that --org is stored and compared in its canonical
// lowercase form: a preview and a send both given the uppercase id go through,
// and a braced id is a usage error before anything is submitted.
func TestSendOrgInAnyCase(t *testing.T) {
	ctx := context.Background()
	const lower = "abcdef01-2345-4678-9abc-def012345678"
	upper := strings.ToUpper(lower)
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", lower)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", lower)
	})
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	recipients := write(t, "r.txt", "jane@example.com,Jane\n")
	sub := &fakeSubmitter{}
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	mk := func(orgArg, mode, previewID string) (*sendCmd, error) {
		c := &sendCmd{e: e, org: orgArg, usecase: "notice", contentPath: contentPath, mode: mode, previewID: previewID, d: d, comms: sub, store: memStore{}, now: time.Now, newUUID: uuid.NewString}
		return c, c.validate(recipients, []string{"reviewer@example.com"})
	}
	if _, err := mk("{"+lower+"}", sendModePreview, ""); err == nil || !strings.Contains(err.Error(), "36-character") {
		t.Fatalf("braced --org: %v, want a usage error", err)
	}
	pc, err := mk(upper, sendModePreview, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.run(ctx); err != nil {
		t.Fatalf("preview with %s: %v", upper, err)
	}
	if got := sub.calls[0].GetOrganizationId(); got != lower {
		t.Fatalf("preview submitted for %q, want %q", got, lower)
	}
	row, err := d.request.GetByHandle(ctx, lower, sub.calls[0].GetIdempotencyKey())
	if err != nil {
		t.Fatal(err)
	}
	testDB.Create(&model.CommsMessage{OrganizationID: lower, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	sc, err := mk(upper, sendModeSend, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.run(ctx); err != nil {
		t.Fatalf("send with %s: %v", upper, err)
	}
	if len(sub.calls) != 2 || sub.calls[1].GetOrganizationId() != lower {
		t.Fatalf("send submitted %d calls, org %q; want 2, %q", len(sub.calls), sub.calls[len(sub.calls)-1].GetOrganizationId(), lower)
	}
}

// raceDAO lists like the real one, then lets another row die before the reset,
// as a send failing between `redrive-dead`'s listing and its reset would.
type raceDAO struct {
	dao.IReconcileDAO
	afterList func()
}

func (r raceDAO) ListDead(ctx context.Context, orgID, reason string, limit int) ([]*model.CommsMessage, error) {
	rows, err := r.IReconcileDAO.ListDead(ctx, orgID, reason, limit)
	r.afterList()
	return rows, err
}

// TestRedriveResetsOnlyTheListedRows pins that --execute resets exactly the
// rows it listed: one that dies after the listing stays DEAD.
func TestRedriveResetsOnlyTheListedRows(t *testing.T) {
	listed := seedDead(t, "listed@example.com", "transient_exhausted")
	var late string
	e, out, _ := newEnv(t, defaults(t))
	open := e.openDAOs
	e.openDAOs = func() (*daos, func(), error) {
		d, closeFn, err := open()
		if err != nil {
			return d, closeFn, err
		}
		d.reconcile = raceDAO{IReconcileDAO: d.reconcile, afterList: func() { late = seedDead(t, "late@example.com", "transient_exhausted") }}
		return d, closeFn, nil
	}
	if code := e.run(context.Background(), []string{"redrive-dead", "--reason", "transient_exhausted", "--execute"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	var a, b string
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", listed).Scan(&a)
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", late).Scan(&b)
	if a != "MESSAGE_STATUS_QUEUED" || b != "MESSAGE_STATUS_DEAD" || !strings.Contains(out.String(), "reset 1 message(s)") {
		t.Fatalf("listed %s, late %s, output %q; want QUEUED, DEAD and one reset", a, b, out.String())
	}
}

// TestSendAttachmentLandsUnderTheOrg pins that admin send stores the file it
// attaches inside the sending organization's folder, and that the preview's
// attachment_key resolves there and nowhere else.
func TestSendAttachmentLandsUnderTheOrg(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	root := t.TempDir()
	store := attachments.Dir{Root: root}
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	recipients := write(t, "r.txt", "jane@example.com\n")
	file := write(t, "plan.pdf", "%PDF-plan")
	sub := &fakeSubmitter{}
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: sendModePreview, attachPath: file, attachName: "plan.pdf",
		d: d, comms: sub, store: store, now: time.Now, newUUID: func() string { return "u1" }}
	if err := c.validate(recipients, []string{"reviewer@example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := c.run(ctx); err != nil {
		t.Fatal(err)
	}
	key := sub.calls[0].GetParams().GetFields()["attachment_key"].GetStringValue()
	if key != "adhoc/u1/plan.pdf" {
		t.Fatalf("attachment_key %q, want adhoc/u1/plan.pdf", key)
	}
	if b, err := os.ReadFile(filepath.Join(root, org, "adhoc", "u1", "plan.pdf")); err != nil || string(b) != "%PDF-plan" {
		t.Fatalf("the file is not under the organization's folder: %q, %v", b, err)
	}
	if b, err := store.Get(ctx, org, key); err != nil || string(b) != "%PDF-plan" {
		t.Fatalf("the key does not resolve for its organization: %q, %v", b, err)
	}
	if _, err := store.Get(ctx, "22222222-2222-2222-2222-222222222222", key); err == nil {
		t.Fatal("the key resolved for another organization")
	}
}

// TestSendAttachmentRefusesAPlantedLink pins that admin send cannot be steered
// into another organization's folder: with the organization's adhoc folder
// linked to a sibling's, the preview fails and the sibling's folder stays empty.
func TestSendAttachmentRefusesAPlantedLink(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	root := t.TempDir()
	sibling := filepath.Join(root, "22222222-2222-2222-2222-222222222222")
	if err := os.MkdirAll(filepath.Join(root, org), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sibling, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sibling, filepath.Join(root, org, "adhoc")); err != nil {
		t.Fatal(err)
	}
	contentPath := write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	recipients := write(t, "r.txt", "jane@example.com\n")
	file := write(t, "plan.pdf", "%PDF-plan")
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ := e.openDAOs()
	c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: contentPath, mode: sendModePreview, attachPath: file, attachName: "plan.pdf",
		d: d, comms: &fakeSubmitter{}, store: attachments.Dir{Root: root}, now: time.Now, newUUID: func() string { return "u1" }}
	if err := c.validate(recipients, []string{"reviewer@example.com"}); err != nil {
		t.Fatal(err)
	}
	if err := c.run(ctx); err == nil || !strings.Contains(err.Error(), "invalid key") {
		t.Fatalf("a preview through a planted link: %v, want refused", err)
	}
	if entries, _ := os.ReadDir(sibling); len(entries) != 0 {
		t.Fatalf("the sibling's folder now holds %d entr(ies)", len(entries))
	}
}
