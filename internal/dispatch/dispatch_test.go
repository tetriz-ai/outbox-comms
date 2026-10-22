// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dispatch_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"

	"github.com/prometheus/client_golang/prometheus"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

const testOrg = "11111111-1111-1111-1111-111111111111"

type fakeSender struct{ res dispatch.SendResult }

func (f fakeSender) Send(context.Context, dispatch.SendInput) dispatch.SendResult { return f.res }

type fakeAuthReporter struct{ calls int }

func (f *fakeAuthReporter) ReportGrantInvalid(context.Context, string, string) error {
	f.calls++
	return nil
}

// fakeRetryScheduler records the delayed-retry arming the dispatcher performs
// after MarkFailed so a test can assert the (attempts, processAt) it passes.
type fakeRetryScheduler struct {
	calls   int
	lastMsg string
	lastAtt int
	lastAt  time.Time
}

func (f *fakeRetryScheduler) Schedule(_ context.Context, _, messageID string, attempts int, processAt time.Time) error {
	f.calls++
	f.lastMsg = messageID
	f.lastAtt = attempts
	f.lastAt = processAt
	return nil
}

type fixture struct {
	tx       *gorm.DB
	reqRepo  *dao.RequestRepo
	msgRepo  *dao.MessageRepo
	supRepo  *dao.SuppressionRepo
	evRepo   *dao.DeliveryEventRepo
	reporter *fakeAuthReporter
	retry    *fakeRetryScheduler
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	return &fixture{
		tx: tx, reqRepo: dao.NewRequestRepo(tx), msgRepo: dao.NewMessageRepo(tx),
		supRepo: dao.NewSuppressionRepo(testDB), evRepo: dao.NewDeliveryEventRepo(tx),
		reporter: &fakeAuthReporter{}, retry: &fakeRetryScheduler{},
	}
}

func (f *fixture) dispatcher(sender dispatch.Sender, ch pb.Channel) *dispatch.Dispatcher {
	return dispatch.NewDispatcher(f.msgRepo, f.reqRepo, f.supRepo, f.evRepo,
		map[pb.Channel]dispatch.Sender{ch: sender},
		dispatch.DefaultRetryPolicy(), f.reporter, f.retry,
		func() time.Time { return time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC) },
		templates.EmailChromeOptions{BrandName: "Example Corp", PreferencesURL: "https://app.example.com/preferences"})
}

// seedQueued creates a notice request + one QUEUED message and returns its id.
func (f *fixture) seedQueued(t *testing.T, key string, ch pb.Channel, vendor pb.Vendor, guarantee pb.DeliveryGuarantee, usecase string) string {
	t.Helper()
	return f.seedQueuedFor(t, testOrg, key, ch, vendor, guarantee, usecase)
}

// seedQueuedFor is seedQueued for a given organization.
func (f *fixture) seedQueuedFor(t *testing.T, org, key string, ch pb.Channel, vendor pb.Vendor, guarantee pb.DeliveryGuarantee, usecase string) string {
	t.Helper()
	ctx := context.Background()
	_, req, err := f.reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{
		OrganizationID: org, IdempotencyKey: key, Usecase: usecase,
		DeliveryGuarantee: guarantee, Status: pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
		Params: &structpb.Struct{},
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	msg := &model.CommsMessage{
		OrganizationID: org, RequestID: req.ID, Channel: ch, Vendor: vendor,
		Recipient: "dev@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: guarantee,
	}
	if _, err := f.msgRepo.MaterializeBatch(ctx, org, []*model.CommsMessage{msg}); err != nil {
		t.Fatalf("seed message: %v", err)
	}
	q, _ := f.msgRepo.ListQueuedForRequest(ctx, org, req.ID)
	return q[0].ID
}

// typedUsecase is a test usecase with a code-side renderer over free-form params,
// exercising the typed-renderer path (a request without block content).
const typedUsecase = "test.typed"

func init() {
	render := func(params proto.Message) (*pb.RenderedPayload, error) {
		title := params.(*structpb.Struct).GetFields()["title"].GetStringValue()
		if title == "" {
			return nil, errors.New("title is required")
		}
		return &pb.RenderedPayload{EmailSubject: title, EmailHtml: "<p>" + title + "</p>", SlackBlocksJson: `[{"type":"section","text":{"type":"mrkdwn","text":"` + title + `"}}]`}, nil
	}
	templates.MustRegister(typedUsecase, templates.Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Renderers: map[templates.ChannelVendor]templates.Renderer{
			{Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES}:   render,
			{Channel: pb.Channel_CHANNEL_SLACK, Vendor: pb.Vendor_VENDOR_SLACK}: render,
		},
	})
}

func (f *fixture) status(t *testing.T, id string) *model.CommsMessage {
	t.Helper()
	m, err := f.msgRepo.GetByID(context.Background(), testOrg, id)
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	return m
}

// TestDispatchOutcomes table-drives the outcome classification to status mapping.
func TestDispatchOutcomes(t *testing.T) {
	const noticeJSON = `{"title":"Welcome"}`
	const reportJSON = `{"title":"Q2 report"}`
	notice := typedUsecase
	report := typedUsecase
	tests := []struct {
		name        string
		channel     pb.Channel
		vendor      pb.Vendor
		usecase     string
		paramsJSON  string
		guarantee   pb.DeliveryGuarantee
		outcome     dispatch.Outcome
		wantStatus  pb.MessageStatus
		wantAuthRpt bool
	}{
		{"email success → SENT", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomeSuccess, pb.MessageStatus_MESSAGE_STATUS_SENT, false},
		{"slack success → DELIVERED", pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, report, reportJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomeSuccess, pb.MessageStatus_MESSAGE_STATUS_DELIVERED, false},
		{"transient → FAILED", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomeTransient, pb.MessageStatus_MESSAGE_STATUS_FAILED, false},
		{"transient BEST_EFFORT → FAILED", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, dispatch.OutcomeTransient, pb.MessageStatus_MESSAGE_STATUS_FAILED, false},
		{"permanent → DEAD", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomePermanent, pb.MessageStatus_MESSAGE_STATUS_DEAD, false},
		{"auth → DEAD + report", pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, report, reportJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomeAuth, pb.MessageStatus_MESSAGE_STATUS_DEAD, true},
		{"uncertain GUARANTEED → FAILED", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, dispatch.OutcomeUncertain, pb.MessageStatus_MESSAGE_STATUS_FAILED, false},
		{"uncertain BEST_EFFORT → UNCONFIRMED", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, notice, noticeJSON, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, dispatch.OutcomeUncertain, pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			id := f.seedQueued(t, "k-"+tt.name, tt.channel, tt.vendor, tt.guarantee, tt.usecase)
			m := f.status(t, id)
			f.tx.Exec(`UPDATE comms_requests SET params = ? WHERE id = ?`, tt.paramsJSON, m.RequestID)

			moved := map[string]string{"usecase": tt.usecase, "channel": tt.channel.String(), "to_status": tt.wantStatus.String()}
			movedBefore := counter(t, "outbox_comms_message_transitions_total", moved)
			deadBefore := map[string]float64{}
			for _, class := range []string{"permanent_config", "transient_exhausted", "render_bug"} {
				deadBefore[class] = counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": class})
			}
			uncertainBefore := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "uncertain_send"})

			d := f.dispatcher(fakeSender{res: dispatch.SendResult{ProviderMessageID: "pmid-1", Outcome: tt.outcome, Err: errors.New("x")}}, tt.channel)
			if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			got := f.status(t, id)
			if got.Status != tt.wantStatus {
				t.Fatalf("status = %v, want %v", got.Status, tt.wantStatus)
			}
			// Each outcome is counted once, under its own status and reason.
			if n := counter(t, "outbox_comms_message_transitions_total", moved); n != movedBefore+1 {
				t.Errorf("transitions{%v} went %v -> %v, want +1", moved, movedBefore, n)
			}
			for class, before := range deadBefore {
				want := before
				if tt.wantStatus == pb.MessageStatus_MESSAGE_STATUS_DEAD && class == "permanent_config" {
					want++
				}
				if n := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": class}); n != want {
					t.Errorf("dead_total{%s} went %v -> %v, want %v", class, before, n, want)
				}
			}
			wantUncertain := uncertainBefore
			if tt.wantStatus == pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED {
				wantUncertain++
			}
			if n := counter(t, "outbox_comms_unconfirmed_total", map[string]string{"reason": "uncertain_send"}); n != wantUncertain {
				t.Errorf("unconfirmed_total{uncertain_send} went %v -> %v, want %v", uncertainBefore, n, wantUncertain)
			}
			if tt.wantStatus == pb.MessageStatus_MESSAGE_STATUS_FAILED {
				if got.Attempts != 1 || got.NextRetryAt == nil {
					t.Errorf("FAILED row must have attempts=1 + next_retry_at, got attempts=%d retry=%v", got.Attempts, got.NextRetryAt)
				}
				// A retryable failure arms exactly one delayed retry at
				// next_retry_at with attempts=1 (the deterministic id's counter).
				if f.retry.calls != 1 {
					t.Errorf("retry scheduler calls = %d, want 1", f.retry.calls)
				} else {
					if f.retry.lastAtt != 1 {
						t.Errorf("retry armed with attempts=%d, want 1", f.retry.lastAtt)
					}
					if got.NextRetryAt != nil && !f.retry.lastAt.Equal(*got.NextRetryAt) {
						t.Errorf("retry processAt=%v, want next_retry_at=%v", f.retry.lastAt, *got.NextRetryAt)
					}
				}
			} else if f.retry.calls != 0 {
				t.Errorf("non-FAILED outcome must not arm a retry, got calls=%d", f.retry.calls)
			}
			if (f.reporter.calls > 0) != tt.wantAuthRpt {
				t.Errorf("auth-report calls=%d, wantReport=%v", f.reporter.calls, tt.wantAuthRpt)
			}
		})
	}
}

// TestDispatchSuppressed proves a live suppression terminates the email send
// SUPPRESSED before any provider call, for every organization: a suppression
// names an address, not an organization, so one recorded while serving one
// organization blocks the same address for another.
func TestDispatchSuppressed(t *testing.T) {
	const otherOrg = "33333333-3333-3333-3333-333333333333"
	for _, org := range []string{testOrg, otherOrg} {
		t.Run(org, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			email := fmt.Sprintf("sup-%d@example.com", time.Now().UnixNano())
			t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
			// The bounce that suppressed the address came from a testOrg send.
			if err := f.supRepo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
				t.Fatalf("suppress: %v", err)
			}
			id := f.seedQueuedFor(t, org, "k-sup", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
			f.tx.Exec("UPDATE comms_messages SET recipient = ? WHERE id = ?", email, id)

			sender := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}}
			d := f.dispatcher(sender, pb.Channel_CHANNEL_EMAIL)
			if err := d.Dispatch(ctx, org, id); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			var got model.CommsMessage
			f.tx.Where("id = ?", id).First(&got)
			if got.Status != pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED {
				t.Fatalf("status = %v, want SUPPRESSED", got.Status)
			}
			if sender.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", sender.calls)
			}
		})
	}
}

// countingSender is a fake Sender that counts its calls.
type countingSender struct {
	res   dispatch.SendResult
	calls int
}

func (s *countingSender) Send(context.Context, dispatch.SendInput) dispatch.SendResult {
	s.calls++
	return s.res
}

// TestDispatchRenderBug proves a render failure terminates DEAD(render_bug), not
// FAILED (it fails identically every retry). digest has no renderer and the
// message carries no content.
func TestDispatchRenderBug(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-render", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, templates.UsecaseDigest)
	before := deadMetric(t, "render_bug")
	d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}}, pb.Channel_CHANNEL_EMAIL)
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	got := f.status(t, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_DEAD || got.DeadReason == nil || *got.DeadReason != "render_bug" {
		t.Fatalf("want DEAD(render_bug), got status=%v reason=%v", got.Status, got.DeadReason)
	}
	if after := deadMetric(t, "render_bug"); after != before+1 {
		t.Fatalf("outbox_comms_dead_total{reason_class=render_bug} went %v -> %v, want +1", before, after)
	}
}

// deadMetric reads outbox_comms_dead_total for one reason class from the
// default registry, or 0 before its first increment.
func deadMetric(t *testing.T, class string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "outbox_comms_dead_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason_class" && l.GetValue() == class {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestDispatchCASMiss proves a redelivery for an already-advanced row is a no-op
// (the at-least-once idempotency guard).
func TestDispatchCASMiss(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-cas", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	// Pre-advance the row to SENT (a prior worker delivered it).
	ok, claimed, err := f.msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if !ok || err != nil {
		t.Fatalf("pre-claim: %v %v", ok, err)
	}
	if err := f.msgRepo.MarkSent(ctx, testOrg, id, *claimed.ClaimID, "pmid-prior"); err != nil {
		t.Fatalf("pre-advance: %v", err)
	}
	d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-new"}}, pb.Channel_CHANNEL_EMAIL)
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	got := f.status(t, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || got.ProviderMessageID == nil || *got.ProviderMessageID != "pmid-prior" {
		t.Fatalf("CAS miss must not re-send: status=%v pmid=%v", got.Status, got.ProviderMessageID)
	}
}

// overrideUsecase retries twice at most, a second apart.
const overrideUsecase = "test.override"

func init() {
	templates.MustRegister(overrideUsecase, templates.Contract{
		Category:            pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL,
		EnabledChannels:     []pb.Channel{pb.Channel_CHANNEL_EMAIL},
		DefaultVerification: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DefaultGuarantee:    pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		RetryOverride:       &templates.RetryPolicy{Ladder: []time.Duration{time.Second}, MaxAttempts: 2},
	})
}

// TestRetryOverride proves the usecase's RetryOverride wins over the global
// policy: its ladder sets next_retry_at, and its cap ends the message DEAD.
func TestRetryOverride(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-override", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, overrideUsecase)
	f.tx.Exec(`UPDATE comms_messages SET content = ? WHERE id = ?`, `{"subject":"s","blocks":[{"paragraph":{"text":"b"}}]}`, id)
	d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}}, pb.Channel_CHANNEL_EMAIL)
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	got := f.status(t, id)
	fixed := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_FAILED || got.NextRetryAt == nil || !got.NextRetryAt.Equal(fixed.Add(time.Second)) {
		t.Fatalf("first failure: status=%v next=%v, want FAILED at +1s (the override ladder)", got.Status, got.NextRetryAt)
	}
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_DEAD || *got.DeadReason != "transient_exhausted" {
		t.Fatalf("second failure: status=%v, want DEAD(transient_exhausted) under MaxAttempts=2", got.Status)
	}
}

// TestDispatchStaleIsNoOp proves an attempt whose row moved on mid-send (the
// reconciler took it) leaves the other actor's state and returns nil.
func TestDispatchStaleIsNoOp(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-stale", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"t"}' WHERE id = (SELECT request_id FROM comms_messages WHERE id = ?)`, id)
	moved := senderFunc(func() dispatch.SendResult {
		f.tx.Exec(`UPDATE comms_messages SET status = 'MESSAGE_STATUS_UNCONFIRMED', unconfirmed_reason = 'uncertain_send' WHERE id = ?`, id)
		return dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}
	})
	if err := f.dispatcher(moved, pb.Channel_CHANNEL_EMAIL).Dispatch(ctx, testOrg, id); err != nil {
		t.Fatalf("want nil, got %v", err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED {
		t.Fatalf("status = %v, want the reconciler's UNCONFIRMED to stand", got.Status)
	}
	if f.retry.calls != 0 {
		t.Fatal("a stale attempt must not arm a retry")
	}
}

type senderFunc func() dispatch.SendResult

func (f senderFunc) Send(context.Context, dispatch.SendInput) dispatch.SendResult { return f() }

func histCount(t *testing.T, name, channel string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "channel" && l.GetValue() == channel {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

// TestDispatchRecordsTimings proves a send records the provider-call duration
// and, on acceptance, the creation-to-acceptance latency.
func TestDispatchRecordsTimings(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-timing", pb.Channel_CHANNEL_SLACK, pb.Vendor_VENDOR_SLACK, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"t"}' WHERE id = (SELECT request_id FROM comms_messages WHERE id = ?)`, id)
	durBefore := histCount(t, "outbox_comms_dispatch_duration_seconds", "CHANNEL_SLACK")
	latBefore := histCount(t, "outbox_comms_delivery_latency_seconds", "CHANNEL_SLACK")
	if err := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "ts"}}, pb.Channel_CHANNEL_SLACK).Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if histCount(t, "outbox_comms_dispatch_duration_seconds", "CHANNEL_SLACK") != durBefore+1 {
		t.Error("dispatch duration not recorded")
	}
	if histCount(t, "outbox_comms_delivery_latency_seconds", "CHANNEL_SLACK") != latBefore+1 {
		t.Error("delivery latency not recorded")
	}
}

// TestRetryLadderStep pins which ladder step a failure waits: the row's
// attempts so far pick the step, and the last step repeats once the ladder runs
// out before the attempt cap.
func TestRetryLadderStep(t *testing.T) {
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	for attempts, want := range []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour} {
		t.Run(want.String(), func(t *testing.T) {
			f := newFixture(t)
			id := f.seedQueued(t, fmt.Sprint("k-ladder-", attempts), pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
			f.tx.Exec("UPDATE comms_messages SET attempts = ? WHERE id = ?", attempts, id)
			f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
			d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}}, pb.Channel_CHANNEL_EMAIL)
			if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
				t.Fatal(err)
			}
			got := f.status(t, id)
			if got.Status != pb.MessageStatus_MESSAGE_STATUS_FAILED || got.NextRetryAt == nil {
				t.Fatalf("status = %v, next_retry_at = %v", got.Status, got.NextRetryAt)
			}
			if wait := got.NextRetryAt.Sub(now); wait != want {
				t.Fatalf("attempts %d waits %v, want %v", attempts, wait, want)
			}
		})
	}
}

// TestRetryAfterFromProvider pins how a provider's requested wait combines with
// the ladder: the retry waits the later of the two.
func TestRetryAfterFromProvider(t *testing.T) {
	now := time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		retryAfter, want time.Duration
	}{{0, time.Minute}, {30 * time.Second, time.Minute}, {10 * time.Minute, 10 * time.Minute}} {
		t.Run(tt.retryAfter.String(), func(t *testing.T) {
			f := newFixture(t)
			id := f.seedQueued(t, "k-retry-after-"+tt.retryAfter.String(), pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
			f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
			d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("429"), RetryAfter: tt.retryAfter}}, pb.Channel_CHANNEL_EMAIL)
			if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
				t.Fatal(err)
			}
			got := f.status(t, id)
			if got.NextRetryAt == nil || got.NextRetryAt.Sub(now) != tt.want {
				t.Fatalf("next_retry_at = %v, want now + %v", got.NextRetryAt, tt.want)
			}
		})
	}
}

// flakyRequests fails GetByID the first time, as a dropped database connection
// would, then behaves.
type flakyRequests struct {
	dao.IRequestDAO
	failed bool
}

func (f *flakyRequests) GetByID(ctx context.Context, orgID, id string) (*model.CommsRequest, error) {
	if !f.failed {
		f.failed = true
		return nil, errors.New("connection reset by peer")
	}
	return f.IRequestDAO.GetByID(ctx, orgID, id)
}

// TestFailureBeforeSendReleasesTheClaim pins that an error after the claim but
// before the provider call puts the row back to QUEUED, attempts unchanged, so
// the redelivery sends exactly once. A BEST_EFFORT row must never end
// UNCONFIRMED for a send that never happened.
func TestFailureBeforeSendReleasesTheClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-release", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	send := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-1"}}
	d := dispatch.NewDispatcher(f.msgRepo, &flakyRequests{IRequestDAO: f.reqRepo}, f.supRepo, f.evRepo,
		map[pb.Channel]dispatch.Sender{pb.Channel_CHANNEL_EMAIL: send},
		dispatch.DefaultRetryPolicy(), f.reporter, f.retry,
		func() time.Time { return time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC) },
		templates.EmailChromeOptions{BrandName: "Example Corp"})

	if err := d.Dispatch(ctx, testOrg, id); err == nil {
		t.Fatal("attempt 1 must return the load error")
	}
	got := f.status(t, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED || got.Attempts != 0 {
		t.Fatalf("after the failed attempt: status %v attempts %d, want QUEUED 0", got.Status, got.Attempts)
	}
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || send.calls != 1 {
		t.Fatalf("after the redelivery: status %v, sends %d; want SENT, 1", got.Status, send.calls)
	}
}

// TestTransitionMetricsFailedAndSuppressed pins that a retryable failure and a
// suppression are counted in outbox_comms_message_transitions_total.
func TestTransitionMetricsFailedAndSuppressed(t *testing.T) {
	ctx := context.Background()
	labels := func(to pb.MessageStatus) map[string]string {
		return map[string]string{"usecase": typedUsecase, "channel": "CHANNEL_EMAIL", "to_status": to.String()}
	}
	failedBefore := counter(t, "outbox_comms_message_transitions_total", labels(pb.MessageStatus_MESSAGE_STATUS_FAILED))
	suppressedBefore := counter(t, "outbox_comms_message_transitions_total", labels(pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED))

	f := newFixture(t)
	id := f.seedQueued(t, "k-metric-failed", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	if err := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}}, pb.Channel_CHANNEL_EMAIL).Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}

	email := fmt.Sprintf("metric-sup-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email) })
	if err := f.supRepo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
		t.Fatal(err)
	}
	id2 := f.seedQueued(t, "k-metric-sup", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec("UPDATE comms_messages SET recipient = ? WHERE id = ?", email, id2)
	if err := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}}, pb.Channel_CHANNEL_EMAIL).Dispatch(ctx, testOrg, id2); err != nil {
		t.Fatal(err)
	}
	if got := counter(t, "outbox_comms_message_transitions_total", labels(pb.MessageStatus_MESSAGE_STATUS_FAILED)); got != failedBefore+1 {
		t.Errorf("FAILED transitions went %v -> %v, want +1", failedBefore, got)
	}
	if got := counter(t, "outbox_comms_message_transitions_total", labels(pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED)); got != suppressedBefore+1 {
		t.Errorf("SUPPRESSED transitions went %v -> %v, want +1", suppressedBefore, got)
	}
}

// counter reads one series of a counter from the default registry, matching
// every label given, or 0 before its first increment.
func counter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metric
				}
			}
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// TestExhaustedRetryIsCountedTransientExhausted pins the DEAD reason class of a
// message whose retries ran out: transient_exhausted, not permanent_config,
// which the dead-incident alert keys on.
func TestExhaustedRetryIsCountedTransientExhausted(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-exhausted", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec("UPDATE comms_messages SET attempts = 5 WHERE id = ?", id)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	exhausted := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "transient_exhausted"})
	permanent := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "permanent_config"})
	moved := map[string]string{"usecase": typedUsecase, "channel": "CHANNEL_EMAIL", "to_status": "MESSAGE_STATUS_DEAD"}
	movedBefore := counter(t, "outbox_comms_message_transitions_total", moved)
	d := f.dispatcher(fakeSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeTransient, Err: errors.New("x")}}, pb.Channel_CHANNEL_EMAIL)
	if err := d.Dispatch(context.Background(), testOrg, id); err != nil {
		t.Fatal(err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_DEAD {
		t.Fatalf("status = %v, want DEAD", got.Status)
	}
	if n := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "transient_exhausted"}); n != exhausted+1 {
		t.Errorf("transient_exhausted went %v -> %v, want +1", exhausted, n)
	}
	if n := counter(t, "outbox_comms_dead_total", map[string]string{"reason_class": "permanent_config"}); n != permanent {
		t.Errorf("permanent_config went %v -> %v, want unchanged", permanent, n)
	}
	if n := counter(t, "outbox_comms_message_transitions_total", moved); n != movedBefore+1 {
		t.Errorf("DEAD transitions went %v -> %v, want +1", movedBefore, n)
	}
}

// flakyMessages fails the chosen IMessageDAO calls once, as a dropped
// connection would, then behaves.
type flakyMessages struct {
	dao.IMessageDAO
	failSnapshot, failMarkSent, failRelease bool
}

func (f *flakyMessages) SnapshotRendered(ctx context.Context, orgID, id, claimID string, p *pb.RenderedPayload) error {
	if f.failSnapshot {
		f.failSnapshot = false
		return errors.New("connection reset by peer")
	}
	return f.IMessageDAO.SnapshotRendered(ctx, orgID, id, claimID, p)
}

func (f *flakyMessages) MarkSent(ctx context.Context, orgID, id, claimID, pmid string) error {
	if f.failMarkSent {
		f.failMarkSent = false
		return errors.New("connection reset by peer")
	}
	return f.IMessageDAO.MarkSent(ctx, orgID, id, claimID, pmid)
}

func (f *flakyMessages) ReleaseClaim(ctx context.Context, orgID, id, claimID string) error {
	if f.failRelease {
		return errors.New("connection reset by peer")
	}
	return f.IMessageDAO.ReleaseClaim(ctx, orgID, id, claimID)
}

// flakySuppressions fails the suppression lookup once.
type flakySuppressions struct {
	dao.ISuppressionDAO
	failed bool
}

func (f *flakySuppressions) GetActiveByEmail(ctx context.Context, email string) (*model.CommsSuppression, error) {
	if !f.failed {
		f.failed = true
		return nil, errors.New("connection reset by peer")
	}
	return f.ISuppressionDAO.GetActiveByEmail(ctx, email)
}

// releaseFixture builds a dispatcher over the fixture's stores with the given
// wrappers in front of them.
func releaseDispatcher(f *fixture, msgs dao.IMessageDAO, reqs dao.IRequestDAO, sups dao.ISuppressionDAO, send dispatch.Sender) *dispatch.Dispatcher {
	return dispatch.NewDispatcher(msgs, reqs, sups, f.evRepo,
		map[pb.Channel]dispatch.Sender{pb.Channel_CHANNEL_EMAIL: send},
		dispatch.DefaultRetryPolicy(), f.reporter, f.retry,
		func() time.Time { return time.Date(2026, 6, 28, 12, 0, 0, 0, time.UTC) },
		templates.EmailChromeOptions{BrandName: "Example Corp"})
}

// TestPreSendFailuresReleaseTheClaim pins every failure point before the
// provider call, under both guarantees: the row goes back to QUEUED with its
// attempts unchanged, and the redelivery sends once. A row claimed from FAILED
// keeps its attempts too.
func TestPreSendFailuresReleaseTheClaim(t *testing.T) {
	for _, point := range []string{"request load", "suppression check", "snapshot"} {
		for _, g := range []pb.DeliveryGuarantee{pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED} {
			for _, attempts := range []int{0, 3} {
				t.Run(fmt.Sprintf("%s/%v/attempts=%d", point, g, attempts), func(t *testing.T) {
					ctx := context.Background()
					f := newFixture(t)
					id := f.seedQueued(t, "k-pre-"+point+g.String()+fmt.Sprint(attempts), pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, g, typedUsecase)
					f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
					if attempts > 0 {
						f.tx.Exec("UPDATE comms_messages SET status = 'MESSAGE_STATUS_FAILED', attempts = ?, next_retry_at = now() - interval '1 minute' WHERE id = ?", attempts, id)
					}
					msgs := &flakyMessages{IMessageDAO: f.msgRepo, failSnapshot: point == "snapshot"}
					var reqs dao.IRequestDAO = f.reqRepo
					if point == "request load" {
						reqs = &flakyRequests{IRequestDAO: f.reqRepo}
					}
					var sups dao.ISuppressionDAO = f.supRepo
					if point == "suppression check" {
						sups = &flakySuppressions{ISuppressionDAO: f.supRepo}
					}
					send := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-1"}}
					d := releaseDispatcher(f, msgs, reqs, sups, send)
					if err := d.Dispatch(ctx, testOrg, id); err == nil {
						t.Fatal("the failed attempt must return its error")
					}
					if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED || got.Attempts != attempts || send.calls != 0 {
						t.Fatalf("after the failure: %v attempts %d sends %d, want QUEUED %d 0", got.Status, got.Attempts, send.calls, attempts)
					}
					if err := d.Dispatch(ctx, testOrg, id); err != nil {
						t.Fatal(err)
					}
					if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || send.calls != 1 {
						t.Fatalf("after the redelivery: %v, sends %d; want SENT, 1", got.Status, send.calls)
					}
				})
			}
		}
	}
}

// TestPostSendWriteFailureKeepsTheClaim pins the other side: once the provider
// call has run, a failed status write never releases the row, so the
// redelivery finds it claimed and sends nothing more.
func TestPostSendWriteFailureKeepsTheClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-post-send", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	send := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-1"}}
	d := releaseDispatcher(f, &flakyMessages{IMessageDAO: f.msgRepo, failMarkSent: true}, f.reqRepo, f.supRepo, send)
	if err := d.Dispatch(ctx, testOrg, id); err == nil {
		t.Fatal("the failed status write must return its error")
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENDING {
		t.Fatalf("status = %v, want SENDING", got.Status)
	}
	if err := d.Dispatch(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if send.calls != 1 {
		t.Fatalf("sends = %d, want 1", send.calls)
	}
}

// TestFailedReleaseIsReported pins that a release that itself fails is part of
// the returned error, and the row stays SENDING for reconcile.
func TestFailedReleaseIsReported(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-release-fails", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	send := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}}
	d := releaseDispatcher(f, &flakyMessages{IMessageDAO: f.msgRepo, failRelease: true}, &flakyRequests{IRequestDAO: f.reqRepo}, f.supRepo, send)
	err := d.Dispatch(context.Background(), testOrg, id)
	if err == nil || !strings.Contains(err.Error(), "release claim") {
		t.Fatalf("err = %v, want one naming the release claim", err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENDING {
		t.Fatalf("status = %v, want SENDING", got.Status)
	}
}

// gatedRequests holds the first GetByID until released, then fails it.
type gatedRequests struct {
	dao.IRequestDAO
	entered, proceed chan struct{}
	used             bool
}

func (g *gatedRequests) GetByID(ctx context.Context, orgID, id string) (*model.CommsRequest, error) {
	if !g.used {
		g.used = true
		close(g.entered)
		<-g.proceed
		return nil, errors.New("connection reset by peer")
	}
	return g.IRequestDAO.GetByID(ctx, orgID, id)
}

// gatedSender holds its send until released, then succeeds.
type gatedSender struct {
	entered, proceed chan struct{}
	calls            int
}

func (g *gatedSender) Send(context.Context, dispatch.SendInput) dispatch.SendResult {
	g.calls++
	close(g.entered)
	<-g.proceed
	return dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-b"}
}

// TestStaleWorkerCannotReleaseAnotherClaim pins that a claim belongs to the
// worker that made it. Worker A claims and stalls, reconcile requeues the row,
// worker B claims it and is in the provider call when A's request load fails.
// A's release must not touch B's claim: the row ends SENT by B, after one send.
func TestStaleWorkerCannotReleaseAnotherClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-two-workers", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)

	reqsA := &gatedRequests{IRequestDAO: f.reqRepo, entered: make(chan struct{}), proceed: make(chan struct{})}
	a := releaseDispatcher(f, f.msgRepo, reqsA, f.supRepo, &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}})
	sendB := &gatedSender{entered: make(chan struct{}), proceed: make(chan struct{})}
	b := releaseDispatcher(f, f.msgRepo, f.reqRepo, f.supRepo, sendB)

	aDone := make(chan error, 1)
	go func() { aDone <- a.Dispatch(ctx, testOrg, id) }()
	<-reqsA.entered // A holds the claim and is stalled
	if _, _, err := dao.NewReconcileRepo(f.tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6}); err != nil {
		t.Fatal(err)
	}
	bDone := make(chan error, 1)
	go func() { bDone <- b.Dispatch(ctx, testOrg, id) }()
	<-sendB.entered // B holds the new claim and is in the provider call
	close(reqsA.proceed)
	if err := <-aDone; err == nil {
		t.Fatal("A's failed request load must return its error")
	}
	close(sendB.proceed)
	if err := <-bDone; err != nil {
		t.Fatalf("B: %v", err)
	}
	got := f.status(t, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || got.ProviderMessageID == nil || *got.ProviderMessageID != "pmid-b" || sendB.calls != 1 {
		t.Fatalf("row %v pmid %v, B sends %d; want SENT by B, 1 send", got.Status, got.ProviderMessageID, sendB.calls)
	}
}

// cancellingRequests cancels the dispatch context inside the request load and
// fails the load with the cancellation, as a shutting-down worker would see it.
type cancellingRequests struct {
	dao.IRequestDAO
	cancel context.CancelFunc
}

func (c *cancellingRequests) GetByID(ctx context.Context, _, _ string) (*model.CommsRequest, error) {
	c.cancel()
	return nil, ctx.Err()
}

// TestCancelledAttemptStillReleasesTheClaim pins that the release does not use
// the attempt's context: a worker cancelled before the provider call still
// puts the row back to QUEUED with its attempts unchanged.
func TestCancelledAttemptStillReleasesTheClaim(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-cancelled", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	send := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess}}
	d := releaseDispatcher(f, f.msgRepo, &cancellingRequests{IRequestDAO: f.reqRepo, cancel: cancel}, f.supRepo, send)
	if err := d.Dispatch(ctx, testOrg, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := f.status(t, id); got.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED || got.Attempts != 0 || send.calls != 0 {
		t.Fatalf("row %v attempts %d sends %d, want QUEUED 0 0", got.Status, got.Attempts, send.calls)
	}
}

// stallingRequests holds the first GetByID until released, then answers it.
type stallingRequests struct {
	dao.IRequestDAO
	entered, proceed chan struct{}
	used             bool
}

func (s *stallingRequests) GetByID(ctx context.Context, orgID, id string) (*model.CommsRequest, error) {
	if !s.used {
		s.used = true
		close(s.entered)
		<-s.proceed
	}
	return s.IRequestDAO.GetByID(ctx, orgID, id)
}

// TestStaleWorkerNeverSendsASecondCopy pins the other half of claim ownership.
// Worker A claims and stalls in its request load, reconcile requeues the row,
// and worker B claims it and is in the provider call. A's load then succeeds:
// its claim is gone, so A sends nothing, and B's send is the only one.
func TestStaleWorkerNeverSendsASecondCopy(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	id := f.seedQueued(t, "k-stale-sender", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)

	reqsA := &stallingRequests{IRequestDAO: f.reqRepo, entered: make(chan struct{}), proceed: make(chan struct{})}
	sendA := &countingSender{res: dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-a"}}
	a := releaseDispatcher(f, f.msgRepo, reqsA, f.supRepo, sendA)
	sendB := &gatedSender{entered: make(chan struct{}), proceed: make(chan struct{})}
	b := releaseDispatcher(f, f.msgRepo, f.reqRepo, f.supRepo, sendB)

	aDone := make(chan error, 1)
	go func() { aDone <- a.Dispatch(ctx, testOrg, id) }()
	<-reqsA.entered
	if _, _, err := dao.NewReconcileRepo(f.tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6}); err != nil {
		t.Fatal(err)
	}
	bDone := make(chan error, 1)
	go func() { bDone <- b.Dispatch(ctx, testOrg, id) }()
	<-sendB.entered
	close(reqsA.proceed)
	if err := <-aDone; err != nil {
		t.Fatalf("A: %v", err)
	}
	close(sendB.proceed)
	if err := <-bDone; err != nil {
		t.Fatalf("B: %v", err)
	}
	got := f.status(t, id)
	if sendA.calls != 0 || sendB.calls != 1 || got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || *got.ProviderMessageID != "pmid-b" {
		t.Fatalf("A sent %d, B sent %d, row %v pmid %v; want 0, 1, SENT by B", sendA.calls, sendB.calls, got.Status, got.ProviderMessageID)
	}
}

// deadlineMessages records the deadline the release runs under, then holds the
// release until that context ends, or 10 s.
type deadlineMessages struct {
	dao.IMessageDAO
	deadline time.Duration
	expired  bool
}

func (d *deadlineMessages) ReleaseClaim(ctx context.Context, _, _, _ string) error {
	if dl, ok := ctx.Deadline(); ok {
		d.deadline = time.Until(dl)
	}
	select {
	case <-ctx.Done():
		d.expired = true
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return errors.New("the release context never ended")
	}
}

// TestReleaseIsBounded pins the release's own deadline: at most 5 s, and the
// release context does end, so a hung store cannot hold the worker.
func TestReleaseIsBounded(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-release-bounded", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	msgs := &deadlineMessages{IMessageDAO: f.msgRepo}
	d := releaseDispatcher(f, msgs, &flakyRequests{IRequestDAO: f.reqRepo}, f.supRepo, &countingSender{})
	start := time.Now()
	_ = d.Dispatch(context.Background(), testOrg, id)
	if msgs.deadline <= 0 || msgs.deadline > 5*time.Second || !msgs.expired || time.Since(start) > 7*time.Second {
		t.Fatalf("release deadline %v, expired %v, took %v; want a deadline of at most 5s that ends the release", msgs.deadline, msgs.expired, time.Since(start))
	}
}

// deadlineSender records the deadline its context carries, then waits for
// that context to end, as a provider call that never answers would, and
// reports the send uncertain: the request was written and no answer came.
type deadlineSender struct {
	deadline time.Duration
	has      bool
	calls    int
	block    bool
}

func (s *deadlineSender) Send(ctx context.Context, _ dispatch.SendInput) dispatch.SendResult {
	s.calls++
	if dl, ok := ctx.Deadline(); ok {
		s.has, s.deadline = true, time.Until(dl)
	}
	if s.block {
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
		return dispatch.SendResult{Outcome: dispatch.OutcomeUncertain, Err: ctx.Err()}
	}
	return dispatch.SendResult{Outcome: dispatch.OutcomeSuccess, ProviderMessageID: "pmid-deadline"}
}

// TestProviderCallHasADeadline pins that every provider call runs under a
// deadline, 60 s by default, so a send cannot outlive the stuck-send window and
// be reconciled and sent again while it is still running.
func TestProviderCallHasADeadline(t *testing.T) {
	f := newFixture(t)
	id := f.seedQueued(t, "k-deadline", pb.Channel_CHANNEL_EMAIL, pb.Vendor_VENDOR_SES, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, typedUsecase)
	f.tx.Exec(`UPDATE comms_requests SET params = '{"title":"Welcome"}' WHERE id = ?`, f.status(t, id).RequestID)
	s := &deadlineSender{}
	if err := releaseDispatcher(f, f.msgRepo, f.reqRepo, f.supRepo, s).Dispatch(context.Background(), testOrg, id); err != nil {
		t.Fatal(err)
	}
	if !s.has || s.deadline <= 0 || s.deadline > 60*time.Second {
		t.Fatalf("provider call deadline: present %v, %v left; want at most 60s", s.has, s.deadline)
	}
}
