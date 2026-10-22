// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package fanout_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/fanout"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

const testOrg = "11111111-1111-1111-1111-111111111111"

type fakeOrgGate struct{ active, frozen bool }

func (g fakeOrgGate) CheckOrg(context.Context, string) (bool, bool, error) {
	return g.active, g.frozen, nil
}

type fakeDispatcher struct{ ids []string }

func (d *fakeDispatcher) PublishDispatch(_ context.Context, _, messageID string) error {
	d.ids = append(d.ids, messageID)
	return nil
}

type harness struct {
	tx         *gorm.DB
	fo         *fanout.FanOut
	dispatcher *fakeDispatcher
	reqRepo    *dao.RequestRepo
	msgRepo    *dao.MessageRepo
	prefRepo   *dao.PreferenceRepo
	destRepo   *dao.DestinationRepo
}

func newHarness(t *testing.T, frozen bool) *harness {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	h := &harness{
		tx:         tx,
		dispatcher: &fakeDispatcher{},
		reqRepo:    dao.NewRequestRepo(tx),
		msgRepo:    dao.NewMessageRepo(tx),
		prefRepo:   dao.NewPreferenceRepo(tx),
		destRepo:   dao.NewDestinationRepo(tx),
	}
	h.fo = fanout.NewFanOut(h.reqRepo, h.msgRepo, h.prefRepo, h.destRepo,
		fakeOrgGate{active: true, frozen: frozen}, h.dispatcher, fanout.StaticVendorResolver{Email: pb.Vendor_VENDOR_SES, Slack: pb.Vendor_VENDOR_SLACK})
	return h
}

func (h *harness) seed(t *testing.T, key string, usecase string, guarantee pb.DeliveryGuarantee, entries ...*pb.CommsEntry) string {
	t.Helper()
	_, req, err := h.reqRepo.CreateIfAbsent(context.Background(), &model.CommsRequest{
		OrganizationID:    testOrg,
		IdempotencyKey:    key,
		Usecase:           usecase,
		Entries:           &pb.CommsEntries{Entries: entries},
		DeliveryGuarantee: guarantee,
		Status:            pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
	})
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	return req.ID
}

func emailEntry(addr string) *pb.CommsEntry {
	return &pb.CommsEntry{
		Channel: pb.Channel_CHANNEL_EMAIL, TargetKind: pb.TargetKind_TARGET_KIND_EMAIL_ADDRESS, Recipient: addr,
	}
}

func orgDestEntry() *pb.CommsEntry {
	return &pb.CommsEntry{
		Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
		DestinationScope: &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityId: testOrg},
	}
}

// TestFanOutTransactionalNotice proves a transactional notice materializes one
// QUEUED row, enqueues one dispatch, and marks the request FANNED_OUT.
func TestFanOutTransactionalNotice(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	id := h.seed(t, "notice:p1:1", templates.UsecaseNotice,
		pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, emailEntry("dev@example.com"))

	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Fatalf("want 1 QUEUED row, got %+v", rows)
	}
	if len(h.dispatcher.ids) != 1 {
		t.Errorf("want 1 dispatch enqueue, got %d", len(h.dispatcher.ids))
	}
	req, _ := h.reqRepo.GetByID(ctx, testOrg, id)
	if req.Status != pb.RequestStatus_REQUEST_STATUS_FANNED_OUT {
		t.Errorf("request status = %v, want FANNED_OUT", req.Status)
	}
}

// TestFanOutConfiguredDestination proves a CONFIGURED_DESTINATION entry fans to
// one row for the scope's configured destination_ref.
func TestFanOutConfiguredDestination(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	if err := h.tx.WithContext(ctx).Create(&model.CommsChannelDestination{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK,
		DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0AB12CD3",
	}).Error; err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	id := h.seed(t, "digest:org:W1", templates.UsecaseDigest,
		pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, orgDestEntry())

	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	queued := 0
	for _, r := range rows {
		if r.Status == pb.MessageStatus_MESSAGE_STATUS_QUEUED {
			queued++
		}
	}
	if queued != 1 || len(rows) != 1 || rows[0].Recipient != "C0AB12CD3" {
		t.Fatalf("want 1 QUEUED row to C0AB12CD3, got %d of %+v", queued, rows)
	}
	if len(h.dispatcher.ids) != 1 {
		t.Errorf("want 1 dispatch enqueue, got %d", len(h.dispatcher.ids))
	}
}

// TestFanOutNoDestination proves an unconfigured CONFIGURED_DESTINATION → a
// deliberate SKIPPED(no_destination) row, not a failure, and no dispatch.
func TestFanOutNoDestination(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	id := h.seed(t, "digest:org:W2", templates.UsecaseDigest,
		pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, orgDestEntry())

	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_SKIPPED || skipReason(rows[0]) != "no_destination" {
		t.Fatalf("want 1 SKIPPED(no_destination) row, got %+v", rows)
	}
	if len(h.dispatcher.ids) != 0 {
		t.Errorf("a skipped row must not enqueue, got %d", len(h.dispatcher.ids))
	}
}

// TestFanOutOptedOut proves the product-category gate: an ORG opt-out makes a
// concrete product send SKIPPED(opted_out).
func TestFanOutOptedOut(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	if err := h.prefRepo.Upsert(ctx, &model.CommsPreference{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false,
	}); err != nil {
		t.Fatalf("seed pref: %v", err)
	}
	id := h.seed(t, "digest:user:W3", templates.UsecaseDigest,
		pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, emailEntry("ana@example.com"))

	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_SKIPPED || skipReason(rows[0]) != "opted_out" {
		t.Fatalf("want 1 SKIPPED(opted_out) row, got %+v", rows)
	}
}

// skipReason dereferences the nullable skip_reason for assertions.
func skipReason(m *model.CommsMessage) string {
	if m.SkipReason == nil {
		return ""
	}
	return *m.SkipReason
}

// TestFanOutCrashRedrive proves a re-driven pass no-ops the materialized rows
// (the unique key) and re-emits the dispatch triggers (the re-SELECT) — the
// no-cursor crash-resume invariant.
func TestFanOutCrashRedrive(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	id := h.seed(t, "notice:p1:redrive", templates.UsecaseNotice,
		pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, emailEntry("dev@example.com"))

	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("first process: %v", err)
	}
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("re-drive process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 {
		t.Fatalf("re-drive must not duplicate rows, got %d", len(rows))
	}
	if len(h.dispatcher.ids) != 2 {
		t.Errorf("re-drive must re-emit the dispatch trigger (2 total), got %d", len(h.dispatcher.ids))
	}
}

// TestNoCrossRequestDedup pins that deduplication is per request: the same
// recipient in two requests with different idempotency keys becomes two
// message rows, and so two sends.
func TestNoCrossRequestDedup(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	for _, key := range []string{"part-1", "part-2"} {
		id := h.seed(t, key, templates.UsecaseNotice, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, emailEntry("same@example.com"))
		if err := h.fo.Process(ctx, testOrg, id); err != nil {
			t.Fatalf("process %s: %v", key, err)
		}
	}
	var rows int64
	h.tx.Model(&model.CommsMessage{}).Where("organization_id = ? AND recipient = ?", testOrg, "same@example.com").Count(&rows)
	if rows != 2 {
		t.Fatalf("message rows for the shared recipient = %d, want 2", rows)
	}
	if len(h.dispatcher.ids) != 2 {
		t.Fatalf("dispatch enqueues = %d, want 2", len(h.dispatcher.ids))
	}
}

// TestTransactionalIgnoresPreferences pins that a transactional usecase skips
// the preference gate: an organization and a person both opted out of
// transactional email still get the notice.
func TestTransactionalIgnoresPreferences(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	const person = "44444444-4444-4444-4444-444444444444"
	for _, p := range []*model.CommsPreference{
		{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg, Category: pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false},
		{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityID: person, Category: pb.MessageCategory_MESSAGE_CATEGORY_TRANSACTIONAL, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false},
	} {
		if err := h.prefRepo.Upsert(ctx, p); err != nil {
			t.Fatalf("seed pref: %v", err)
		}
	}
	e := emailEntry("ana@example.com")
	e.SubjectPersonId = person
	id := h.seed(t, "notice:opted-out", templates.UsecaseNotice, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, e)
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatalf("process: %v", err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Fatalf("want 1 QUEUED row, got %+v", rows)
	}
}

// TestTransitionMetricsSkipped pins that a SKIPPED row is counted in
// outbox_comms_message_transitions_total.
func TestTransitionMetricsSkipped(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"usecase": templates.UsecaseAlert, "channel": "CHANNEL_SLACK", "to_status": "MESSAGE_STATUS_SKIPPED"}
	before := counter(t, "outbox_comms_message_transitions_total", labels)
	h := newHarness(t, false)
	id := h.seed(t, "alert:no-dest", templates.UsecaseAlert, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, orgDestEntry())
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	if got := counter(t, "outbox_comms_message_transitions_total", labels); got != before+1 {
		t.Fatalf("SKIPPED transitions went %v -> %v, want +1", before, got)
	}
}

// TestRedriveDoesNotRecountSkips pins that a SKIPPED row is counted once, by
// the pass that inserted it: a re-driven fan-out adds nothing, and a frozen
// re-run over a row already QUEUED inserts no SKIPPED row and counts none.
func TestRedriveDoesNotRecountSkips(t *testing.T) {
	ctx := context.Background()
	t.Run("same pass twice", func(t *testing.T) {
		labels := map[string]string{"usecase": templates.UsecaseAlert, "channel": "CHANNEL_SLACK", "to_status": "MESSAGE_STATUS_SKIPPED"}
		before := counter(t, "outbox_comms_message_transitions_total", labels)
		h := newHarness(t, false)
		id := h.seed(t, "alert:redrive", templates.UsecaseAlert, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, orgDestEntry())
		for range 2 {
			if err := h.fo.Process(ctx, testOrg, id); err != nil {
				t.Fatal(err)
			}
		}
		if got := counter(t, "outbox_comms_message_transitions_total", labels); got != before+1 {
			t.Fatalf("SKIPPED transitions went %v -> %v, want +1", before, got)
		}
	})
	t.Run("unfrozen then frozen", func(t *testing.T) {
		labels := map[string]string{"usecase": templates.UsecaseDigest, "channel": "CHANNEL_EMAIL", "to_status": "MESSAGE_STATUS_SKIPPED"}
		before := counter(t, "outbox_comms_message_transitions_total", labels)
		h := newHarness(t, false)
		id := h.seed(t, "digest:refrozen", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, emailEntry("dev@example.com"))
		if err := h.fo.Process(ctx, testOrg, id); err != nil {
			t.Fatal(err)
		}
		frozen := fanout.NewFanOut(h.reqRepo, h.msgRepo, h.prefRepo, h.destRepo,
			fakeOrgGate{active: true, frozen: true}, h.dispatcher, fanout.StaticVendorResolver{Email: pb.Vendor_VENDOR_SES, Slack: pb.Vendor_VENDOR_SLACK})
		if err := frozen.Process(ctx, testOrg, id); err != nil {
			t.Fatal(err)
		}
		if got := counter(t, "outbox_comms_message_transitions_total", labels); got != before {
			t.Fatalf("SKIPPED transitions went %v -> %v, want +0", before, got)
		}
	})
}

// TestRedriveCountsOnlyNewSkips pins a re-drive whose rows changed between
// passes. The org has opted out of product email and Slack. Pass 1 skips the
// email (opted_out) and the unconfigured Slack destination (no_destination).
// A Slack channel is then configured, so pass 2 skips it (opted_out): it counts
// that new row and not the email again.
func TestRedriveCountsOnlyNewSkips(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_EMAIL, pb.Channel_CHANNEL_SLACK} {
		if err := h.prefRepo.Upsert(ctx, &model.CommsPreference{
			OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
			Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: ch, Enabled: false,
		}); err != nil {
			t.Fatal(err)
		}
	}
	email := map[string]string{"usecase": templates.UsecaseDigest, "channel": "CHANNEL_EMAIL", "to_status": "MESSAGE_STATUS_SKIPPED"}
	slack := map[string]string{"usecase": templates.UsecaseDigest, "channel": "CHANNEL_SLACK", "to_status": "MESSAGE_STATUS_SKIPPED"}
	id := h.seed(t, "digest:dest-added", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT,
		emailEntry("ana@example.com"), orgDestEntry())

	e0, s0 := counter(t, "outbox_comms_message_transitions_total", email), counter(t, "outbox_comms_message_transitions_total", slack)
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	e1, s1 := counter(t, "outbox_comms_message_transitions_total", email), counter(t, "outbox_comms_message_transitions_total", slack)
	if e1-e0 != 1 || s1-s0 != 1 {
		t.Fatalf("pass 1 counted email +%v, Slack +%v; want +1, +1", e1-e0, s1-s0)
	}
	if err := h.tx.WithContext(ctx).Create(&model.CommsChannelDestination{
		OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK,
		DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0AB12CD3",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	e2, s2 := counter(t, "outbox_comms_message_transitions_total", email), counter(t, "outbox_comms_message_transitions_total", slack)
	if e2-e1 != 0 || s2-s1 != 1 {
		t.Fatalf("pass 2 counted email +%v, Slack +%v; want +0, +1", e2-e1, s2-s1)
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

// TestFanOutPersonDestination pins that a PERSON-scoped configured destination
// sits beside the organization's and a PERSON-scoped entry resolves to it.
func TestFanOutPersonDestination(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	person := "22222222-2222-2222-2222-222222222222"
	for _, d := range []*model.CommsChannelDestination{
		{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg, Usecase: templates.UsecaseDigest,
			Channel: pb.Channel_CHANNEL_SLACK, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0ORG"},
		{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityID: person, Usecase: templates.UsecaseDigest,
			Channel: pb.Channel_CHANNEL_SLACK, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0PERSON"},
	} {
		if err := h.destRepo.ReplaceForChannel(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	entry := &pb.CommsEntry{
		Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
		DestinationScope: &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityId: person},
	}
	id := h.seed(t, "digest:person-dest", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, entry)
	if err := h.fo.Process(ctx, testOrg, id); err != nil {
		t.Fatal(err)
	}
	rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
	if len(rows) != 1 || rows[0].Recipient != "C0PERSON" || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Fatalf("rows %+v; want one QUEUED row for C0PERSON", rows)
	}
}

// TestFanOutHasNoScopeFallback pins that a configured-destination entry reads
// only the scope it names: a PERSON entry whose person has no route, with an
// ORG route present, is SKIPPED(no_destination), and so is an ORG entry with
// only a PERSON route.
func TestFanOutHasNoScopeFallback(t *testing.T) {
	ctx := context.Background()
	person := "22222222-2222-2222-2222-222222222222"
	for _, tc := range []struct {
		name  string
		route pb.EntityType
		entry *pb.CommsEntry
	}{
		{"PERSON entry, ORG route only", pb.EntityType_ENTITY_TYPE_ORG, &pb.CommsEntry{
			Channel: pb.Channel_CHANNEL_SLACK, TargetKind: pb.TargetKind_TARGET_KIND_CONFIGURED_DESTINATION,
			DestinationScope: &pb.DestinationScope{EntityType: pb.EntityType_ENTITY_TYPE_PERSON, EntityId: person}}},
		{"ORG entry, PERSON route only", pb.EntityType_ENTITY_TYPE_PERSON, orgDestEntry()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, false)
			entity := testOrg
			if tc.route == pb.EntityType_ENTITY_TYPE_PERSON {
				entity = person
			}
			if err := h.destRepo.ReplaceForChannel(ctx, &model.CommsChannelDestination{OrganizationID: testOrg, EntityType: tc.route, EntityID: entity,
				Usecase: templates.UsecaseDigest, Channel: pb.Channel_CHANNEL_SLACK, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0ROUTE"}); err != nil {
				t.Fatal(err)
			}
			id := h.seed(t, "digest:no-fallback", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, tc.entry)
			if err := h.fo.Process(ctx, testOrg, id); err != nil {
				t.Fatal(err)
			}
			rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id)
			if len(rows) != 1 || rows[0].Status != pb.MessageStatus_MESSAGE_STATUS_SKIPPED || skipReason(rows[0]) != "no_destination" {
				t.Fatalf("rows %+v; want one SKIPPED(no_destination) row", rows)
			}
		})
	}
}

// failingDestinations is a destination store whose lookups fail.
type failingDestinations struct{ dao.IDestinationDAO }

func (failingDestinations) Resolve(context.Context, string, pb.EntityType, string, string, pb.Channel) ([]string, error) {
	return nil, errors.New("destination store down")
}

// TestFanOutResolveFailureIsAnError pins that a failed destination lookup fails
// the pass, so it is retried, instead of writing a SKIPPED(no_destination) row
// that would end the message for good.
func TestFanOutResolveFailureIsAnError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, false)
	fo := fanout.NewFanOut(h.reqRepo, h.msgRepo, h.prefRepo, failingDestinations{},
		fakeOrgGate{active: true}, h.dispatcher, fanout.StaticVendorResolver{Email: pb.Vendor_VENDOR_SES, Slack: pb.Vendor_VENDOR_SLACK})
	id := h.seed(t, "digest:resolve-fails", templates.UsecaseDigest, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_BEST_EFFORT, orgDestEntry())
	if err := fo.Process(ctx, testOrg, id); err == nil {
		t.Fatal("Process returned nil after the destination lookup failed")
	}
	if rows, _ := h.msgRepo.ListByRequest(ctx, testOrg, id); len(rows) != 0 {
		t.Fatalf("rows %+v written after a failed lookup; want none", rows)
	}
}
