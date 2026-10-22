// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

var testDB *gorm.DB

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, &testDB)) }

// newTestTx gives each test a rolled-back-at-cleanup transaction for isolation.
func newTestTx(t *testing.T) *gorm.DB {
	t.Helper()
	tx := testDB.Begin()
	if tx.Error != nil {
		t.Fatalf("begin tx: %v", tx.Error)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

const testOrg = "11111111-1111-1111-1111-111111111111"

func newRequest(org, key string) *model.CommsRequest {
	return &model.CommsRequest{
		OrganizationID:    org,
		IdempotencyKey:    key,
		Usecase:           templates.UsecaseNotice,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		Status:            pb.RequestStatus_REQUEST_STATUS_ACCEPTED,
	}
}

// TestRequestIdempotentSubmit proves CreateIfAbsent collapses a re-submit on
// (org, idempotency_key) to one row, returning the same handle.
func TestRequestIdempotentSubmit(t *testing.T) {
	ctx := context.Background()
	repo := dao.NewRequestRepo(newTestTx(t))

	created1, r1, err := repo.CreateIfAbsent(ctx, newRequest(testOrg, "notice:p1:1"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if !created1 {
		t.Fatalf("first submit should create the row")
	}

	created2, r2, err := repo.CreateIfAbsent(ctx, newRequest(testOrg, "notice:p1:1"))
	if err != nil {
		t.Fatalf("re-submit: %v", err)
	}
	if created2 {
		t.Fatalf("re-submit must not create a second row")
	}
	if r1.ID != r2.ID {
		t.Fatalf("re-submit must return the same handle: %s != %s", r1.ID, r2.ID)
	}
}

// TestMessageMaterializeDedup proves the fan-out's
// ON CONFLICT(request_id, channel, recipient) DO NOTHING no-ops a re-driven
// overlap.
func TestMessageMaterializeDedup(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)

	_, req, err := reqRepo.CreateIfAbsent(ctx, newRequest(testOrg, "k-dedup"))
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	mk := func() *model.CommsMessage {
		return &model.CommsMessage{
			OrganizationID:    testOrg,
			RequestID:         req.ID,
			Channel:           pb.Channel_CHANNEL_EMAIL,
			Vendor:            pb.Vendor_VENDOR_SES,
			Recipient:         "dev@example.com",
			Status:            pb.MessageStatus_MESSAGE_STATUS_QUEUED,
			VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		}
	}
	if got, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{mk()}); err != nil || len(got) != 1 {
		t.Fatalf("first materialize: %d inserted, %v; want 1", len(got), err)
	}
	// Re-drive the same (request, channel, recipient): must no-op, not error,
	// and report nothing inserted.
	if got, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{mk()}); err != nil || len(got) != 0 {
		t.Fatalf("re-materialize: %d inserted, %v; want 0", len(got), err)
	}
	rows, err := msgRepo.ListByRequest(ctx, testOrg, req.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("dedup must leave exactly one row, got %d", len(rows))
	}
}

// TestMaterializeBatchReturnsOnlyInserted pins the partial conflict: a batch
// of a row already stored, a new SKIPPED row and a new QUEUED row returns
// exactly the two new rows, whatever their position. Passing the same slice
// again reports nothing inserted, though the first call gave its rows ids.
func TestMaterializeBatchReturnsOnlyInserted(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	reqRepo, msgRepo := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
	_, req, err := reqRepo.CreateIfAbsent(ctx, newRequest(testOrg, "k-mixed"))
	if err != nil {
		t.Fatal(err)
	}
	row := func(recipient string, st pb.MessageStatus) *model.CommsMessage {
		return &model.CommsMessage{
			OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
			Recipient: recipient, Status: st, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
			DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
		}
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{row("stored@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED)}); err != nil {
		t.Fatal(err)
	}
	batch := []*model.CommsMessage{
		row("stored@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED),
		row("skipped@example.com", pb.MessageStatus_MESSAGE_STATUS_SKIPPED),
		row("queued@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED),
	}
	got, err := msgRepo.MaterializeBatch(ctx, testOrg, batch)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range got {
		names = append(names, m.Recipient+"/"+m.Status.String())
	}
	if want := "skipped@example.com/MESSAGE_STATUS_SKIPPED,queued@example.com/MESSAGE_STATUS_QUEUED"; strings.Join(names, ",") != want {
		t.Fatalf("inserted %v, want %s", names, want)
	}
	again, err := msgRepo.MaterializeBatch(ctx, testOrg, batch)
	if err != nil || len(again) != 0 {
		t.Fatalf("the same slice again: %d inserted, %v; want 0", len(again), err)
	}
}

// TestClaimForDispatchCAS proves the dispatcher CAS claims a QUEUED row once and
// rejects a second claim on the now-SENDING row (the at-least-once guard).
func TestClaimForDispatchCAS(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	reqRepo := dao.NewRequestRepo(tx)
	msgRepo := dao.NewMessageRepo(tx)

	_, req, err := reqRepo.CreateIfAbsent(ctx, newRequest(testOrg, "k-cas"))
	if err != nil {
		t.Fatalf("seed request: %v", err)
	}
	msg := &model.CommsMessage{
		OrganizationID:    testOrg,
		RequestID:         req.ID,
		Channel:           pb.Channel_CHANNEL_EMAIL,
		Vendor:            pb.Vendor_VENDOR_SES,
		Recipient:         "dev@example.com",
		Status:            pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK,
		DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{msg}); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	queued, err := msgRepo.ListQueuedForRequest(ctx, testOrg, req.ID)
	if err != nil || len(queued) != 1 {
		t.Fatalf("expected 1 queued, got %d (err=%v)", len(queued), err)
	}
	id := queued[0].ID

	ok, claimed, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !ok || claimed.Status != pb.MessageStatus_MESSAGE_STATUS_SENDING {
		t.Fatalf("first claim must succeed and move to SENDING, got ok=%v status=%v", ok, claimed.Status)
	}
	ok2, _, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok2 {
		t.Fatalf("second claim on a SENDING row must CAS-miss (no double-send)")
	}
}

// TestSuppressionSoftDeleteLifecycle proves a suppression blocks while live,
// stops blocking once soft-deleted, and a re-bounce inserts a fresh live row
// (the unique-live index and soft delete).
func TestSuppressionSoftDeleteLifecycle(t *testing.T) {
	ctx := context.Background()
	// The repo opens its own transaction, so it takes the base
	// handle (not a test tx). A unique email keeps runs independent; clean up after.
	repo := dao.NewSuppressionRepo(testDB)
	email := fmt.Sprintf("bounce-%d@example.com", time.Now().UnixNano())
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_suppressions WHERE email = ?", email)
	})

	if err := repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
		t.Fatalf("suppress: %v", err)
	}
	got, err := repo.GetActiveByEmail(ctx, email)
	if err != nil || got == nil {
		t.Fatalf("expected a live suppression, got %v (err=%v)", got, err)
	}
	// A duplicate suppress is a no-op (unique-live index).
	if err := repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "complaint"}); err != nil {
		t.Fatalf("duplicate suppress: %v", err)
	}

	if err := repo.Deactivate(ctx, email); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	got, err = repo.GetActiveByEmail(ctx, email)
	if err != nil {
		t.Fatalf("get after deactivate: %v", err)
	}
	if got != nil {
		t.Fatalf("deactivated address must not block")
	}
	// A re-bounce after deactivation inserts a fresh live row.
	if err := repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: email, Reason: "hard_bounce"}); err != nil {
		t.Fatalf("re-suppress: %v", err)
	}
	got, err = repo.GetActiveByEmail(ctx, email)
	if err != nil || got == nil {
		t.Fatalf("re-bounce must re-block the address, got %v (err=%v)", got, err)
	}
}

// otherOrg is a second org used only to prove GetUsecasesByIDs is org-scoped.
const otherOrg = "33333333-3333-3333-3333-333333333333"

// TestRequestGetUsecasesByIDs proves the batch usecase lookup ListMessages uses:
// resolves every requested id that belongs to the caller's org, silently omits a
// foreign-org id from the result (no cross-org leak) and a nonexistent id, and
// returns an empty map without querying when given no ids.
func TestRequestGetUsecasesByIDs(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	repo := dao.NewRequestRepo(tx)

	digest := newRequest(testOrg, "k-usecase-digest")
	digest.Usecase = templates.UsecaseDigest
	_, digestReq, err := repo.CreateIfAbsent(ctx, digest)
	if err != nil {
		t.Fatalf("seed digest request: %v", err)
	}
	_, noticeReq, err := repo.CreateIfAbsent(ctx, newRequest(testOrg, "k-usecase-notice"))
	if err != nil {
		t.Fatalf("seed notice request: %v", err)
	}
	_, foreignReq, err := repo.CreateIfAbsent(ctx, newRequest(otherOrg, "k-usecase-foreign"))
	if err != nil {
		t.Fatalf("seed foreign-org request: %v", err)
	}

	// id is a uuid column — a well-formed-but-absent uuid exercises "not found",
	// not a type error (a malformed non-uuid string would 22P02 before matching).
	const nonexistentID = "99999999-9999-9999-9999-999999999999"
	got, err := repo.GetUsecasesByIDs(ctx, testOrg, []string{digestReq.ID, noticeReq.ID, foreignReq.ID, nonexistentID})
	if err != nil {
		t.Fatalf("GetUsecasesByIDs: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected exactly the 2 same-org ids resolved, got %d: %v", len(got), got)
	}
	if got[digestReq.ID] != templates.UsecaseDigest {
		t.Fatalf("digest request usecase mismatch: got %v", got[digestReq.ID])
	}
	if got[noticeReq.ID] != templates.UsecaseNotice {
		t.Fatalf("notice request usecase mismatch: got %v", got[noticeReq.ID])
	}
	if _, ok := got[foreignReq.ID]; ok {
		t.Fatalf("a foreign-org request id must not resolve (cross-org leak)")
	}

	empty, err := repo.GetUsecasesByIDs(ctx, testOrg, nil)
	if err != nil {
		t.Fatalf("GetUsecasesByIDs with no ids: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("no ids must resolve to an empty map, got %v", empty)
	}
}

// TestOrgScoping pins how organizations are kept apart: every query carries an
// explicit organization_id filter, and the tables have no Postgres row-level
// security. A read or a move with another organization's id finds nothing of
// this organization's rows.
func TestOrgScoping(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	reqRepo, msgRepo := dao.NewRequestRepo(tx), dao.NewMessageRepo(tx)
	evRepo, prefRepo, destRepo := dao.NewDeliveryEventRepo(tx), dao.NewPreferenceRepo(tx), dao.NewDestinationRepo(tx)

	_, req, err := reqRepo.CreateIfAbsent(ctx, newRequest(testOrg, "scope-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{{
		OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SES,
		Recipient: "scope@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED,
		VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED,
	}}); err != nil {
		t.Fatal(err)
	}
	msgs, _ := msgRepo.ListByRequest(ctx, testOrg, req.ID)
	if len(msgs) != 1 {
		t.Fatalf("seed: want 1 message, got %d", len(msgs))
	}
	msgID := msgs[0].ID
	if err := evRepo.Append(ctx, &model.CommsDeliveryEvent{OrganizationID: testOrg, MessageID: msgID, EventType: pb.DeliveryEventType_DELIVERY_EVENT_TYPE_SENT, OccurredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := prefRepo.Upsert(ctx, &model.CommsPreference{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Category: pb.MessageCategory_MESSAGE_CATEGORY_PRODUCT, Channel: pb.Channel_CHANNEL_EMAIL, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := tx.WithContext(ctx).Create(&model.CommsChannelDestination{OrganizationID: testOrg, EntityType: pb.EntityType_ENTITY_TYPE_ORG, EntityID: testOrg,
		Usecase: templates.UsecaseAlert, Channel: pb.Channel_CHANNEL_SLACK, DestinationKind: pb.DestinationKind_DESTINATION_KIND_CHANNEL, DestinationRef: "C0SCOPE"}).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := reqRepo.GetByHandle(ctx, otherOrg, "scope-1"); err == nil {
		t.Error("GetByHandle found another organization's request")
	}
	if _, err := reqRepo.GetByID(ctx, otherOrg, req.ID); err == nil {
		t.Error("request GetByID found another organization's request")
	}
	if _, err := msgRepo.GetByID(ctx, otherOrg, msgID); err == nil {
		t.Error("message GetByID found another organization's message")
	}
	if rows, _ := msgRepo.ListByRequest(ctx, otherOrg, req.ID); len(rows) != 0 {
		t.Errorf("ListByRequest returned %d foreign rows", len(rows))
	}
	if rows, _ := msgRepo.ListByOrg(ctx, otherOrg, nil, "", 50); len(rows) != 0 {
		t.Errorf("ListByOrg returned %d foreign rows", len(rows))
	}
	if rows, _ := evRepo.ListByMessage(ctx, otherOrg, msgID); len(rows) != 0 {
		t.Errorf("ListByMessage returned %d foreign events", len(rows))
	}
	if rows, _ := prefRepo.ListForEntity(ctx, otherOrg, pb.EntityType_ENTITY_TYPE_ORG, testOrg); len(rows) != 0 {
		t.Errorf("preference ListForEntity returned %d foreign rows", len(rows))
	}
	if rows, _ := destRepo.ListForOrg(ctx, otherOrg); len(rows) != 0 {
		t.Errorf("destination ListForOrg returned %d foreign rows", len(rows))
	}
	if ok, _, _ := msgRepo.ClaimForDispatch(ctx, otherOrg, msgID); ok {
		t.Error("ClaimForDispatch claimed another organization's message")
	}
	if got, _ := msgRepo.GetByID(ctx, testOrg, msgID); got.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Errorf("a foreign claim moved the row to %v", got.Status)
	}

	var rls int64
	tx.Raw(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname LIKE 'comms\_%' AND c.relkind = 'r' AND c.relrowsecurity`).Scan(&rls)
	if rls != 0 {
		t.Errorf("%d comms tables have row-level security on; the scoping is the explicit filter", rls)
	}
}

// TestRequestUpdateStatusNoRow pins that moving a request that is not there,
// under the wrong organization or an absent id, is an error, not a silent
// success the fan-out would take as progress.
func TestRequestUpdateStatusNoRow(t *testing.T) {
	ctx := context.Background()
	repo := dao.NewRequestRepo(newTestTx(t))
	_, req, err := repo.CreateIfAbsent(ctx, newRequest(testOrg, "status-no-row"))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateStatus(ctx, otherOrg, req.ID, pb.RequestStatus_REQUEST_STATUS_FANNED_OUT); err == nil {
		t.Error("a move under another organization succeeded")
	}
	if err := repo.UpdateStatus(ctx, testOrg, "99999999-9999-9999-9999-999999999999", pb.RequestStatus_REQUEST_STATUS_FANNED_OUT); err == nil {
		t.Error("a move of an absent request succeeded")
	}
	if err := repo.UpdateStatus(ctx, testOrg, req.ID, pb.RequestStatus_REQUEST_STATUS_FANNED_OUT); err != nil {
		t.Errorf("the real move failed: %v", err)
	}
}

// TestRedriveDeadSkipsRowsThatLeftDead pins the re-check in RedriveDead's
// update: a row that left DEAD after it was listed is not reset to QUEUED.
func TestRedriveDeadSkipsRowsThatLeftDead(t *testing.T) {
	ctx := context.Background()
	const org = "66666666-6666-6666-6666-666666666666"
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	_, req, err := dao.NewRequestRepo(testDB).CreateIfAbsent(ctx, newRequest(org, "redrive-race"))
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range []string{"a@example.com", "b@example.com"} {
		var id string
		testDB.Raw(`INSERT INTO comms_messages (organization_id, request_id, channel, vendor, recipient, status, verification_level, delivery_guarantee, dead_reason)
			VALUES (?, ?, 'CHANNEL_EMAIL', 'VENDOR_SES', ?, 'MESSAGE_STATUS_DEAD', 'VERIFICATION_LEVEL_PROVIDER_ACK', 'DELIVERY_GUARANTEE_GUARANTEED', 'transient_exhausted') RETURNING id`,
			org, req.ID, r).Scan(&id)
		ids = append(ids, id)
	}
	// The first row leaves DEAD after the listing and before the reset.
	testDB.Exec("UPDATE comms_messages SET status = 'MESSAGE_STATUS_SENT' WHERE id = ?", ids[0])
	n, err := dao.NewReconcileRepo(testDB).RedriveDead(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	var first, second string
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", ids[0]).Scan(&first)
	testDB.Raw("SELECT status FROM comms_messages WHERE id = ?", ids[1]).Scan(&second)
	if n != 1 || first != "MESSAGE_STATUS_SENT" || second != "MESSAGE_STATUS_QUEUED" {
		t.Fatalf("redrove %d; the moved row is %s, the other %s; want 1, SENT, QUEUED", n, first, second)
	}
}
