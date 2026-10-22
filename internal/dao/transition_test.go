// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package dao_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
	"github.com/tetriz-ai/outbox-comms/internal/testdb"
	"github.com/tetriz-ai/outbox-comms/templates"
)

func seedMsg(ctx context.Context, t *testing.T, msgRepo *dao.MessageRepo, reqRepo *dao.RequestRepo, key, recipient string, st pb.MessageStatus, g pb.DeliveryGuarantee) string {
	t.Helper()
	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{OrganizationID: testOrg, IdempotencyKey: key, Usecase: templates.UsecaseNotice, DeliveryGuarantee: g, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{{OrganizationID: testOrg, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: recipient, Status: st, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: g}}); err != nil {
		t.Fatal(err)
	}
	rows, _ := msgRepo.ListByRequest(ctx, testOrg, req.ID)
	return rows[0].ID
}

// claimOf reads a row's current claim id, "" when it has none. A row seeded
// straight into SENDING gets one, as a real claim would give it.
func claimOf(t *testing.T, db *gorm.DB, id string) string {
	t.Helper()
	db.Exec("UPDATE comms_messages SET claim_id = gen_random_uuid() WHERE id = ? AND status = 'MESSAGE_STATUS_SENDING' AND claim_id IS NULL", id)
	var c *string
	db.Raw("SELECT claim_id::text FROM comms_messages WHERE id = ?", id).Scan(&c)
	if c == nil {
		return ""
	}
	return *c
}

// TestSuppressionCaseInsensitive proves a suppression written for one casing of
// an address blocks every casing, and lifting it works in any casing.
func TestSuppressionCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	repo := dao.NewSuppressionRepo(testDB)
	t.Cleanup(func() { testDB.Exec("DELETE FROM comms_suppressions WHERE lower(email) = 'jane.case@example.com'") })
	if err := repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: " Jane.Case@Example.com ", Reason: "hard_bounce"}); err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{"jane.case@example.com", "JANE.CASE@EXAMPLE.COM", "Jane.Case@Example.com"} {
		got, err := repo.GetActiveByEmail(ctx, addr)
		if err != nil || got == nil {
			t.Fatalf("GetActiveByEmail(%q) = %v, %v; want the suppression", addr, got, err)
		}
		if got.Email != "jane.case@example.com" {
			t.Fatalf("stored address = %q, want it normalized", got.Email)
		}
	}
	if err := repo.SuppressIfAbsent(ctx, &model.CommsSuppression{Email: "JANE.case@example.com", Reason: "complaint"}); err != nil {
		t.Fatalf("a second casing must be a no-op, got %v", err)
	}
	if err := repo.Deactivate(ctx, "JANE.CASE@example.com"); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.GetActiveByEmail(ctx, "jane.case@example.com"); got != nil {
		t.Fatal("deactivating in another casing must lift the suppression")
	}
}

// TestClaimRespectsNextRetryAt proves a FAILED row is claimable only once its
// next_retry_at has passed, so a redelivered trigger cannot skip the backoff.
func TestClaimRespectsNextRetryAt(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
	id := seedMsg(ctx, t, msgRepo, reqRepo, "claim-b", "b@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	ok, claimed, _ := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if !ok {
		t.Fatal("a QUEUED row must be claimable")
	}
	if err := msgRepo.MarkFailed(ctx, testOrg, id, *claimed.ClaimID, time.Now().Add(time.Hour), "boom"); err != nil {
		t.Fatal(err)
	}
	if ok, _, err := msgRepo.ClaimForDispatch(ctx, testOrg, id); ok || err != nil {
		t.Fatalf("a FAILED row due in 1h must not be claimable, got ok=%v err=%v", ok, err)
	}
	tx.Exec("UPDATE comms_messages SET next_retry_at = now() - interval '1 second' WHERE id = ?", id)
	if ok, m, err := msgRepo.ClaimForDispatch(ctx, testOrg, id); !ok || err != nil || m.Status != pb.MessageStatus_MESSAGE_STATUS_SENDING {
		t.Fatalf("a FAILED row past its retry time must be claimed, got ok=%v err=%v", ok, err)
	}
}

// TestTransitionGuards proves each move applies only from the states it may
// start from; any other state returns ErrStaleTransition and leaves the row.
func TestTransitionGuards(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
	type move func(id string) error
	markSent := func(id string) error { return msgRepo.MarkSent(ctx, testOrg, id, claimOf(t, tx, id), "pmid") }
	markFailed := func(id string) error {
		return msgRepo.MarkFailed(ctx, testOrg, id, claimOf(t, tx, id), time.Now(), "x")
	}
	markDead := func(id string) error { return msgRepo.MarkDead(ctx, testOrg, id, claimOf(t, tx, id), "r", "x") }
	markDelivered := func(id string) error { return msgRepo.MarkDelivered(ctx, testOrg, id, "", "") }
	markBounced := func(id string) error { return msgRepo.MarkBounced(ctx, testOrg, id) }
	tests := []struct {
		name   string
		from   pb.MessageStatus
		reason string
		move   move
		wantOK bool
	}{
		{"MarkSent from SENDING", pb.MessageStatus_MESSAGE_STATUS_SENDING, "", markSent, true},
		{"MarkSent from DEAD", pb.MessageStatus_MESSAGE_STATUS_DEAD, "", markSent, false},
		{"MarkFailed from SENDING", pb.MessageStatus_MESSAGE_STATUS_SENDING, "", markFailed, true},
		{"MarkFailed from UNCONFIRMED", pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, "uncertain_send", markFailed, false},
		{"MarkDead from QUEUED", pb.MessageStatus_MESSAGE_STATUS_QUEUED, "", markDead, false},
		{"MarkDelivered from SENT", pb.MessageStatus_MESSAGE_STATUS_SENT, "", markDelivered, true},
		{"MarkDelivered from UNCONFIRMED(no_delivery_feedback)", pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, "no_delivery_feedback", markDelivered, true},
		{"MarkDelivered from UNCONFIRMED(uncertain_send)", pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED, "uncertain_send", markDelivered, false},
		{"MarkDelivered from BOUNCED", pb.MessageStatus_MESSAGE_STATUS_BOUNCED, "", markDelivered, false},
		{"MarkBounced from DELIVERED", pb.MessageStatus_MESSAGE_STATUS_DELIVERED, "", markBounced, true},
		{"MarkBounced from SUPPRESSED", pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED, "", markBounced, false},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := seedMsg(ctx, t, msgRepo, reqRepo, "guard-"+string(rune('a'+i)), string(rune('a'+i))+"@example.com", tt.from, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
			if tt.reason != "" {
				tx.Exec("UPDATE comms_messages SET unconfirmed_reason = ? WHERE id = ?", tt.reason, id)
			}
			err := tt.move(id)
			after, _ := msgRepo.GetByID(ctx, testOrg, id)
			if tt.wantOK {
				if err != nil || after.Status == tt.from {
					t.Fatalf("move must apply: err=%v status=%v", err, after.Status)
				}
				return
			}
			if !errors.Is(err, dao.ErrStaleTransition) || after.Status != tt.from {
				t.Fatalf("move must be refused: err=%v status=%v (was %v)", err, after.Status, tt.from)
			}
		})
	}
	if err := markSent("00000000-0000-0000-0000-000000000000"); err == nil || errors.Is(err, dao.ErrStaleTransition) {
		t.Fatalf("a missing row is an error, not a stale move: %v", err)
	}
}

// TestConcurrentClaim proves the CAS claim admits exactly one of many workers
// racing for the same row, on the real store.
func TestConcurrentClaim(t *testing.T) {
	ctx := context.Background()
	msgRepo, reqRepo := dao.NewMessageRepo(testDB), dao.NewRequestRepo(testDB)
	id := seedMsg(ctx, t, msgRepo, reqRepo, "race", "race@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE id = ?", id)
		testDB.Exec("DELETE FROM comms_requests WHERE idempotency_key = 'race'")
	})
	const workers = 16
	wins := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		go func() {
			ok, _, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
			wins <- ok && err == nil
		}()
	}
	n := 0
	for i := 0; i < workers; i++ {
		if <-wins {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d workers claimed the row, want 1", n)
	}
}

// TestMaterializeRefusesMixedOrgBatch proves a batch carrying another org's row
// is refused whole, so a caller bug cannot write across organizations.
func TestMaterializeRefusesMixedOrgBatch(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
	_, req, err := reqRepo.CreateIfAbsent(ctx, &model.CommsRequest{OrganizationID: testOrg, IdempotencyKey: "mixed", Usecase: templates.UsecaseNotice, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED, Status: pb.RequestStatus_REQUEST_STATUS_FANNED_OUT})
	if err != nil {
		t.Fatal(err)
	}
	row := func(org, to string) *model.CommsMessage {
		return &model.CommsMessage{OrganizationID: org, RequestID: req.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: to, Status: pb.MessageStatus_MESSAGE_STATUS_QUEUED, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED}
	}
	_, err = msgRepo.MaterializeBatch(ctx, testOrg, []*model.CommsMessage{row(testOrg, "a@example.com"), row("33333333-3333-3333-3333-333333333333", "b@example.com")})
	if err == nil {
		t.Fatal("a mixed-org batch must be refused")
	}
	if rows, _ := msgRepo.ListByRequest(ctx, testOrg, req.ID); len(rows) != 0 {
		t.Fatalf("nothing may be written, got %d rows", len(rows))
	}
}

// TestReleaseClaimOnlyFromSending pins ReleaseClaim's guard: on a row that has
// already reached SENT, SUPPRESSED, DEAD or UNCONFIRMED it is a stale
// transition, and the row is unchanged.
func TestReleaseClaimOnlyFromSending(t *testing.T) {
	ctx := context.Background()
	for _, st := range []pb.MessageStatus{
		pb.MessageStatus_MESSAGE_STATUS_SENT, pb.MessageStatus_MESSAGE_STATUS_SUPPRESSED,
		pb.MessageStatus_MESSAGE_STATUS_DEAD, pb.MessageStatus_MESSAGE_STATUS_UNCONFIRMED,
	} {
		t.Run(st.String(), func(t *testing.T) {
			tx := newTestTx(t)
			repo := dao.NewMessageRepo(tx)
			id := seedMsg(ctx, t, repo, dao.NewRequestRepo(tx), "release-"+st.String(), "release@example.com", st, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
			tx.Exec("UPDATE comms_messages SET attempts = 2 WHERE id = ?", id)
			if err := repo.ReleaseClaim(ctx, testOrg, id, claimOf(t, tx, id)); !errors.Is(err, dao.ErrStaleTransition) {
				t.Fatalf("err = %v, want ErrStaleTransition", err)
			}
			got, _ := repo.GetByID(ctx, testOrg, id)
			if got.Status != st || got.Attempts != 2 {
				t.Fatalf("row is %v attempts %d, want %v 2", got.Status, got.Attempts, st)
			}
		})
	}
}

// TestClaimBelongsToItsWorker pins the claim id at the store: A claims,
// reconcile requeues the row, B claims it. A's release is a stale transition
// that leaves the row with B, and B's MarkSent succeeds.
func TestClaimBelongsToItsWorker(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
	id := seedMsg(ctx, t, msgRepo, reqRepo, "claim-ab", "ab@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	okA, a, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if !okA || err != nil {
		t.Fatalf("A's claim: %v %v", okA, err)
	}
	refs, _, err := dao.NewReconcileRepo(tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6})
	if err != nil || len(refs) != 1 {
		t.Fatalf("requeue: %d rows, %v", len(refs), err)
	}
	okB, b, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	if !okB || err != nil || *b.ClaimID == *a.ClaimID {
		t.Fatalf("B's claim: %v %v, same id as A: %v", okB, err, b.ClaimID != nil && *b.ClaimID == *a.ClaimID)
	}
	if err := msgRepo.ReleaseClaim(ctx, testOrg, id, *a.ClaimID); !errors.Is(err, dao.ErrStaleTransition) {
		t.Fatalf("A's release: %v, want ErrStaleTransition", err)
	}
	if err := msgRepo.MarkSent(ctx, testOrg, id, *a.ClaimID, "pmid-a"); !errors.Is(err, dao.ErrStaleTransition) {
		t.Fatalf("A's MarkSent: %v, want ErrStaleTransition", err)
	}
	if err := msgRepo.MarkSent(ctx, testOrg, id, *b.ClaimID, "pmid-b"); err != nil {
		t.Fatalf("B's MarkSent: %v", err)
	}
	got, _ := msgRepo.GetByID(ctx, testOrg, id)
	if got.Status != pb.MessageStatus_MESSAGE_STATUS_SENT || *got.ProviderMessageID != "pmid-b" {
		t.Fatalf("row %v pmid %v, want SENT by B", got.Status, *got.ProviderMessageID)
	}
}

// TestEveryMoveOutOfSendingChecksTheClaim pins the claim guard on each method
// that acts on a SENDING row. A claims, reconcile requeues the row, B claims
// it. A's call is a stale transition that changes nothing, and B's same call
// then succeeds.
func TestEveryMoveOutOfSendingChecksTheClaim(t *testing.T) {
	rendered := &pb.RenderedPayload{EmailSubject: "s", EmailHtml: "<p>h</p>"}
	moves := map[string]func(ctx context.Context, r *dao.MessageRepo, id, claim string) error{
		"MarkSent": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkSent(ctx, testOrg, id, c, "pmid-"+c)
		},
		"MarkDelivered": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkDelivered(ctx, testOrg, id, c, "ts-"+c)
		},
		"MarkFailed": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkFailed(ctx, testOrg, id, c, time.Now().Add(time.Hour), "x")
		},
		"MarkDead": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkDead(ctx, testOrg, id, c, "permanent_config", "x")
		},
		"MarkUnconfirmed": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkUnconfirmed(ctx, testOrg, id, c, "uncertain_send")
		},
		"MarkSuppressed": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.MarkSuppressed(ctx, testOrg, id, c)
		},
		"SnapshotRendered": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.SnapshotRendered(ctx, testOrg, id, c, rendered)
		},
		"ReleaseClaim": func(ctx context.Context, r *dao.MessageRepo, id, c string) error {
			return r.ReleaseClaim(ctx, testOrg, id, c)
		},
	}
	for name, move := range moves {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tx := newTestTx(t)
			msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
			id := seedMsg(ctx, t, msgRepo, reqRepo, "claim-"+name, "ab@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
			_, a, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
			if err != nil || a == nil {
				t.Fatalf("A's claim: %v", err)
			}
			if _, _, err := dao.NewReconcileRepo(tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, dao.AttemptCap{Default: 6}); err != nil {
				t.Fatal(err)
			}
			_, b, err := msgRepo.ClaimForDispatch(ctx, testOrg, id)
			if err != nil || b == nil {
				t.Fatalf("B's claim: %v", err)
			}
			before, _ := msgRepo.GetByID(ctx, testOrg, id)
			if err := move(ctx, msgRepo, id, *a.ClaimID); !errors.Is(err, dao.ErrStaleTransition) {
				t.Fatalf("A's %s: %v, want ErrStaleTransition", name, err)
			}
			after, _ := msgRepo.GetByID(ctx, testOrg, id)
			if after.Status != before.Status || after.Attempts != before.Attempts || after.RenderedContent != nil || *after.ClaimID != *b.ClaimID {
				t.Fatalf("A's stale %s changed the row: %v attempts %d snapshot %v", name, after.Status, after.Attempts, after.RenderedContent != nil)
			}
			if err := move(ctx, msgRepo, id, *b.ClaimID); err != nil {
				t.Fatalf("B's %s: %v", name, err)
			}
		})
	}
}

// TestNullClaimRows pins the two store paths that meet a row with no claim id.
// RedriveDead resets a DEAD row whatever its claim_id. ReleaseClaim on a
// SENDING row with a NULL claim_id is a stale transition for an empty claim and
// for any claim, so a row no worker holds is left for reconcile.
func TestNullClaimRows(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)

	dead := seedMsg(ctx, t, msgRepo, reqRepo, "null-claim-dead", "dead@example.com", pb.MessageStatus_MESSAGE_STATUS_DEAD, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	n, err := dao.NewReconcileRepo(tx).RedriveDead(ctx, []string{dead})
	if err != nil || n != 1 {
		t.Fatalf("RedriveDead: %d rows, %v; want 1", n, err)
	}
	if got, _ := msgRepo.GetByID(ctx, testOrg, dead); got.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED || got.ClaimID != nil {
		t.Fatalf("redriven row %v claim %v, want QUEUED with no claim", got.Status, got.ClaimID)
	}

	sending := seedMsg(ctx, t, msgRepo, reqRepo, "null-claim-sending", "sending@example.com", pb.MessageStatus_MESSAGE_STATUS_SENDING, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	for _, claim := range []string{"", "11111111-2222-3333-4444-555555555555"} {
		if err := msgRepo.ReleaseClaim(ctx, testOrg, sending, claim); !errors.Is(err, dao.ErrStaleTransition) {
			t.Fatalf("ReleaseClaim(%q) on a NULL claim: %v, want ErrStaleTransition", claim, err)
		}
	}
	if got, _ := msgRepo.GetByID(ctx, testOrg, sending); got.Status != pb.MessageStatus_MESSAGE_STATUS_SENDING {
		t.Fatalf("row %v, want SENDING", got.Status)
	}
}

// TestClaimSurvivesACancelAfterTheStatement pins that a claim leaves no gap
// between the row becoming SENDING and the caller holding it. The claim's
// context is cancelled the moment its UPDATE has run. The result is either the
// claimed row, or an error with the row still QUEUED; never an error with the
// row SENDING, which nobody would release. It uses its own connection, so the
// callback that cancels touches no other test.
func TestClaimSurvivesACancelAfterTheStatement(t *testing.T) {
	db, err := gorm.Open(postgres.Open(testdb.SchemaURL()), &gorm.Config{Logger: gormlogger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s, err := db.DB(); err == nil {
			_ = s.Close()
		}
	})
	var cancelClaim context.CancelFunc
	if err := db.Callback().Update().After("gorm:update").Register("test:cancel_after_claim", func(tx *gorm.DB) {
		if cancelClaim != nil && tx.Statement.Table == "comms_messages" {
			cancelClaim()
		}
	}); err != nil {
		t.Fatal(err)
	}
	msgRepo, reqRepo := dao.NewMessageRepo(db), dao.NewRequestRepo(db)
	key := fmt.Sprintf("claim-cancel-%d", time.Now().UnixNano())
	id := seedMsg(context.Background(), t, msgRepo, reqRepo, key, "cancel@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)

	ctx, cancel := context.WithCancel(context.Background())
	cancelClaim = cancel
	ok, msg, claimErr := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	cancelClaim = nil
	cancel()
	row, err := msgRepo.GetByID(context.Background(), testOrg, id)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case claimErr == nil && ok && msg != nil && msg.ClaimID != nil && row.ClaimID != nil && *msg.ClaimID == *row.ClaimID && row.Status == pb.MessageStatus_MESSAGE_STATUS_SENDING:
	case claimErr != nil && !ok && row.Status == pb.MessageStatus_MESSAGE_STATUS_QUEUED:
	default:
		t.Fatalf("claim ok=%v err=%v, row %v; want the claimed row, or an error with the row QUEUED", ok, claimErr, row.Status)
	}
}

// cancellingPool is a gorm connection pool that cancels the claim's context
// once the claim's query has returned and before its rows are read: the gap
// between the statement and the caller holding its result.
type cancellingPool struct {
	db     *sql.DB
	cancel func()
}

type cancellingTx struct {
	*sql.Tx
	cancel func()
}

func cancelAfterClaim(cancel func(), query string) {
	if cancel != nil && strings.HasPrefix(query, `UPDATE "comms_messages"`) && strings.Contains(query, "RETURNING") {
		cancel()
		time.Sleep(50 * time.Millisecond) // let database/sql close the rows on the cancelled context
	}
}

func (p *cancellingPool) PrepareContext(ctx context.Context, q string) (*sql.Stmt, error) {
	return p.db.PrepareContext(ctx, q)
}
func (p *cancellingPool) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, q, args...)
}
func (p *cancellingPool) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	rows, err := p.db.QueryContext(ctx, q, args...)
	cancelAfterClaim(p.cancel, q)
	return rows, err
}
func (p *cancellingPool) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, q, args...)
}
func (p *cancellingPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	tx, err := p.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &cancellingTx{Tx: tx, cancel: p.cancel}, nil
}
func (t *cancellingTx) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	rows, err := t.Tx.QueryContext(ctx, q, args...)
	cancelAfterClaim(t.cancel, q)
	return rows, err
}

// TestClaimSurvivesACancelDuringTheRead pins that the claim's guarantee does
// not rest on GORM's default transaction. On a handle opened with
// SkipDefaultTransaction, the claim's context is cancelled after its UPDATE has
// run and before its RETURNING row is read. The claim must fail with the row
// still QUEUED: its own transaction rolls the UPDATE back.
func TestClaimSurvivesACancelDuringTheRead(t *testing.T) {
	sqlDB, err := sql.Open("pgx", testdb.SchemaURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	pool := &cancellingPool{db: sqlDB}
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{Logger: gormlogger.Discard, SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	msgRepo, reqRepo := dao.NewMessageRepo(db), dao.NewRequestRepo(db)
	id := seedMsg(context.Background(), t, msgRepo, reqRepo, fmt.Sprintf("claim-read-%d", time.Now().UnixNano()), "read@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.cancel = cancel
	ok, _, claimErr := msgRepo.ClaimForDispatch(ctx, testOrg, id)
	pool.cancel = nil
	row, err := msgRepo.GetByID(context.Background(), testOrg, id)
	if err != nil {
		t.Fatal(err)
	}
	if claimErr == nil || ok || row.Status != pb.MessageStatus_MESSAGE_STATUS_QUEUED {
		t.Fatalf("claim ok=%v err=%v, row %v; want an error with the row QUEUED", ok, claimErr, row.Status)
	}
}

// TestCrashCapByUsecase pins that a crash requeue reads the cap of the
// message's own usecase: notice capped at 2 goes DEAD on its second crash,
// while the default of 6 would still requeue it.
func TestCrashCapByUsecase(t *testing.T) {
	ctx := context.Background()
	tx := newTestTx(t)
	msgRepo, reqRepo := dao.NewMessageRepo(tx), dao.NewRequestRepo(tx)
	id := seedMsg(ctx, t, msgRepo, reqRepo, "crash-cap", "cap@example.com", pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED)
	capped := dao.AttemptCap{Default: 6, ByUsecase: map[string]int{templates.UsecaseNotice: 2, "other": 9}}
	for crash, want := range []pb.MessageStatus{pb.MessageStatus_MESSAGE_STATUS_QUEUED, pb.MessageStatus_MESSAGE_STATUS_DEAD} {
		if ok, _, err := msgRepo.ClaimForDispatch(ctx, testOrg, id); !ok || err != nil {
			t.Fatalf("claim %d: %v %v", crash+1, ok, err)
		}
		requeued, dead, err := dao.NewReconcileRepo(tx).RequeueStuckSendingGuaranteed(ctx, time.Now().Add(time.Hour), 10, capped)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := msgRepo.GetByID(ctx, testOrg, id)
		if got.Status != want || got.Attempts != crash+1 || len(requeued)+len(dead) != 1 {
			t.Fatalf("crash %d: %v attempts %d (%d requeued, %d dead); want %v, %d", crash+1, got.Status, got.Attempts, len(requeued), len(dead), want, crash+1)
		}
	}
	got, _ := msgRepo.GetByID(ctx, testOrg, id)
	if got.DeadReason == nil || *got.DeadReason != dao.DeadReasonCrashExhausted {
		t.Fatalf("dead_reason %v, want crash_exhausted", got.DeadReason)
	}
}
