// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dao"
	"github.com/tetriz-ai/outbox-comms/internal/dao/model"
)

// lockedSubmitter is fakeSubmitter safe for concurrent runs.
type lockedSubmitter struct {
	mu    sync.Mutex
	inner *fakeSubmitter
}

func (l *lockedSubmitter) SubmitComms(ctx context.Context, req *pb.SubmitCommsRequest, opts ...grpc.CallOption) (*pb.SubmitCommsResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.SubmitComms(ctx, req, opts...)
}

func (l *lockedSubmitter) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.inner.calls)
}

// failingHandles fails every chunk-key lookup with a database error.
type failingHandles struct{ dao.IRequestDAO }

func (f failingHandles) GetByHandle(ctx context.Context, orgID, handle string) (*model.CommsRequest, error) {
	if strings.HasPrefix(handle, sendKeyPrefix) {
		return nil, errors.New("connection reset by peer")
	}
	return f.IRequestDAO.GetByHandle(ctx, orgID, handle)
}

// cutOffSend makes a preview of 1001 recipients, delivers it, submits chunk 1
// of 2, ages the preview past 24 hours, and returns what a resume needs.
func cutOffSend(t *testing.T) (mk func(previewID, content, recipients string, comms submitter, d *daos) *sendCmd, previewID, content, recipients string, sub *lockedSubmitter, d *daos) {
	t.Helper()
	ctx := context.Background()
	t.Cleanup(func() {
		testDB.Exec("DELETE FROM comms_messages WHERE organization_id = ?", org)
		testDB.Exec("DELETE FROM comms_requests WHERE organization_id = ?", org)
	})
	content = write(t, "c.json", `{"base":{"subject":"Maintenance tonight"}}`)
	var lines strings.Builder
	for i := range maxEntries + 1 {
		fmt.Fprintf(&lines, "user%d@example.com\n", i)
	}
	recipients = write(t, "r.txt", lines.String())
	sub = &lockedSubmitter{inner: &fakeSubmitter{}}
	e, _, _ := newEnv(t, defaults(t))
	d, _, _ = e.openDAOs()
	mk = func(previewID, content, recipients string, comms submitter, d *daos) *sendCmd {
		mode := sendModeSend
		if previewID == "" {
			mode = sendModePreview
		}
		c := &sendCmd{e: e, org: org, usecase: "notice", contentPath: content, mode: mode, previewID: previewID, d: d, comms: comms, store: memStore{}, now: time.Now, newUUID: uuid.NewString}
		if err := c.validate(recipients, []string{"reviewer@example.com"}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if err := mk("", content, recipients, sub, d).run(ctx); err != nil {
		t.Fatal(err)
	}
	row, _ := d.request.GetByHandle(ctx, org, sub.inner.calls[0].GetIdempotencyKey())
	testDB.Create(&model.CommsMessage{OrganizationID: org, RequestID: row.ID, Channel: pb.Channel_CHANNEL_EMAIL, Vendor: pb.Vendor_VENDOR_SMTP, Recipient: "reviewer@example.com", Status: pb.MessageStatus_MESSAGE_STATUS_SENT, VerificationLevel: pb.VerificationLevel_VERIFICATION_LEVEL_PROVIDER_ACK, DeliveryGuarantee: pb.DeliveryGuarantee_DELIVERY_GUARANTEE_GUARANTEED})
	if err := mk(row.ID, content, recipients, &failingAfter{inner: sub.inner, n: 2}, d).run(ctx); err == nil {
		t.Fatal("the cut-off send reported success")
	}
	testDB.Exec("UPDATE comms_requests SET created_at = now() - interval '25 hours' WHERE id = ?", row.ID)
	return mk, row.ID, content, recipients, sub, d
}

// TestResumeStillChecksThePreview pins that resuming past 24 hours lifts only
// the age limit: a changed recipient or changed content is still refused, and
// nothing new is submitted.
func TestResumeStillChecksThePreview(t *testing.T) {
	ctx := context.Background()
	mk, id, content, recipients, sub, d := cutOffSend(t)
	before := sub.count()
	changed := write(t, "r2.txt", strings.Replace(readFile(t, recipients), "user5@example.com", "other5@example.com", 1))
	if err := mk(id, content, changed, sub, d).run(ctx); err == nil || !strings.Contains(err.Error(), "changed since the preview") {
		t.Fatalf("resume with a changed recipient: %v", err)
	}
	other := write(t, "c2.json", `{"base":{"subject":"Maintenance tomorrow"}}`)
	if err := mk(id, other, recipients, sub, d).run(ctx); err == nil || !strings.Contains(err.Error(), "changed since the preview") {
		t.Fatalf("resume with changed content: %v", err)
	}
	if n := sub.count() - before; n != 0 {
		t.Fatalf("%d submit(s) after refused resumes, want 0", n)
	}
}

// TestResumeProbeReportsLookupErrors pins that a database error while looking
// for earlier chunks is returned, not taken to mean a fresh send (which would
// then fail on the preview's age with a misleading message).
func TestResumeProbeReportsLookupErrors(t *testing.T) {
	ctx := context.Background()
	mk, id, content, recipients, sub, d := cutOffSend(t)
	before := sub.count()
	broken := &daos{reconcile: d.reconcile, suppression: d.suppression, request: failingHandles{d.request}, message: d.message}
	err := mk(id, content, recipients, sub, broken).run(ctx)
	if err == nil || !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("resume with a failing lookup: %v, want the database error", err)
	}
	if n := sub.count() - before; n != 0 {
		t.Fatalf("%d submit(s), want 0", n)
	}
}

// TestConcurrentResumesSendEachChunkOnce pins that two resumes racing each
// other still leave one request per chunk.
func TestConcurrentResumesSendEachChunkOnce(t *testing.T) {
	ctx := context.Background()
	mk, id, content, recipients, sub, d := cutOffSend(t)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	cmds := []*sendCmd{mk(id, content, recipients, sub, d), mk(id, content, recipients, sub, d)}
	for i := range cmds {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = cmds[i].run(ctx) }()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
	}
	var n int64
	testDB.Model(&model.CommsRequest{}).Where("organization_id = ? AND idempotency_key LIKE ?", org, sendKeyPrefix+id+"%").Count(&n)
	if n != 2 {
		t.Fatalf("%d chunk requests, want 2", n)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
