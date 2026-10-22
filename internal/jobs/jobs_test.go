// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testQueue = "outbox_comms"

// TestSourceEmitsCrons asserts the source emits exactly the reconcile and prune
// crons on the configured queue; the retry task is ad hoc and must not appear.
func TestSourceEmitsCrons(t *testing.T) {
	defs, err := Source{Queue: testQueue, ReconcileCron: "*/10 * * * *", PruneCron: "0 0 * * *"}.FetchSchedules(context.Background())
	require.NoError(t, err)
	require.Len(t, defs, 2)
	byJob := map[string]string{}
	for _, d := range defs {
		byJob[d.JobType] = d.CronExpression
		assert.Equal(t, testQueue, d.Queue, "job %s", d.JobType)
	}
	assert.Equal(t, "*/10 * * * *", byJob[TaskReconcile])
	assert.Equal(t, "0 0 * * *", byJob[TaskPrune])
	_, hasRetry := byJob[TaskRetry]
	assert.False(t, hasRetry, "retry must not be cron-registered")
}

// TestRetryTaskIDDeterministic locks the dedup id shape: the same
// (message, attempts) always maps to one id; a different attempt is a distinct id.
func TestRetryTaskIDDeterministic(t *testing.T) {
	assert.Equal(t, "dispatch:msg-1:3", retryTaskID("msg-1", 3))
	assert.NotEqual(t, retryTaskID("msg-1", 3), retryTaskID("msg-1", 4))
}

// TestScheduleEnqueuesDelayedTask proves RetryScheduler.Schedule stamps the
// deterministic id, the default queue, the retry type, ProcessAt at
// next_retry_at, and a payload carrying org+message.
func TestScheduleEnqueuesDelayedTask(t *testing.T) {
	mr := miniredis.RunT(t)
	redisOpt := asynq.RedisClientOpt{Addr: mr.Addr()}
	client := asynq.NewClient(redisOpt)
	t.Cleanup(func() { _ = client.Close() })

	s := NewRetryScheduler(client, testQueue)
	processAt := time.Now().Add(30 * time.Minute).UTC().Truncate(time.Second)
	require.NoError(t, s.Schedule(context.Background(), "org-1", "msg-7", 2, processAt))

	insp := asynq.NewInspector(redisOpt)
	t.Cleanup(func() { _ = insp.Close() })
	scheduled, err := insp.ListScheduledTasks(testQueue)
	require.NoError(t, err)
	require.Len(t, scheduled, 1)

	info := scheduled[0]
	assert.Equal(t, TaskRetry, info.Type)
	assert.Equal(t, testQueue, info.Queue)
	assert.Equal(t, "dispatch:msg-7:2", info.ID)
	assert.WithinDuration(t, processAt, info.NextProcessAt, time.Second)

	var p retryPayload
	require.NoError(t, json.Unmarshal(info.Payload, &p))
	assert.Equal(t, "org-1", p.OrgID)
	assert.Equal(t, "msg-7", p.MessageID)
	assert.Equal(t, 2, p.Attempts)
}

// TestScheduleDedup proves a re-enqueue with the same (message, attempts) is a
// no-op (ErrTaskIDConflict swallowed).
func TestScheduleDedup(t *testing.T) {
	mr := miniredis.RunT(t)
	redisOpt := asynq.RedisClientOpt{Addr: mr.Addr()}
	client := asynq.NewClient(redisOpt)
	t.Cleanup(func() { _ = client.Close() })

	s := NewRetryScheduler(client, testQueue)
	at := time.Now().Add(time.Hour)
	require.NoError(t, s.Schedule(context.Background(), "org-1", "msg-7", 1, at))
	require.NoError(t, s.Schedule(context.Background(), "org-1", "msg-7", 1, at), "duplicate enqueue must be a no-op")

	insp := asynq.NewInspector(redisOpt)
	t.Cleanup(func() { _ = insp.Close() })
	scheduled, err := insp.ListScheduledTasks(testQueue)
	require.NoError(t, err)
	assert.Len(t, scheduled, 1, "second enqueue of same id must not add a task")
}

// --- handler routing ---

type fakeReconciler struct{ runs, prunes int }

func (f *fakeReconciler) Run(context.Context) error   { f.runs++; return nil }
func (f *fakeReconciler) Prune(context.Context) error { f.prunes++; return nil }

type recordingDispatch struct {
	calls   int
	lastOrg string
	lastMsg string
	err     error
}

func (r *recordingDispatch) PublishDispatch(_ context.Context, orgID, messageID string) error {
	r.calls++
	r.lastOrg = orgID
	r.lastMsg = messageID
	return r.err
}

func TestProcessTaskRouting(t *testing.T) {
	rec := &fakeReconciler{}
	disp := &recordingDispatch{}
	h := NewHandler(rec, disp)
	ctx := context.Background()

	require.NoError(t, h.ProcessTask(ctx, asynq.NewTask(TaskReconcile, []byte("{}"))))
	assert.Equal(t, 1, rec.runs)
	assert.Equal(t, 0, rec.prunes)

	require.NoError(t, h.ProcessTask(ctx, asynq.NewTask(TaskPrune, []byte("{}"))))
	assert.Equal(t, 1, rec.prunes)

	body, _ := json.Marshal(retryPayload{OrgID: "org-9", MessageID: "msg-9", Attempts: 1})
	require.NoError(t, h.ProcessTask(ctx, asynq.NewTask(TaskRetry, body)))
	assert.Equal(t, 1, disp.calls)
	assert.Equal(t, "org-9", disp.lastOrg)
	assert.Equal(t, "msg-9", disp.lastMsg)

	err := h.ProcessTask(ctx, asynq.NewTask("outbox_comms:unknown", []byte("{}")))
	assert.Error(t, err, "unknown task type must error, never silently drop")
}

// TestRetryRoutingPropagatesPublishError proves a re-dispatch failure surfaces
// (so asynq retries the retry task) rather than being swallowed.
func TestRetryRoutingPropagatesPublishError(t *testing.T) {
	disp := &recordingDispatch{err: errors.New("queue down")}
	h := NewHandler(&fakeReconciler{}, disp)
	body, _ := json.Marshal(retryPayload{OrgID: "o", MessageID: "m", Attempts: 1})
	err := h.ProcessTask(context.Background(), asynq.NewTask(TaskRetry, body))
	assert.Error(t, err)
}
