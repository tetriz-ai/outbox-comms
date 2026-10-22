// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package queue

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hibiken/asynq"
)

func TestPermanent(t *testing.T) {
	base := errors.New("bad body")
	p := Permanent(base)
	if !IsPermanent(p) || !errors.Is(p, base) || IsPermanent(base) || IsPermanent(nil) {
		t.Fatal("Permanent must wrap and be detectable")
	}
}

func TestTriggerRoundTrip(t *testing.T) {
	b, _ := encodeRequest("org", "req")
	r, err := decodeRequest(b)
	if err != nil || r.GetOrganizationId() != "org" || r.GetRequestId() != "req" {
		t.Fatalf("request trigger: %v %v", r, err)
	}
	b, _ = encodeDispatch("org", "msg")
	d, err := decodeDispatch(b)
	if err != nil || d.GetMessageId() != "msg" {
		t.Fatalf("dispatch trigger: %v %v", d, err)
	}
	if _, err := decodeDispatch([]byte("{not json")); !IsPermanent(err) {
		t.Fatalf("a body that does not decode is permanent, got %v", err)
	}
}

// TestAsynqPublishAndRoute proves both triggers land on the outbox_comms queue
// and the registered handlers receive their ids; a permanent handler error
// skips asynq's retries.
func TestAsynqPublishAndRoute(t *testing.T) {
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}
	client := asynq.NewClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	p := NewAsynq(client)
	ctx := context.Background()
	if err := p.PublishRequest(ctx, "o1", "r1"); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishDispatch(ctx, "o1", "m1"); err != nil {
		t.Fatal(err)
	}
	insp := asynq.NewInspector(opt)
	t.Cleanup(func() { _ = insp.Close() })
	tasks, err := insp.ListPendingTasks(AsynqQueue)
	if err != nil || len(tasks) != 2 {
		t.Fatalf("pending tasks = %v, %v", tasks, err)
	}

	var mu sync.Mutex
	got := map[string]string{}
	mux := asynq.NewServeMux()
	RegisterAsynq(mux, Handlers{
		Request: func(_ context.Context, org, id string) error {
			mu.Lock()
			got["request"] = org + "/" + id
			mu.Unlock()
			return nil
		},
		Dispatch: func(_ context.Context, org, id string) error {
			mu.Lock()
			got["dispatch"] = org + "/" + id
			mu.Unlock()
			return Permanent(errors.New("gone"))
		},
	})
	for _, task := range tasks {
		err := mux.ProcessTask(ctx, asynq.NewTask(task.Type, task.Payload))
		if task.Type == TaskDispatch && !errors.Is(err, asynq.SkipRetry) {
			t.Fatalf("a permanent error must skip retries, got %v", err)
		}
		if task.Type == TaskRequest && err != nil {
			t.Fatal(err)
		}
	}
	if got["request"] != "o1/r1" || got["dispatch"] != "o1/m1" {
		t.Fatalf("handlers got %v", got)
	}
}

// fakeSQS is a one-queue SQS stand-in: ReceiveMessage hands out what was sent
// and remembers deletes.
type fakeSQS struct {
	mu      sync.Mutex
	pending []sqstypes.Message
	sent    []string
	deleted []string
}

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, aws.ToString(in.QueueUrl)+" "+aws.ToString(in.MessageBody))
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	if len(f.pending) > 0 {
		out := f.pending
		f.pending = nil
		f.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: out}, nil
	}
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQS) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func msg(handle, body, count string) sqstypes.Message {
	return sqstypes.Message{ReceiptHandle: aws.String(handle), Body: aws.String(body), Attributes: map[string]string{"ApproximateReceiveCount": count}}
}

// TestSQSPoller proves a success and a permanent failure delete the message, a
// transient failure leaves it for redelivery, and the receive count reaches the
// handler.
func TestSQSPoller(t *testing.T) {
	f := &fakeSQS{pending: []sqstypes.Message{msg("ok", "a", "1"), msg("perm", "b", "2"), msg("retry", "c", "3")}}
	var mu sync.Mutex
	counts := map[string]int{}
	p := &Poller{Client: f, QueueURL: "q", Workers: 1, Handle: func(_ context.Context, m Message) error {
		mu.Lock()
		counts[string(m.Body)] = m.ReceiveCount
		mu.Unlock()
		switch string(m.Body) {
		case "b":
			return Permanent(errors.New("poison"))
		case "c":
			return errors.New("try again")
		}
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p.Run(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 2 || f.deleted[0] != "ok" || f.deleted[1] != "perm" {
		t.Fatalf("deleted %v, want [ok perm]", f.deleted)
	}
	if counts["a"] != 1 || counts["c"] != 3 {
		t.Fatalf("receive counts %v", counts)
	}
}

func TestSQSPublish(t *testing.T) {
	f := &fakeSQS{}
	s := NewSQS(f, "req-url", "disp-url")
	_ = s.PublishRequest(context.Background(), "o", "r")
	_ = s.PublishDispatch(context.Background(), "o", "m")
	if len(f.sent) != 2 || !strings.HasPrefix(f.sent[0], "req-url ") || !strings.HasPrefix(f.sent[1], "disp-url ") {
		t.Fatalf("sent %v", f.sent)
	}
	r, err := decodeRequest([]byte(strings.TrimPrefix(f.sent[0], "req-url ")))
	if err != nil || r.GetRequestId() != "r" {
		t.Fatalf("request body: %v %v", r, err)
	}
	d, err := decodeDispatch([]byte(strings.TrimPrefix(f.sent[1], "disp-url ")))
	if err != nil || d.GetMessageId() != "m" {
		t.Fatalf("dispatch body: %v %v", d, err)
	}
	if n := len(s.Pollers(Handlers{}, 1)); n != 2 {
		t.Fatalf("Pollers = %d, want one per queue", n)
	}
}
