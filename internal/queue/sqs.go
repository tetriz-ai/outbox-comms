// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSAPI is the subset of the SQS client the transport calls; *sqs.Client
// satisfies it.
type SQSAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// SQS is the Amazon SQS transport: one queue for request triggers, one for
// dispatch triggers. Redelivery and dead-lettering follow the queues' own
// visibility timeout and redrive policy.
type SQS struct {
	client      SQSAPI
	requestsURL string
	dispatchURL string
}

// NewSQS builds the publisher over the two queue URLs.
func NewSQS(client SQSAPI, requestsURL, dispatchURL string) *SQS {
	return &SQS{client: client, requestsURL: requestsURL, dispatchURL: dispatchURL}
}

var _ Publisher = (*SQS)(nil)

// PublishRequest sends a fan-out trigger.
func (s *SQS) PublishRequest(ctx context.Context, orgID, requestID string) error {
	b, err := encodeRequest(orgID, requestID)
	if err != nil {
		return err
	}
	if _, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(s.requestsURL), MessageBody: aws.String(string(b))}); err != nil {
		return fmt.Errorf("publish request trigger (org=%s, req=%s): %w", orgID, requestID, err)
	}
	return nil
}

// PublishDispatch sends a dispatch trigger.
func (s *SQS) PublishDispatch(ctx context.Context, orgID, messageID string) error {
	b, err := encodeDispatch(orgID, messageID)
	if err != nil {
		return err
	}
	if _, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(s.dispatchURL), MessageBody: aws.String(string(b))}); err != nil {
		return fmt.Errorf("publish dispatch trigger (org=%s, msg=%s): %w", orgID, messageID, err)
	}
	return nil
}

// Pollers returns the two pollers that deliver this transport's triggers to h.
func (s *SQS) Pollers(h Handlers, workers int) []*Poller {
	return []*Poller{
		{Client: s.client, QueueURL: s.requestsURL, Workers: workers, Handle: func(ctx context.Context, m Message) error {
			t, err := decodeRequest(m.Body)
			if err != nil {
				return err
			}
			return h.Request(ctx, t.GetOrganizationId(), t.GetRequestId())
		}},
		{Client: s.client, QueueURL: s.dispatchURL, Workers: workers, Handle: func(ctx context.Context, m Message) error {
			t, err := decodeDispatch(m.Body)
			if err != nil {
				return err
			}
			return h.Dispatch(ctx, t.GetOrganizationId(), t.GetMessageId())
		}},
	}
}

// Message is one received SQS message as a handler sees it.
type Message struct {
	Body []byte
	// ReceiveCount is SQS's ApproximateReceiveCount: 1 on the first delivery.
	ReceiveCount int
}

// Poller long-polls one SQS queue and hands each message to Handle. A nil error
// or a Permanent one deletes the message; any other error leaves it for SQS to
// redeliver after the visibility timeout.
type Poller struct {
	Client   SQSAPI
	QueueURL string
	Workers  int
	Handle   func(ctx context.Context, m Message) error
	// Logger receives the poller's log lines. Nil means slog's default.
	Logger *slog.Logger
}

// Run polls until ctx is cancelled, then waits for in-flight messages.
func (p *Poller) Run(ctx context.Context) {
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx)
		}()
	}
	wg.Wait()
}

func (p *Poller) loop(ctx context.Context) {
	for ctx.Err() == nil {
		out, err := p.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(p.QueueURL),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             20,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			p.log().ErrorContext(ctx, "sqs: receive failed", "queue", p.QueueURL, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		for _, m := range out.Messages {
			p.handle(ctx, m)
		}
	}
}

func (p *Poller) handle(ctx context.Context, m sqstypes.Message) {
	count, _ := strconv.Atoi(m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)])
	// Handling continues past shutdown so an in-flight message finishes cleanly.
	hctx := context.WithoutCancel(ctx)
	err := p.Handle(hctx, Message{Body: []byte(aws.ToString(m.Body)), ReceiveCount: count})
	if err != nil && !IsPermanent(err) {
		p.log().WarnContext(ctx, "sqs: handler failed; message will be redelivered", "queue", p.QueueURL, "receive_count", count, "error", err)
		return
	}
	if err != nil {
		p.log().ErrorContext(ctx, "sqs: dropping message after a permanent failure", "queue", p.QueueURL, "error", err)
	}
	if _, derr := p.Client.DeleteMessage(hctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(p.QueueURL), ReceiptHandle: m.ReceiptHandle}); derr != nil {
		p.log().ErrorContext(ctx, "sqs: delete failed; message will be redelivered", "queue", p.QueueURL, "error", derr)
	}
}

func (p *Poller) log() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
