// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package sender_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/tetriz-ai/outbox-comms/gen/outboxcomms/v1"
	"github.com/tetriz-ai/outbox-comms/internal/dispatch"
	"github.com/tetriz-ai/outbox-comms/internal/sender"
	"github.com/tetriz-ai/outbox-comms/internal/slackclient"
)

// provider is a raw TCP endpoint scripted per connection. It counts the
// requests it read in full, which is what "the provider got it" means here.
type provider struct {
	addr  string
	calls atomic.Int32
}

// answer reads one request (headers, then Content-Length bytes) unless
// readBody is false, and then runs reply on the connection.
func newProvider(t *testing.T, readBody bool, reply func(c net.Conn)) *provider {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &provider{addr: ln.Addr().String()}
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
				if readBody {
					if _, err := io.CopyN(io.Discard, r, int64(length)); err != nil {
						return
					}
					p.calls.Add(1)
				}
				reply(c)
			}(conn)
		}
	}()
	return p
}

// resetOnAccept resets every connection as soon as it is accepted, before any
// byte of a TLS handshake is answered.
func resetOnAccept(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			reset(c)
			_ = c.Close()
		}
	}()
	return ln.Addr().String()
}

func reset(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
}

func raw(s string) func(net.Conn) {
	return func(c net.Conn) { _, _ = io.WriteString(c, s) }
}

func status(code int, body string) func(net.Conn) {
	return raw(fmt.Sprintf("HTTP/1.1 %d %s\r\nContent-Type: text/xml\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", code, http.StatusText(code), len(body), body))
}

// offlineDNS resolves through a closed local port, so every lookup fails
// without leaving the machine.
func offlineTransport(t *testing.T) *http.Transport {
	t.Helper()
	dead := closedAddr(t)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", dead)
	}}
	return &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Resolver: resolver}).DialContext}
}

func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

const sesOK = `<SendEmailResponse><SendEmailResult><MessageId>0100-ses-1</MessageId></SendEmailResult></SendEmailResponse>`

func sesSender(endpoint string, httpClient *http.Client) *sender.EmailSender {
	cfg := aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		}),
	}
	if httpClient != nil {
		cfg.HTTPClient = httpClient
	}
	return sender.NewEmailSender(sender.NewSESClient(cfg), nil, "noreply@example.com", "")
}

func slackSender(t *testing.T, base string, httpClient *http.Client) *sender.SlackSender {
	t.Helper()
	if httpClient == nil {
		httpClient = &http.Client{Transport: &http.Transport{Proxy: nil}}
	}
	c, err := slackclient.New(httpClient, base+"/api")
	if err != nil {
		t.Fatal(err)
	}
	return sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, nil)
}

func emailIn() dispatch.SendInput {
	return dispatch.SendInput{Channel: pb.Channel_CHANNEL_EMAIL, Recipient: "dev@example.com",
		Rendered: &pb.RenderedPayload{EmailSubject: "Hi", EmailHtml: "<p>hello</p>"}}
}

func slackIn(blocks string) dispatch.SendInput {
	return dispatch.SendInput{Channel: pb.Channel_CHANNEL_SLACK, Recipient: "C0TRANSPORT", SlackDelivery: pb.SlackDelivery_SLACK_DELIVERY_LINK,
		Rendered: &pb.RenderedPayload{SlackText: "hi", SlackBlocksJson: blocks}}
}

// TestTransportShapes pins how each way a provider call can end is read. A
// failure before the request was written sends nothing, so it is transient.
// After the write, any failure without a parsed provider error code may have
// sent, so it is uncertain. Each case also counts the requests the provider
// read in full. TestDispatchOutcomes pins what each outcome does under each
// delivery guarantee.
func TestTransportShapes(t *testing.T) {
	bigBlocks := `[{"type":"section","text":{"type":"mrkdwn","text":"` + strings.Repeat("x", 8<<20) + `"}}]`
	type tc struct {
		name      string
		send      func(t *testing.T) (dispatch.SendResult, int32)
		want      dispatch.Outcome
		wantCalls int32
	}
	ses := func(readBody bool, reply func(net.Conn), ctx func() (context.Context, context.CancelFunc)) func(t *testing.T) (dispatch.SendResult, int32) {
		return func(t *testing.T) (dispatch.SendResult, int32) {
			p := newProvider(t, readBody, reply)
			c, cancel := ctx()
			defer cancel()
			res := sesSender("http://"+p.addr, nil).Send(c, emailIn())
			return res, p.calls.Load()
		}
	}
	slack := func(readBody bool, reply func(net.Conn), blocks string, client *http.Client) func(t *testing.T) (dispatch.SendResult, int32) {
		return func(t *testing.T) (dispatch.SendResult, int32) {
			p := newProvider(t, readBody, reply)
			res := slackSender(t, "http://"+p.addr, client).Send(context.Background(), slackIn(blocks))
			return res, p.calls.Load()
		}
	}
	bg := func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }
	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 300*time.Millisecond)
	}
	slow := func(net.Conn) { time.Sleep(2 * time.Second) }
	cases := []tc{
		// After the write.
		{"ses/eof_no_answer", ses(true, func(net.Conn) {}, bg), dispatch.OutcomeUncertain, 1},
		{"ses/reset_after_write", ses(true, reset, bg), dispatch.OutcomeUncertain, 1},
		{"ses/cut_status_line", ses(true, raw("HTTP/1.1 2"), bg), dispatch.OutcomeUncertain, 1},
		{"ses/cut_body", ses(true, raw("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n<SendEmail"), bg), dispatch.OutcomeUncertain, 1},
		{"ses/reset_mid_body", ses(true, func(c net.Conn) {
			_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n<SendEmail")
			reset(c)
		}, bg), dispatch.OutcomeUncertain, 1},
		{"ses/200_not_xml", ses(true, status(200, "<html>proxy</html>"), bg), dispatch.OutcomeUncertain, 1},
		{"ses/200_empty_message_id", ses(true, status(200, `<SendEmailResponse><SendEmailResult><MessageId></MessageId></SendEmailResult></SendEmailResponse>`), bg), dispatch.OutcomeUncertain, 1},
		{"ses/500", ses(true, status(500, ""), bg), dispatch.OutcomeUncertain, 1},
		{"ses/502", ses(true, status(502, "<html>bad gateway</html>"), bg), dispatch.OutcomeUncertain, 1},
		{"ses/503", ses(true, status(503, ""), bg), dispatch.OutcomeUncertain, 1},
		{"ses/504", ses(true, status(504, ""), bg), dispatch.OutcomeUncertain, 1},
		{"ses/deadline_after_write", ses(true, slow, short), dispatch.OutcomeUncertain, 1},
		{"ses/400_message_rejected", ses(true, status(400, `<ErrorResponse><Error><Type>Sender</Type><Code>MessageRejected</Code><Message>no</Message></Error><RequestId>r</RequestId></ErrorResponse>`), bg), dispatch.OutcomePermanent, 1},
		{"ses/400_throttling", ses(true, status(400, `<ErrorResponse><Error><Type>Sender</Type><Code>Throttling</Code><Message>slow</Message></Error><RequestId>r</RequestId></ErrorResponse>`), bg), dispatch.OutcomeTransient, 1},
		{"ses/success", ses(true, status(200, sesOK), bg), dispatch.OutcomeSuccess, 1},
		{"slack/eof_no_answer", slack(true, func(net.Conn) {}, "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/reset_after_write", slack(true, reset, "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/cut_status_line", slack(true, raw("HTTP/1.1 2"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/garbage", slack(true, raw("not http at all\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/200_not_json", slack(true, raw("HTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n<html>proxy</html>"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/200_empty", slack(true, raw("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/500", slack(true, raw("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/502", slack(true, raw("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/503", slack(true, raw("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/504", slack(true, raw("HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\n\r\n"), "", nil), dispatch.OutcomeUncertain, 1},
		{"slack/client_timeout_after_write", slack(true, slow, "", &http.Client{Timeout: 300 * time.Millisecond, Transport: &http.Transport{Proxy: nil}}), dispatch.OutcomeUncertain, 1},
		// Before the write.
		{"slack/broken_pipe_mid_request", slack(false, reset, bigBlocks, nil), dispatch.OutcomeTransient, 0},
		{"ses/broken_pipe_mid_request", func(t *testing.T) (dispatch.SendResult, int32) {
			p := newProvider(t, false, reset)
			in := emailIn()
			in.Rendered.EmailHtml = "<p>" + strings.Repeat("x", 8<<20) + "</p>"
			return sesSender("http://"+p.addr, nil).Send(context.Background(), in), p.calls.Load()
		}, dispatch.OutcomeTransient, 0},
		{"ses/dial_refused", func(t *testing.T) (dispatch.SendResult, int32) {
			return sesSender("http://"+closedAddr(t), nil).Send(context.Background(), emailIn()), 0
		}, dispatch.OutcomeTransient, 0},
		{"slack/dial_refused", func(t *testing.T) (dispatch.SendResult, int32) {
			return slackSender(t, "http://"+closedAddr(t), nil).Send(context.Background(), slackIn("")), 0
		}, dispatch.OutcomeTransient, 0},
		{"ses/dns_failure", func(t *testing.T) (dispatch.SendResult, int32) {
			return sesSender("http://provider.invalid", &http.Client{Transport: offlineTransport(t)}).Send(context.Background(), emailIn()), 0
		}, dispatch.OutcomeTransient, 0},
		{"slack/dns_failure", func(t *testing.T) (dispatch.SendResult, int32) {
			return slackSender(t, "http://provider.invalid", &http.Client{Transport: offlineTransport(t)}).Send(context.Background(), slackIn("")), 0
		}, dispatch.OutcomeTransient, 0},
		{"ses/tls_handshake_reset", func(t *testing.T) (dispatch.SendResult, int32) {
			client := &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
			return sesSender("https://"+resetOnAccept(t), client).Send(context.Background(), emailIn()), 0
		}, dispatch.OutcomeTransient, 0},
		{"slack/tls_handshake_reset", func(t *testing.T) (dispatch.SendResult, int32) {
			client := &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
			return slackSender(t, "https://"+resetOnAccept(t), client).Send(context.Background(), slackIn("")), 0
		}, dispatch.OutcomeTransient, 0},
		{"ses/cancelled_before_call", func(t *testing.T) (dispatch.SendResult, int32) {
			p := newProvider(t, true, status(200, sesOK))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return sesSender("http://"+p.addr, nil).Send(ctx, emailIn()), p.calls.Load()
		}, dispatch.OutcomeTransient, 0},
		{"slack/token_lookup_deadline", func(t *testing.T) (dispatch.SendResult, int32) {
			p := newProvider(t, true, raw("HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}"))
			c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, "http://"+p.addr+"/api")
			if err != nil {
				t.Fatal(err)
			}
			s := sender.NewSlackSender(slowTokens{}, c, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			return s.Send(ctx, slackIn("")), p.calls.Load()
		}, dispatch.OutcomeTransient, 0},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			res, calls := tt.send(t)
			if res.Outcome != tt.want {
				t.Fatalf("outcome = %v, want %v (err %v)", res.Outcome, tt.want, res.Err)
			}
			if calls != tt.wantCalls {
				t.Fatalf("the provider read %d requests, want %d", calls, tt.wantCalls)
			}
		})
	}
}

// slowTokens is a TokenProvider that answers only after its context ends.
type slowTokens struct{}

func (slowTokens) Token(ctx context.Context, _ string) (string, error) {
	<-ctx.Done()
	return "", fmt.Errorf("token store: %w", ctx.Err())
}

// TestSlackFileUploadSteps pins which step of a Slack file upload can leave a
// send uncertain. The first two steps, getting the upload URL and posting the
// bytes, share nothing with the channel, so any failure there is transient.
// Only completeUploadExternal shares the file, so a failure after it was
// written is uncertain.
func TestSlackFileUploadSteps(t *testing.T) {
	cut := func(w http.ResponseWriter) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}
	fail := map[string]func(http.ResponseWriter){
		"500":      func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
		"cut":      cut,
		"not_json": func(w http.ResponseWriter) { _, _ = io.WriteString(w, "<html>proxy</html>") },
		"internal_error": func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, `{"ok":false,"error":"internal_error"}`)
		},
	}
	for _, step := range []string{"files.getUploadURLExternal", "upload", "files.completeUploadExternal"} {
		for mode, reply := range fail {
			if step == "upload" && (mode == "internal_error" || mode == "not_json") {
				continue // the byte upload answers by status only
			}
			want := dispatch.OutcomeTransient
			if step == "files.completeUploadExternal" {
				want = dispatch.OutcomeUncertain
			}
			t.Run(step+"/"+mode, func(t *testing.T) {
				var completes atomic.Int32
				var srv *httptest.Server
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api"), "/")
					if name == "files.completeUploadExternal" {
						completes.Add(1)
					}
					if name == step {
						reply(w)
						return
					}
					switch name {
					case "files.getUploadURLExternal":
						_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q,"file_id":"F0FILE"}`, srv.URL+"/upload")
					default:
						_, _ = io.WriteString(w, `{"ok":true}`)
					}
				}))
				t.Cleanup(srv.Close)
				c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, srv.URL+"/api")
				if err != nil {
					t.Fatal(err)
				}
				s := sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, fakeFetcher{data: []byte("%PDF-1.4")})
				in := slackIn("")
				in.SlackDelivery = pb.SlackDelivery_SLACK_DELIVERY_FILE
				in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
				res := s.Send(context.Background(), in)
				if res.Outcome != want {
					t.Fatalf("outcome = %v, want %v (err %v)", res.Outcome, want, res.Err)
				}
				if step != "files.completeUploadExternal" && completes.Load() != 0 {
					t.Fatalf("completeUploadExternal ran %d times after an earlier step failed", completes.Load())
				}
			})
		}
	}
}

// sesRawSender is sesSender with an attachment store, so a send carrying an
// attachment_key takes the raw-message path.
func sesRawSender(endpoint string) *sender.EmailSender {
	cfg := aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
		}),
	}
	return sender.NewEmailSender(sender.NewSESClient(cfg), fakeFetcher{data: []byte("%PDF-1.4")}, "noreply@example.com", "")
}

func emailWithAttachment() dispatch.SendInput {
	in := emailIn()
	in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
	return in
}

// TestRawEmailTransportShapes pins the raw-message (attachment) path to the same
// rule as the plain one: after the write, a failure with no SES error code, or
// an answer with no MessageId, is uncertain; before it, transient.
func TestRawEmailTransportShapes(t *testing.T) {
	short := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 300*time.Millisecond)
	}
	bg := func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }
	for _, tc := range []struct {
		name      string
		reply     func(net.Conn)
		ctx       func() (context.Context, context.CancelFunc)
		want      dispatch.Outcome
		wantCalls int32
	}{
		{"eof_no_answer", func(net.Conn) {}, bg, dispatch.OutcomeUncertain, 1},
		{"reset_after_write", reset, bg, dispatch.OutcomeUncertain, 1},
		{"503", status(503, ""), bg, dispatch.OutcomeUncertain, 1},
		{"deadline_after_write", func(net.Conn) { time.Sleep(2 * time.Second) }, short, dispatch.OutcomeUncertain, 1},
		{"200_empty_message_id", status(200, `<SendRawEmailResponse><SendRawEmailResult><MessageId></MessageId></SendRawEmailResult></SendRawEmailResponse>`), bg, dispatch.OutcomeUncertain, 1},
		{"success", status(200, `<SendRawEmailResponse><SendRawEmailResult><MessageId>0100-raw-1</MessageId></SendRawEmailResult></SendRawEmailResponse>`), bg, dispatch.OutcomeSuccess, 1},
	} {
		t.Run("ses_raw/"+tc.name, func(t *testing.T) {
			p := newProvider(t, true, tc.reply)
			ctx, cancel := tc.ctx()
			defer cancel()
			res := sesRawSender("http://"+p.addr).Send(ctx, emailWithAttachment())
			if res.Outcome != tc.want || p.calls.Load() != tc.wantCalls {
				t.Fatalf("outcome %v (err %v), provider read %d; want %v, %d", res.Outcome, res.Err, p.calls.Load(), tc.want, tc.wantCalls)
			}
		})
	}
	t.Run("ses_raw/broken_pipe_mid_request", func(t *testing.T) {
		p := newProvider(t, false, reset)
		cfg := aws.Config{Region: "us-east-1", BaseEndpoint: aws.String("http://" + p.addr),
			Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only"}, nil
			})}
		s := sender.NewEmailSender(sender.NewSESClient(cfg), fakeFetcher{data: []byte(strings.Repeat("x", 6<<20))}, "noreply@example.com", "")
		if res := s.Send(context.Background(), emailWithAttachment()); res.Outcome != dispatch.OutcomeTransient || p.calls.Load() != 0 {
			t.Fatalf("outcome %v (err %v), provider read %d; want transient, 0", res.Outcome, res.Err, p.calls.Load())
		}
	})
	t.Run("ses_raw/dial_refused", func(t *testing.T) {
		if res := sesRawSender("http://"+closedAddr(t)).Send(context.Background(), emailWithAttachment()); res.Outcome != dispatch.OutcomeTransient {
			t.Fatalf("outcome %v (err %v), want transient", res.Outcome, res.Err)
		}
	})
}

// failCompleteTransport fails the files.completeUploadExternal request before
// writing a byte of it, and passes every other request through.
type failCompleteTransport struct{ next http.RoundTripper }

func (f failCompleteTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/files.completeUploadExternal") {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return f.next.RoundTrip(r)
}

// TestSlackStatusAnswers pins Slack's 4xx answers without a JSON error code. At
// the byte upload nothing is shared yet, so the send is transient and the file
// is never completed; on chat.postMessage, after the write, it is uncertain. A
// completeUploadExternal that fails before its write is transient, and an
// ok:true with no ts or file id counts as success with an empty provider id.
func TestSlackStatusAnswers(t *testing.T) {
	type answer struct {
		code int
		body string
	}
	serve := func(t *testing.T, at string, a answer, client *http.Client) (dispatch.SendResult, int32) {
		var completes atomic.Int32
		var srv *httptest.Server
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/api"), "/")
			if name == "files.completeUploadExternal" {
				completes.Add(1)
			}
			if name == at {
				w.WriteHeader(a.code)
				_, _ = io.WriteString(w, a.body)
				return
			}
			switch name {
			case "files.getUploadURLExternal":
				_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q,"file_id":"F0FILE"}`, srv.URL+"/upload")
			default:
				_, _ = io.WriteString(w, `{"ok":true,"ts":"1.2"}`)
			}
		}))
		t.Cleanup(srv.Close)
		if client == nil {
			client = &http.Client{Transport: &http.Transport{Proxy: nil}}
		}
		c, err := slackclient.New(client, srv.URL+"/api")
		if err != nil {
			t.Fatal(err)
		}
		s := sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, fakeFetcher{data: []byte("%PDF-1.4")})
		in := slackIn("")
		if at != "chat.postMessage" {
			in.SlackDelivery = pb.SlackDelivery_SLACK_DELIVERY_FILE
			in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
		}
		res := s.Send(context.Background(), in)
		return res, completes.Load()
	}
	for _, code := range []int{400, 403, 413} {
		t.Run(fmt.Sprintf("upload/%d", code), func(t *testing.T) {
			res, completes := serve(t, "upload", answer{code, "no"}, nil)
			if res.Outcome != dispatch.OutcomeTransient || completes != 0 {
				t.Fatalf("outcome %v (err %v), completes %d; want transient, 0", res.Outcome, res.Err, completes)
			}
		})
		t.Run(fmt.Sprintf("chat.postMessage/%d", code), func(t *testing.T) {
			res, _ := serve(t, "chat.postMessage", answer{code, "no"}, nil)
			if res.Outcome != dispatch.OutcomeUncertain {
				t.Fatalf("outcome %v (err %v), want uncertain", res.Outcome, res.Err)
			}
		})
	}
	t.Run("complete/before_write", func(t *testing.T) {
		client := &http.Client{Transport: failCompleteTransport{next: &http.Transport{Proxy: nil}}}
		res, _ := serve(t, "", answer{}, client)
		if res.Outcome != dispatch.OutcomeTransient {
			t.Fatalf("outcome %v (err %v), want transient", res.Outcome, res.Err)
		}
	})
	t.Run("ok_without_id/chat.postMessage", func(t *testing.T) {
		res, _ := serve(t, "chat.postMessage", answer{200, `{"ok":true}`}, nil)
		if res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID != "" {
			t.Fatalf("outcome %v id %q (err %v), want success with an empty id", res.Outcome, res.ProviderMessageID, res.Err)
		}
	})
	t.Run("ok_without_id/file", func(t *testing.T) {
		var srv *httptest.Server
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if strings.HasSuffix(r.URL.Path, "/files.getUploadURLExternal") {
				_, _ = fmt.Fprintf(w, `{"ok":true,"upload_url":%q}`, srv.URL+"/upload")
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		}))
		t.Cleanup(srv.Close)
		c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, srv.URL+"/api")
		if err != nil {
			t.Fatal(err)
		}
		in := slackIn("")
		in.SlackDelivery = pb.SlackDelivery_SLACK_DELIVERY_FILE
		in.Params, _ = structpb.NewStruct(map[string]any{sender.AttachmentKeyParam: "reports/r.pdf"})
		res := sender.NewSlackSender(sender.StaticTokens{Default: "example-token"}, c, fakeFetcher{data: []byte("%PDF-1.4")}).Send(context.Background(), in)
		if res.Outcome != dispatch.OutcomeSuccess || res.ProviderMessageID != "" {
			t.Fatalf("outcome %v id %q (err %v), want success with an empty id", res.Outcome, res.ProviderMessageID, res.Err)
		}
	})
}

// TestCredentialFetchIsNotTheSend pins that only the SES operation's own HTTP
// request counts as written. The credentials come from a provider that POSTs
// to a local stub, as STS, IMDS or the ECS endpoint do; the SES endpoint then
// refuses the connection. Nothing reached SES, so the send is transient, on the
// plain path and on the attachment path.
func TestCredentialFetchIsNotTheSend(t *testing.T) {
	var fetches atomic.Int32
	creds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		fetches.Add(1)
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(creds.Close)
	provider := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, creds.URL, strings.NewReader("Action=AssumeRoleWithWebIdentity"))
		if err != nil {
			return aws.Credentials{}, err
		}
		resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}}).Do(req)
		if err != nil {
			return aws.Credentials{}, err
		}
		_ = resp.Body.Close()
		return aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "example-only", CanExpire: true, Expires: time.Now().Add(time.Hour)}, nil
	})
	for _, path := range []string{"plain", "attachment"} {
		t.Run(path, func(t *testing.T) {
			before := fetches.Load()
			cfg := aws.Config{Region: "us-east-1", BaseEndpoint: aws.String("http://" + closedAddr(t)), Credentials: aws.NewCredentialsCache(provider)}
			s := sender.NewEmailSender(sender.NewSESClient(cfg), fakeFetcher{data: []byte("%PDF-1.4")}, "noreply@example.com", "")
			in := emailIn()
			if path == "attachment" {
				in = emailWithAttachment()
			}
			res := s.Send(context.Background(), in)
			if fetches.Load()-before != 1 {
				t.Fatalf("the credentials stub was called %d times, want 1", fetches.Load()-before)
			}
			if res.Outcome != dispatch.OutcomeTransient {
				t.Fatalf("outcome %v (err %v), want transient", res.Outcome, res.Err)
			}
		})
	}
}

// TestSlackTokenFetchIsNotTheSend pins the same for Slack: a TokenProvider
// that makes its own HTTP call before a refused Slack endpoint leaves the send
// transient.
func TestSlackTokenFetchIsNotTheSend(t *testing.T) {
	var fetches atomic.Int32
	store := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		fetches.Add(1)
		_, _ = io.WriteString(w, "example-token")
	}))
	t.Cleanup(store.Close)
	c, err := slackclient.New(&http.Client{Transport: &http.Transport{Proxy: nil}}, "http://"+closedAddr(t)+"/api")
	if err != nil {
		t.Fatal(err)
	}
	res := sender.NewSlackSender(httpTokens{url: store.URL}, c, nil).Send(context.Background(), slackIn(""))
	if fetches.Load() != 1 || res.Outcome != dispatch.OutcomeTransient {
		t.Fatalf("token fetches %d, outcome %v (err %v); want 1, transient", fetches.Load(), res.Outcome, res.Err)
	}
}

// httpTokens is a TokenProvider that POSTs to a token store.
type httpTokens struct{ url string }

func (h httpTokens) Token(ctx context.Context, _ string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, strings.NewReader("org"))
	if err != nil {
		return "", err
	}
	resp, err := (&http.Client{Transport: &http.Transport{Proxy: nil}}).Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}
