// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package slackstub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func post(ctx context.Context, t *testing.T, url, token, body string) (*http.Response, error) {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/api/chat.postMessage", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return http.DefaultClient.Do(req)
}

// TestPostCommitsOnAnswer proves a post counts only once answered: a client that
// gives up during the delay leaves nothing, and FailNext answers 500.
func TestPostCommitsOnAnswer(t *testing.T) {
	s := &Stub{Delay: 300 * time.Millisecond}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := post(ctx, t, srv.URL, "tok", `{"channel":"C1","text":"x"}`); err == nil {
		t.Fatal("the client must time out")
	}
	time.Sleep(400 * time.Millisecond)
	if n := len(s.Messages()); n != 0 {
		t.Fatalf("an abandoned post was recorded (%d)", n)
	}
	s = &Stub{}
	srv = httptest.NewServer(s.Handler())
	defer srv.Close()
	s.FailNext(1)
	if resp, _ := post(context.Background(), t, srv.URL, "tok", `{"channel":"C1","text":"x"}`); resp.StatusCode != 500 {
		t.Fatalf("FailNext: status %d", resp.StatusCode)
	}
	if resp, _ := post(context.Background(), t, srv.URL, "tok", `{"channel":"C1","text":"x"}`); resp.StatusCode != 200 || len(s.Messages()) != 1 {
		t.Fatalf("the next post must succeed and be recorded")
	}
	if resp, _ := post(context.Background(), t, srv.URL, "", `{"channel":"C1"}`); resp.StatusCode != 200 || len(s.Messages()) != 1 {
		t.Fatal("an unauthenticated post must not be recorded")
	}
}

// TestFaults pins the control endpoint: a fault matches its method and channel,
// answers with its status, Retry-After, error code or missing id, stops after
// Times calls, and DELETE removes every fault.
func TestFaults(t *testing.T) {
	stub := &Stub{}
	srv := httptest.NewServer(stub.Handler())
	defer srv.Close()
	addFault := func(body string) {
		resp, err := http.Post(srv.URL+"/control/faults", "application/json", strings.NewReader(body))
		if err != nil || resp.StatusCode != http.StatusNoContent {
			t.Fatalf("add fault %s: %v %v", body, err, resp)
		}
		_ = resp.Body.Close()
	}
	post := func(channel string) (int, string, map[string]any) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/chat.postMessage", strings.NewReader(`{"channel":"`+channel+`","text":"hi"}`))
		req.Header.Set("Authorization", "Bearer t")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var body map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return resp.StatusCode, resp.Header.Get("Retry-After"), body
	}
	addFault(`{"method":"chat.postMessage","channel":"C0FAIL","status":503,"times":1}`)
	addFault(`{"method":"chat.postMessage","channel":"C0LIMIT","status":429,"retry_after":3}`)
	addFault(`{"method":"chat.postMessage","channel":"C0AUTH","error":"invalid_auth"}`)
	addFault(`{"method":"chat.postMessage","channel":"C0NOTS","no_id":true}`)
	if code, _, _ := post("C0FAIL"); code != 503 {
		t.Fatalf("first C0FAIL post: %d, want 503", code)
	}
	if code, _, body := post("C0FAIL"); code != 200 || body["ok"] != true {
		t.Fatalf("second C0FAIL post: %d %v, want ok (times 1)", code, body)
	}
	if code, ra, _ := post("C0LIMIT"); code != 429 || ra != "3" {
		t.Fatalf("C0LIMIT: %d Retry-After %q, want 429 3", code, ra)
	}
	if _, _, body := post("C0AUTH"); body["ok"] != false || body["error"] != "invalid_auth" {
		t.Fatalf("C0AUTH: %v, want ok:false invalid_auth", body)
	}
	if _, _, body := post("C0NOTS"); body["ok"] != true || body["ts"] != nil {
		t.Fatalf("C0NOTS: %v, want ok with no ts", body)
	}
	if _, _, body := post("C0OTHER"); body["ok"] != true || body["ts"] == nil {
		t.Fatalf("another channel: %v, want an ordinary post", body)
	}
	resp, err := http.Get(srv.URL + "/control/faults")
	if err != nil {
		t.Fatal(err)
	}
	var uses []FaultUse
	_ = json.NewDecoder(resp.Body).Decode(&uses)
	_ = resp.Body.Close()
	if len(uses) != 4 || uses[0].Channel != "C0FAIL" || uses[0].Used != 1 || uses[1].Used != 1 {
		t.Fatalf("fault listing %+v, want 4 faults, C0FAIL and C0LIMIT used once", uses)
	}
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/control/faults", nil)
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("clear faults: %v %v", err, resp)
	}
	if code, _, body := post("C0LIMIT"); code != 200 || body["ok"] != true {
		t.Fatalf("C0LIMIT after clearing: %d %v, want ok", code, body)
	}
}

// TestFaultDelayDropsOnDisconnect pins a delay fault: a client that gives up
// before the delay ends leaves no post behind.
func TestFaultDelayDropsOnDisconnect(t *testing.T) {
	stub := &Stub{}
	if err := stub.AddFault(Fault{Method: "chat.postMessage", Delay: "2s"}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(stub.Handler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/api/chat.postMessage", strings.NewReader(`{"channel":"C0SLOW","text":"hi"}`))
	req.Header.Set("Authorization", "Bearer t")
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("want the client to give up")
	}
	time.Sleep(2500 * time.Millisecond)
	if n := len(stub.Messages()); n != 0 {
		t.Fatalf("%d posts recorded, want 0", n)
	}
}

// TestFileStepFaults pins a fault on each step of the file upload: the URL
// request, the byte upload and the completion (per channel), each answering
// its failure once and then succeeding.
func TestFileStepFaults(t *testing.T) {
	stub := &Stub{}
	srv := httptest.NewServer(stub.Handler())
	defer srv.Close()
	for _, f := range []Fault{
		{Method: "files.getUploadURLExternal", Status: 500, Times: 1},
		{Method: "upload", Status: 503, Times: 1},
		{Method: "files.completeUploadExternal", Channel: "C0FILE", Error: "channel_not_found", Times: 1},
	} {
		if err := stub.AddFault(f); err != nil {
			t.Fatal(err)
		}
	}
	call := func(path, body string) (int, map[string]any) {
		resp, err := http.Post(srv.URL+path, "application/x-www-form-urlencoded", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	complete := "channel_id=C0FILE&files=" + `[{"id":"F2"}]`
	if code, _ := call("/api/files.getUploadURLExternal", "filename=r.csv&length=3"); code != 500 {
		t.Fatalf("first URL request: %d, want 500", code)
	}
	if _, m := call("/api/files.getUploadURLExternal", "filename=r.csv&length=3"); m["file_id"] == nil {
		t.Fatalf("second URL request: %v, want a file id", m)
	}
	if code, _ := call("/upload/F2", "abc"); code != 503 {
		t.Fatalf("first upload: %d, want 503", code)
	}
	if code, _ := call("/upload/F2", "abc"); code != 200 {
		t.Fatalf("second upload: %d, want 200", code)
	}
	if _, m := call("/api/files.completeUploadExternal", complete); m["ok"] != false || m["error"] != "channel_not_found" {
		t.Fatalf("first completion: %v, want channel_not_found", m)
	}
	if n := len(stub.Messages()); n != 0 {
		t.Fatalf("%d message(s) after a failed completion, want 0", n)
	}
	if _, m := call("/api/files.completeUploadExternal", complete); m["ok"] != true {
		t.Fatalf("second completion: %v, want ok", m)
	}
	if ms := stub.Messages(); len(ms) != 1 || ms[0].Channel != "C0FILE" || ms[0].TS != "F2" {
		t.Fatalf("messages %+v, want one file post on C0FILE", ms)
	}
}
