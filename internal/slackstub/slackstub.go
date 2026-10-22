// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package slackstub is a local stand-in for the parts of the Slack Web API the
// service calls: chat.postMessage, conversations.join and the external file
// upload. It keeps what it receives in memory, lists it at GET /messages, and
// forgets it at DELETE /messages, which scripts/run-examples.sh calls before
// each run.
//
// A post counts as received only when the stub answers it: with a Delay set, a
// client that disconnects before the delay ends (a worker killed mid-send) leaves
// nothing behind, the way a request that never completed leaves nothing in a real
// workspace. FailNext makes the next N posts answer HTTP 500.
//
// The control endpoint scripts failures for tests, and is for local use only:
//
//	POST   /control/faults   add a Fault (JSON body)
//	GET    /control/faults   the faults, each with how many calls it answered
//	DELETE /control/faults   remove every fault
//
// A Fault applies to one method (chat.postMessage, files.getUploadURLExternal,
// upload for the byte upload, or files.completeUploadExternal) and, for
// chat.postMessage and files.completeUploadExternal, to one channel or, empty, to
// any. It answers with an HTTP status (with Retry-After for a 429), an ok:false
// error code, a delay that drops the call if the client disconnects, or ok:true
// with no ts or file id. Times limits how many calls it applies to; zero means
// every call until the faults are removed. The first matching fault wins.
package slackstub

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message is one post the stub accepted.
type Message struct {
	Channel string          `json:"channel"`
	Text    string          `json:"text"`
	Blocks  json.RawMessage `json:"blocks,omitempty"`
	TS      string          `json:"ts"`
	Token   string          `json:"token"`
}

// Fault is one scripted failure; see the package doc.
type Fault struct {
	Method     string `json:"method"`
	Channel    string `json:"channel,omitempty"`
	Status     int    `json:"status,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"`
	Error      string `json:"error,omitempty"`
	Delay      string `json:"delay,omitempty"`
	NoID       bool   `json:"no_id,omitempty"`
	Times      int    `json:"times,omitempty"`

	delay time.Duration
	used  int
}

// Stub is the fake Slack API. The zero value is ready to use.
type Stub struct {
	// Delay holds each chat.postMessage this long before answering.
	Delay time.Duration

	mu       sync.Mutex
	messages []Message
	files    []string
	failNext int
	seq      int
	faults   []*Fault
}

// AddFault scripts a failure.
func (s *Stub) AddFault(f Fault) error {
	if f.Delay != "" {
		d, err := time.ParseDuration(f.Delay)
		if err != nil {
			return err
		}
		f.delay = d
	}
	s.mu.Lock()
	s.faults = append(s.faults, &f)
	s.mu.Unlock()
	return nil
}

// ClearFaults removes every scripted failure.
func (s *Stub) ClearFaults() {
	s.mu.Lock()
	s.faults = nil
	s.mu.Unlock()
}

// FaultUse is a scripted fault and how many calls it has answered.
type FaultUse struct {
	Fault
	Used int `json:"used"`
}

// Faults returns the scripted faults with their use counts, so a test can
// count the calls a failure answered, which leave no message behind.
func (s *Stub) Faults() []FaultUse {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]FaultUse, 0, len(s.faults))
	for _, f := range s.faults {
		out = append(out, FaultUse{Fault: *f, Used: f.used})
	}
	return out
}

// fault returns the first fault that matches a call, and counts its use.
func (s *Stub) fault(method, channel string) *Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.faults {
		if f.Method != method || (f.Channel != "" && f.Channel != channel) || (f.Times > 0 && f.used >= f.Times) {
			continue
		}
		f.used++
		c := *f
		return &c
	}
	return nil
}

// apply runs a fault's delay and failure. It reports whether the call is over:
// answered with a failure, or dropped because the client went away. A NoID
// fault is not over; the handler answers ok without an id.
func apply(f *Fault, w http.ResponseWriter, r *http.Request) bool {
	if f == nil {
		return false
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-r.Context().Done():
			return true
		}
	}
	switch {
	case f.Status == http.StatusTooManyRequests:
		if f.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(f.RetryAfter))
		}
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	case f.Status != 0:
		http.Error(w, "stub fault", f.Status)
		return true
	case f.Error != "":
		writeJSON(w, map[string]any{"ok": false, "error": f.Error})
		return true
	}
	return false
}

// FailNext makes the next n chat.postMessage calls answer HTTP 500.
func (s *Stub) FailNext(n int) {
	s.mu.Lock()
	s.failNext = n
	s.mu.Unlock()
}

// Messages returns the posts accepted so far.
func (s *Stub) Messages() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.messages...)
}

// Reset forgets everything received. Scripted faults stay until ClearFaults.
func (s *Stub) Reset() {
	s.mu.Lock()
	s.messages, s.files, s.failNext = nil, nil, 0
	s.mu.Unlock()
}

// Handler serves the API under /api and the stub's own endpoints:
// GET /messages (the accepted posts as JSON), DELETE /messages (reset),
// GET /healthz.
func (s *Stub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/chat.postMessage", s.postMessage)
	mux.HandleFunc("/api/conversations.join", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, map[string]any{"ok": true}) })
	mux.HandleFunc("/api/files.getUploadURLExternal", s.getUploadURL)
	mux.HandleFunc("/upload/", s.upload)
	mux.HandleFunc("/api/files.completeUploadExternal", s.completeUpload)
	mux.HandleFunc("/control/faults", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var f Fault
			if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := s.AddFault(f); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			writeJSON(w, s.Faults())
		case http.MethodDelete:
			s.ClearFaults()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "use GET, POST or DELETE", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			s.Reset()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, s.Messages())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (s *Stub) postMessage(w http.ResponseWriter, r *http.Request) {
	var m Message
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		writeJSON(w, map[string]any{"ok": false, "error": "invalid_json"})
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" {
		writeJSON(w, map[string]any{"ok": false, "error": "not_authed"})
		return
	}
	if token == "revoked" {
		writeJSON(w, map[string]any{"ok": false, "error": "token_revoked"})
		return
	}
	if s.Delay > 0 {
		select {
		case <-time.After(s.Delay):
		case <-r.Context().Done():
			return // the client went away: nothing was posted
		}
	}
	f := s.fault("chat.postMessage", m.Channel)
	if apply(f, w, r) {
		return
	}
	s.mu.Lock()
	if s.failNext > 0 {
		s.failNext--
		s.mu.Unlock()
		http.Error(w, "stub failure", http.StatusInternalServerError)
		return
	}
	if m.Channel == "" {
		s.mu.Unlock()
		writeJSON(w, map[string]any{"ok": false, "error": "channel_not_found"})
		return
	}
	s.seq++
	m.TS = strconv.Itoa(1700000000+s.seq) + ".000100"
	m.Token = token
	s.messages = append(s.messages, m)
	s.mu.Unlock()
	if f != nil && f.NoID {
		writeJSON(w, map[string]any{"ok": true, "channel": m.Channel})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "channel": m.Channel, "ts": m.TS})
}

func (s *Stub) getUploadURL(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f := s.fault("files.getUploadURLExternal", "")
	if apply(f, w, r) {
		return
	}
	s.mu.Lock()
	s.seq++
	id := "F" + strconv.Itoa(s.seq)
	s.mu.Unlock()
	if f != nil && f.NoID {
		writeJSON(w, map[string]any{"ok": true, "upload_url": "http://" + r.Host + "/upload/" + id})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "file_id": id, "upload_url": "http://" + r.Host + "/upload/" + id})
}

func (s *Stub) upload(w http.ResponseWriter, r *http.Request) {
	if apply(s.fault("upload", ""), w, r) {
		return
	}
	s.mu.Lock()
	s.files = append(s.files, strings.TrimPrefix(r.URL.Path, "/upload/"))
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// completeUpload shares an uploaded file to its channel and records it as a
// message on that channel, with the file id as its ts.
func (s *Stub) completeUpload(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	channel := r.Form.Get("channel_id")
	var files []struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(r.Form.Get("files")), &files)
	f := s.fault("files.completeUploadExternal", channel)
	if apply(f, w, r) {
		return
	}
	id := ""
	if len(files) > 0 {
		id = files[0].ID
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	s.messages = append(s.messages, Message{Channel: channel, Text: "file " + id, TS: id, Token: token})
	s.mu.Unlock()
	if f != nil && f.NoID {
		writeJSON(w, map[string]any{"ok": true})
		return
	}
	writeJSON(w, map[string]any{"ok": true, "files": []map[string]string{{"id": id}}})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
