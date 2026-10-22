// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package fakeses is a local stand-in for the two Amazon SES v1 calls the email
// sender makes, SendEmail and SendRawEmail, for tests and the e2e run. It
// answers the SES Query API with a MessageId and records each call. Nothing
// leaves the machine, and it checks no signature.
//
// Its own endpoints, for local use only:
//
//	GET    /calls            the calls received, as JSON
//	DELETE /calls            forget the calls and remove every fault
//	POST   /control/faults   add a Fault (JSON body)
//	DELETE /control/faults   remove every fault
//	GET    /healthz          liveness
//
// A Fault applies to one action (SendEmail or SendRawEmail) or, empty, to both.
// It answers with an HTTP status and SES error code, a 200 with an empty
// MessageId, or a delay before answering (the call is still recorded, as SES
// would have taken it; a client that disconnects ends the delay). Times limits
// how many calls it applies to; zero means every call. The first matching fault
// wins. Each recorded call says whether another was in flight when it arrived.
package fakeses

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/mail"
	"strings"
	"sync"
	"time"
)

// Call is one SendEmail or SendRawEmail the fake received.
type Call struct {
	Action    string `json:"action"`
	To        string `json:"to"`
	MessageID string `json:"message_id"`
	// Overlapped is true when another call was still in flight as this one
	// arrived: two sends of the same message at once show up here.
	Overlapped bool `json:"overlapped,omitempty"`
}

// Fault is one scripted failure; see the package doc.
type Fault struct {
	Action         string `json:"action,omitempty"`
	Status         int    `json:"status,omitempty"`
	Code           string `json:"code,omitempty"`
	EmptyMessageID bool   `json:"empty_message_id,omitempty"`
	Delay          string `json:"delay,omitempty"`
	Times          int    `json:"times,omitempty"`

	delay time.Duration
	used  int
}

// SES is the fake. The zero value is ready to use.
type SES struct {
	mu     sync.Mutex
	calls  []Call
	faults []*Fault
	seq    int
	// inFlight counts calls received and not yet answered or dropped.
	inFlight int
}

// AddFault scripts a failure.
func (s *SES) AddFault(f Fault) error {
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

// Calls returns the calls received so far.
func (s *SES) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// Reset forgets the calls and removes every fault.
func (s *SES) Reset() {
	s.mu.Lock()
	s.calls, s.faults = nil, nil
	s.mu.Unlock()
}

// Handler serves the SES Query API at / and the fake's own endpoints.
func (s *SES) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.query)
	mux.HandleFunc("/calls", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			s.Reset()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Calls())
	})
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
		case http.MethodDelete:
			s.mu.Lock()
			s.faults = nil
			s.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "use POST or DELETE", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (s *SES) query(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	action := r.Form.Get("Action")
	if action != "SendEmail" && action != "SendRawEmail" {
		writeError(w, http.StatusBadRequest, "InvalidAction", "the fake answers SendEmail and SendRawEmail only")
		return
	}
	to := r.Form.Get("Destination.ToAddresses.member.1")
	if action == "SendRawEmail" {
		to = rawRecipient(r.Form.Get("RawMessage.Data"))
	}
	s.mu.Lock()
	overlapped := s.inFlight > 0
	s.inFlight++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight--
		s.mu.Unlock()
	}()
	f := s.fault(action)
	if f != nil && f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-r.Context().Done():
		}
	}
	s.mu.Lock()
	s.seq++
	id := fmt.Sprintf("0100-fake-%06d", s.seq)
	if f != nil && (f.Status != 0 || f.EmptyMessageID) {
		id = ""
	}
	s.calls = append(s.calls, Call{Action: action, To: to, MessageID: id, Overlapped: overlapped})
	s.mu.Unlock()
	if f != nil && f.Status != 0 {
		writeError(w, f.Status, f.Code, "fake failure")
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	_, _ = fmt.Fprintf(w, `<%[1]sResponse xmlns="http://ses.amazonaws.com/doc/2010-12-01/"><%[1]sResult><MessageId>%[2]s</MessageId></%[1]sResult><ResponseMetadata><RequestId>fake</RequestId></ResponseMetadata></%[1]sResponse>`, action, id)
}

// fault returns the first fault that matches an action, and counts its use.
func (s *SES) fault(action string) *Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.faults {
		if (f.Action != "" && f.Action != action) || (f.Times > 0 && f.used >= f.Times) {
			continue
		}
		f.used++
		c := *f
		return &c
	}
	return nil
}

// rawRecipient reads the To header of a base64 raw MIME message.
func rawRecipient(data string) string {
	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return ""
	}
	m, err := mail.ReadMessage(strings.NewReader(string(b)))
	if err != nil {
		return ""
	}
	return m.Header.Get("To")
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	if code == "" {
		code = "ServiceUnavailable"
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<ErrorResponse><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>fake</RequestId></ErrorResponse>`, code, msg)
}
