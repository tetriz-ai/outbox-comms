// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package metrics

import (
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestImportRegistersNothing pins that importing the package leaves the default
// registry without any of its metrics, so a program that imports the server
// gets no metrics it did not ask for. Nothing in this package's tests registers
// on the default registry.
func TestImportRegistersNothing(t *testing.T) {
	for _, c := range all() {
		if prometheus.DefaultRegisterer.Unregister(c) {
			t.Fatalf("%v was on the default registry at import", c)
		}
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "outbox_comms_") {
			t.Fatalf("%s is on the default registry", f.GetName())
		}
	}
}

// TestRegister pins Register: every metric lands on the registry given, a
// second call is a no-op, and a different metric already holding a name is an
// error.
func TestRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	if err := Register(reg); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	for _, c := range all() {
		err := reg.Register(c)
		var dup prometheus.AlreadyRegisteredError
		if !errors.As(err, &dup) {
			t.Fatalf("%v is not on the registry after Register (%v)", c, err)
		}
	}
	clash := prometheus.NewRegistry()
	if err := clash.Register(prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_comms_bounce_rate", Help: "another"})); err != nil {
		t.Fatal(err)
	}
	if err := Register(clash); err == nil {
		t.Fatal("Register over a different metric of the same name returned nil")
	}
}
