// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestComposePortsBindToLocalhost pins that every port any compose file
// publishes binds to 127.0.0.1: the gRPC API has no authentication, so a port
// on all interfaces would expose it. It reads each ports list item; an inline
// list or a long-form entry fails, so a new syntax cannot slip past the check.
func TestComposePortsBindToLocalhost(t *testing.T) {
	files, err := filepath.Glob("../compose*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no compose files: %v", err)
	}
	seen := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		inPorts, portsIndent := false, 0
		for i, line := range strings.Split(string(b), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			indent := len(line) - len(strings.TrimLeft(line, " "))
			if inPorts && indent <= portsIndent {
				inPorts = false
			}
			if inPorts {
				item, ok := strings.CutPrefix(trimmed, "- ")
				if !ok || !strings.HasPrefix(strings.Trim(item, `"'`), "127.0.0.1:") {
					t.Errorf("%s:%d: port %q does not bind to 127.0.0.1", f, i+1, trimmed)
				}
				seen++
				continue
			}
			if key, rest, ok := strings.Cut(trimmed, ":"); ok && key == "ports" {
				if strings.TrimSpace(rest) != "" {
					t.Errorf("%s:%d: inline ports list; write one item per line", f, i+1)
				}
				inPorts, portsIndent = true, indent
			}
		}
	}
	if seen == 0 {
		t.Fatal("no published port found; the check read nothing")
	}
}
