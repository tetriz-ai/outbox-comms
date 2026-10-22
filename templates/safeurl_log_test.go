// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package templates

import (
	"bytes"
	"log/slog"
	"testing"
)

// TestDroppedLinksAreNotLogged pins that safeURL drops an unsafe or unparseable
// link without writing to the process's default logger. Not parallel: it swaps
// that logger.
func TestDroppedLinksAreNotLogged(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	for _, bad := range []string{"javascript:alert(1)", "https://example.com/%zz?token=secret"} {
		if _, ok := safeURL(bad); ok {
			t.Fatalf("safeURL(%q) kept the link", bad)
		}
	}
	if buf.Len() != 0 {
		t.Fatalf("dropping links logged: %s", buf.String())
	}
}
