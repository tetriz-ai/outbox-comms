// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package cli_test

import (
	"os"
	"strings"
	"testing"
)

// TestReleaseVersionsMatch pins that the binaries and the image are stamped
// from the same value, the git tag: goreleaser's {{.Tag}} for the archives,
// and the release workflow's tag name as the Dockerfile's VERSION. goreleaser's
// {{.Version}} drops the leading v, so the two would print 0.1.0 and v0.1.0.
func TestReleaseVersionsMatch(t *testing.T) {
	for _, tc := range []struct{ file, want string }{
		{"../.goreleaser.yaml", "cli.Version={{.Tag}}"},
		{"../.github/workflows/release.yml", "build-args: VERSION=${{ github.ref_name }}"},
		{"../.github/workflows/release.yml", `tags: ["v*"]`},
		{"../Dockerfile", "cli.Version=${VERSION}"},
		{"../compose.yaml", "VERSION: v0.1.0"},
	} {
		b, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), tc.want) {
			t.Errorf("%s lacks %q", tc.file, tc.want)
		}
	}
}
