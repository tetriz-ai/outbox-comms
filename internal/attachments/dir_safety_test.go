// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package attachments

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// orgDirs makes a store root with both organizations' folders.
func orgDirs(t *testing.T) (Dir, string, string, string) {
	t.Helper()
	root := t.TempDir()
	a, b := filepath.Join(root, orgA), filepath.Join(root, orgB)
	for _, d := range []string{a, b} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	return Dir{Root: root}, root, a, b
}

// TestPutStaysInTheOrgFolder pins that a write never follows a symlink out of
// the organization's folder: not a linked directory, not a linked file, and
// not a dangling link that would create a file elsewhere.
func TestPutStaysInTheOrgFolder(t *testing.T) {
	ctx := context.Background()
	d, _, a, b := orgDirs(t)
	if err := os.WriteFile(filepath.Join(b, "b.pdf"), []byte("B"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(b, filepath.Join(a, "sib")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(b, "b.pdf"), filepath.Join(a, "theirs.pdf")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(b, "b2.pdf"), filepath.Join(a, "new.pdf")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"sib/b.pdf", "sib/c.pdf", "theirs.pdf", "new.pdf"} {
		if err := d.Put(ctx, orgA, key, []byte("A")); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Put(%q): %v, want ErrInvalidKey", key, err)
		}
	}
	if got, _ := os.ReadFile(filepath.Join(b, "b.pdf")); string(got) != "B" {
		t.Errorf("the sibling's b.pdf now reads %q", got)
	}
	for _, name := range []string{"b2.pdf", "c.pdf"} {
		if _, err := os.Stat(filepath.Join(b, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("a write landed in the sibling's folder as %s: %v", name, err)
		}
	}
}

// TestGetLinkShapes pins the read side: an absolute symlink that points inside
// the organization's folder is followed, and a loop, a path through a file
// and an over-long name are refused as invalid keys, not retried.
func TestGetLinkShapes(t *testing.T) {
	ctx := context.Background()
	d, _, a, _ := orgDirs(t)
	if err := os.WriteFile(filepath.Join(a, "real.pdf"), []byte("%PDF"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(a, "real.pdf"), filepath.Join(a, "abs.pdf")); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get(ctx, orgA, "abs.pdf"); err != nil || string(got) != "%PDF" {
		t.Errorf("an absolute symlink inside the folder: %q, %v", got, err)
	}
	if err := os.Symlink("l2", filepath.Join(a, "l1")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("l1", filepath.Join(a, "l2")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"l1", "real.pdf/x", strings.Repeat("a", 300)} {
		if _, err := d.Get(ctx, orgA, key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Get(%.20q): %v, want ErrInvalidKey", key, err)
		}
	}
}

// TestGetRefusesALinkSwappedAfterTheCheck pins the second guard: a link that
// is swapped to another organization's file between the check and the read is
// still refused.
func TestGetRefusesALinkSwappedAfterTheCheck(t *testing.T) {
	ctx := context.Background()
	d, _, a, b := orgDirs(t)
	if err := os.WriteFile(filepath.Join(a, "mine.pdf"), []byte("A"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "b.pdf"), []byte("B"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(a, "d"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a, "d", "f.pdf"), []byte("A"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeOpen = func() {
		_ = os.RemoveAll(filepath.Join(a, "d"))
		_ = os.Symlink(b, filepath.Join(a, "d"))
		_ = os.Rename(filepath.Join(b, "b.pdf"), filepath.Join(b, "f.pdf"))
	}
	t.Cleanup(func() { beforeOpen = func() {} })
	if got, err := d.Get(ctx, orgA, "d/f.pdf"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("a link swapped to another organization: %q, %v; want ErrInvalidKey", got, err)
	}
}

// TestOrgFolderMustBeADirectory pins that an organization's folder that is
// itself a symlink, here to another organization's folder, is refused for
// reads and writes alike.
func TestOrgFolderMustBeADirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b := filepath.Join(root, orgB)
	if err := os.MkdirAll(b, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(b, "b.pdf"), []byte("B"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(b, filepath.Join(root, orgA)); err != nil {
		t.Fatal(err)
	}
	d := Dir{Root: root}
	if got, err := d.Get(ctx, orgA, "b.pdf"); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("Get through a linked folder: %q, %v; want ErrInvalidKey", got, err)
	}
	if err := d.Put(ctx, orgA, "a.pdf", []byte("A")); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("Put through a linked folder: %v, want ErrInvalidKey", err)
	}
	if _, err := os.Stat(filepath.Join(b, "a.pdf")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a write landed in the sibling's folder: %v", err)
	}
}
