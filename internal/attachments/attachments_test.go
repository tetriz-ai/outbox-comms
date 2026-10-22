// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

package attachments

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	orgA = "8c4f6f0e-6a7b-4c1d-9e2f-3a4b5c6d7e8f"
	orgB = "2b0c7a3e-1f4d-4e5a-8b9c-0d1e2f3a4b5c"
)

// TestObjectPath pins the key shapes either side of the boundary: a clean
// relative path is placed under the canonical lowercase organization id, and
// anything that could leave that folder or be read two ways is refused.
func TestObjectPath(t *testing.T) {
	for _, tc := range []struct{ org, key, want string }{
		{orgA, "r.pdf", orgA + "/r.pdf"},
		{orgA, "reports/2026-10.pdf", orgA + "/reports/2026-10.pdf"},
		{strings.ToUpper(orgA), "r.pdf", orgA + "/r.pdf"},
		{orgA, "a..b.pdf", orgA + "/a..b.pdf"},
		{orgA, "...", orgA + "/..."},
		{orgA, "%2e%2e%2fx", orgA + "/%2e%2e%2fx"},
		{orgA, "é.pdf", orgA + "/é.pdf"},
		{orgA, "\xff.pdf", orgA + "/\xff.pdf"},
		{orgA, orgB + "/r.pdf", orgA + "/" + orgB + "/r.pdf"},
		{"{" + orgA + "}", "r.pdf", orgA + "/r.pdf"},
		{"urn:uuid:" + orgA, "r.pdf", orgA + "/r.pdf"},
		{strings.ReplaceAll(orgA, "-", ""), "r.pdf", orgA + "/r.pdf"},
	} {
		if got, err := objectPath(tc.org, tc.key); err != nil || got != tc.want {
			t.Errorf("objectPath(%q, %q) = %q, %v; want %q", tc.org, tc.key, got, err, tc.want)
		}
	}
	for _, tc := range []struct{ org, key string }{
		{orgA, ""}, {orgA, "."}, {orgA, "/r.pdf"}, {orgA, ".."}, {orgA, "../x"}, {orgA, "a/../../x"},
		{orgA, "a/../b"}, {orgA, "./r.pdf"}, {orgA, "a//b"}, {orgA, "a/"}, {orgA, `..\x`},
		{orgA, "a\x00b"}, {"not-a-uuid", "r.pdf"}, {"", "r.pdf"},
	} {
		if _, err := objectPath(tc.org, tc.key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("objectPath(%q, %q) must be ErrInvalidKey, got %v", tc.org, tc.key, err)
		}
	}
}

func TestDirRoundTrip(t *testing.T) {
	root := t.TempDir()
	d := Dir{Root: root}
	ctx := context.Background()
	if err := d.Put(ctx, orgA, "1/report.pdf", []byte("%PDF")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, orgA, "1", "report.pdf")); err != nil {
		t.Fatalf("the object is not under the organization's folder: %v", err)
	}
	got, err := d.Get(ctx, orgA, "1/report.pdf")
	if err != nil || string(got) != "%PDF" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if _, err := d.Get(ctx, orgB, "1/report.pdf"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("another organization's Get: %v, want fs.ErrNotExist", err)
	}
	if err := d.Put(ctx, orgA, "../escape", []byte("x")); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("an escaping key must be refused, got %v", err)
	}
	// A symlink that stays inside the organization's folder is followed.
	if err := os.Symlink("report.pdf", filepath.Join(root, orgA, "1", "latest.pdf")); err != nil {
		t.Fatal(err)
	}
	if got, err := d.Get(ctx, orgA, "1/latest.pdf"); err != nil || string(got) != "%PDF" {
		t.Fatalf("a symlink inside the folder: %q, %v", got, err)
	}
	// One that reaches another organization's folder is not.
	if err := d.Put(ctx, orgB, "b.pdf", []byte("B")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, orgB, "b.pdf"), filepath.Join(root, orgA, "1", "theirs.pdf")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Get(ctx, orgA, "1/theirs.pdf"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("a symlink into another organization: %v, want ErrInvalidKey", err)
	}
	if _, err := d.Get(ctx, orgA, "1"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("a directory: %v, want ErrInvalidKey", err)
	}
}

type fakeS3 struct{ objects map[string][]byte }

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, _ := io.ReadAll(in.Body)
	f.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)] = b
	return &s3.PutObjectOutput{}, nil
}

// TestS3Prefix proves objects land at <prefix>/<org>/<key>, and that one
// organization's key does not reach another's object.
func TestS3Prefix(t *testing.T) {
	f := &fakeS3{objects: map[string][]byte{}}
	s := S3{Client: f, Bucket: "b", Prefix: "/attach/"}
	ctx := context.Background()
	if err := s.Put(ctx, orgA, "r.pdf", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objects["b/attach/"+orgA+"/r.pdf"]; !ok {
		t.Fatalf("objects = %v", f.objects)
	}
	if got, err := s.Get(ctx, orgA, "r.pdf"); err != nil || string(got) != "x" {
		t.Fatalf("Get: %q %v", got, err)
	}
	if _, err := s.Get(ctx, orgB, "r.pdf"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("another organization's Get: %v, want fs.ErrNotExist", err)
	}
	if err := s.Put(ctx, orgA, "attach/r.pdf", []byte("y")); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.objects["b/attach/"+orgA+"/attach/r.pdf"]; !ok {
		t.Fatalf("a key that repeats the prefix is not under the organization: %v", f.objects)
	}
	if _, err := s.Get(ctx, orgB, "../"+orgA+"/r.pdf"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("a key climbing to another organization: %v, want ErrInvalidKey", err)
	}
}

// TestMissingAndInvalidKeys pins the two errors a caller can act on: a key with
// no object matches fs.ErrNotExist in both stores, and an invalid key matches
// fs.ErrInvalid.
func TestMissingAndInvalidKeys(t *testing.T) {
	ctx := context.Background()
	for name, st := range map[string]Store{
		"dir": Dir{Root: t.TempDir()},
		"s3":  S3{Client: &fakeS3{objects: map[string][]byte{}}, Bucket: "b"},
	} {
		if _, err := st.Get(ctx, orgA, "missing.pdf"); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: missing key: %v, want fs.ErrNotExist", name, err)
		}
		if _, err := st.Get(ctx, orgA, "../up"); !errors.Is(err, fs.ErrInvalid) || !errors.Is(err, ErrInvalidKey) {
			t.Errorf("%s: invalid key: %v, want fs.ErrInvalid and ErrInvalidKey", name, err)
		}
	}
}
