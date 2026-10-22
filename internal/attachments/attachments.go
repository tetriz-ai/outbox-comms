// Copyright 2026 Workloom Solutions Private Limited
// SPDX-License-Identifier: MIT

// Package attachments stores the files a producer attaches by key: a directory on
// local disk by default, or an S3 bucket. The producer writes the object under
// its organization, passes the key as the attachment_key param, and the senders
// read it back at dispatch from that same organization only.
//
// Every object lives at <organization_id>/<key>, the organization id in its
// canonical lowercase form, so one organization's key can never name another
// organization's file.
package attachments

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
)

// Store reads and writes attachment objects by organization and key.
type Store interface {
	Get(ctx context.Context, orgID, key string) ([]byte, error)
	Put(ctx context.Context, orgID, key string, data []byte) error
}

// ErrInvalidKey is returned for a key the store refuses: empty, absolute, with a
// ".." segment, a backslash or a NUL byte, naming a directory, or leaving the
// organization's folder through a symlink. It matches fs.ErrInvalid. A key with
// no object behind it matches fs.ErrNotExist, and an unreadable file
// fs.ErrPermission, so a caller can tell a bad key from a store outage.
var ErrInvalidKey = fmt.Errorf("attachments: invalid key: %w", fs.ErrInvalid)

// objectPath returns <org>/<key> for a valid organization id and key. The key
// must already be a clean relative path: it is refused, not rewritten, so the
// name the producer sent is the name the store reads.
func objectPath(orgID, key string) (string, error) {
	id, err := uuid.Parse(orgID)
	if err != nil {
		return "", fmt.Errorf("%w: organization id %q", ErrInvalidKey, orgID)
	}
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\x00") || path.Clean(key) != key {
		return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == ".." || seg == "." {
			return "", fmt.Errorf("%w: %q", ErrInvalidKey, key)
		}
	}
	return id.String() + "/" + key, nil
}

// Dir is a Store over a local directory, one folder per organization.
type Dir struct {
	Root string
}

// Get reads the object at <org>/<key> under Root. Symlinks are followed only
// while they stay inside the organization's folder, and an absolute one that
// points inside it is followed too.
func (d Dir) Get(_ context.Context, orgID, key string) ([]byte, error) {
	root, rel, err := d.open(orgID, key, false)
	if err != nil {
		return nil, fmt.Errorf("read attachment %q: %w", key, err)
	}
	defer func() { _ = root.Close() }()
	target, err := inside(root.Name(), filepath.Join(root.Name(), rel))
	if err != nil {
		return nil, fmt.Errorf("read attachment %q: %w", key, err)
	}
	beforeOpen()
	// The read goes through os.Root by the resolved relative path, so a link
	// swapped in after the check above still cannot leave the folder.
	b, err := root.ReadFile(target)
	if err != nil {
		return nil, fmt.Errorf("read attachment %q: %w", key, classify(err))
	}
	return b, nil
}

// beforeOpen runs between the containment check and the read. Tests replace it
// to change the tree in that window.
var beforeOpen = func() {}

// open returns the organization's folder as an os.Root and the key as a path
// relative to it. The folder must be a real directory, not a symlink, so it
// cannot stand for another organization's. With create, a missing folder is
// made.
func (d Dir) open(orgID, key string, create bool) (*os.Root, string, error) {
	p, err := objectPath(orgID, key)
	if err != nil {
		return nil, "", err
	}
	org, rel, _ := strings.Cut(p, "/")
	orgDir := filepath.Join(d.Root, org)
	if create {
		if err := os.MkdirAll(orgDir, 0o750); err != nil {
			return nil, "", fmt.Errorf("create attachment dir: %w", err)
		}
	}
	if info, err := os.Lstat(orgDir); err != nil {
		return nil, "", classify(err)
	} else if !info.IsDir() {
		return nil, "", fmt.Errorf("%w: the organization's folder is not a directory", ErrInvalidKey)
	}
	root, err := os.OpenRoot(orgDir)
	if err != nil {
		return nil, "", classify(err)
	}
	return root, filepath.FromSlash(rel), nil
}

// inside resolves file's symlinks and returns its path relative to orgDir. A
// result outside orgDir, or a directory, is refused. A file that does not
// exist is reported as fs.ErrNotExist.
func inside(orgDir, file string) (string, error) {
	base, err := filepath.EvalSymlinks(orgDir)
	if err != nil {
		return "", classify(err)
	}
	target, err := filepath.EvalSymlinks(file)
	if err != nil {
		return "", classify(err)
	}
	rel, err := filepath.Rel(base, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: it leaves the organization's folder", ErrInvalidKey)
	}
	info, err := os.Stat(target)
	if err != nil {
		return "", classify(err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%w: it names a directory", ErrInvalidKey)
	}
	return rel, nil
}

// classify marks the file-system errors no retry can fix as ErrInvalidKey: a
// path through a file, a name too long, a symlink loop, and an os.Root escape.
// The last two have no exported error value in the standard library, so they
// are matched by their text.
func classify(err error) error {
	switch {
	case errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.ENAMETOOLONG),
		errors.Is(err, syscall.ELOOP), errors.Is(err, syscall.EISDIR):
		return fmt.Errorf("%w: %w", ErrInvalidKey, err)
	case strings.Contains(err.Error(), "too many links"), strings.Contains(err.Error(), "path escapes from parent"):
		return fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	return err
}

// Put writes the object at <org>/<key> under Root, creating parent
// directories. Every step goes through os.Root, so no symlink, whether a
// directory or the file itself, can carry the write out of the organization's
// folder.
func (d Dir) Put(_ context.Context, orgID, key string, data []byte) error {
	root, rel, err := d.open(orgID, key, true)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	if dir := filepath.Dir(rel); dir != "." {
		if err := root.MkdirAll(dir, 0o750); err != nil {
			// An element that exists but is not a directory inside the
			// folder, such as a symlink, makes os.Root report it as existing.
			if errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("%w: %w", ErrInvalidKey, err)
			}
			return fmt.Errorf("create attachment dir: %w", classify(err))
		}
	}
	if err := root.WriteFile(rel, data, 0o640); err != nil {
		return fmt.Errorf("write attachment %q: %w", key, classify(err))
	}
	return nil
}

// S3API is the subset of the S3 client the store calls; *s3.Client satisfies it.
type S3API interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3 is a Store over one bucket, with every object at <Prefix>/<org>/<key>.
type S3 struct {
	Client S3API
	Bucket string
	Prefix string
}

// resolve returns the object key for an organization's key.
func (s S3) resolve(orgID, key string) (string, error) {
	p, err := objectPath(orgID, key)
	if err != nil {
		return "", err
	}
	if prefix := strings.Trim(s.Prefix, "/"); prefix != "" {
		return prefix + "/" + p, nil
	}
	return p, nil
}

// Get reads the organization's object at key.
func (s S3) Get(ctx context.Context, orgID, key string) ([]byte, error) {
	k, err := s.resolve(orgID, key)
	if err != nil {
		return nil, err
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(k)})
	if err != nil {
		var missing *s3types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, fmt.Errorf("get s3 object %q: %w: %w", k, fs.ErrNotExist, err)
		}
		return nil, fmt.Errorf("get s3 object %q: %w", k, err)
	}
	defer func() { _ = out.Body.Close() }()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 object %q: %w", k, err)
	}
	return b, nil
}

// Put writes the organization's object at key.
func (s S3) Put(ctx context.Context, orgID, key string, data []byte) error {
	k, err := s.resolve(orgID, key)
	if err != nil {
		return err
	}
	if _, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(k), Body: bytes.NewReader(data)}); err != nil {
		return fmt.Errorf("put s3 object %q: %w", k, err)
	}
	return nil
}
