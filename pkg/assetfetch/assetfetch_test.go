// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package assetfetch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testBucket = "bucket"

func TestParseURI(t *testing.T) {
	for _, tc := range []struct {
		uri  string
		want codes.Code
	}{
		{"gs://bucket/kata-assets/vmlinux", codes.OK},
		{"https://acct.blob.core.windows.net/assets/vmlinux", codes.OK},
		{"https://acct.blob.core.windows.net/assets/vmlinux?sig=secret", codes.InvalidArgument},
		{"https://acct.blob.core.windows.net/assets/vmlinux?", codes.InvalidArgument},
		{"https://user:secret@acct.blob.core.windows.net/assets/vmlinux", codes.InvalidArgument},
		{"https://acct.blob.core.windows.net/assets/vmlinux#frag", codes.InvalidArgument},
		{"mailto:secret@example.com", codes.InvalidArgument},
		{"https://acct\x7f/x", codes.InvalidArgument},
		{"gs://bucket/" + strings.Repeat("x", MaxURIBytes), codes.InvalidArgument},
	} {
		_, err := ParseURI(tc.uri)
		if got := status.Code(err); got != tc.want {
			t.Errorf("ParseURI(%.60q) = %v, want %s", tc.uri, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("ParseURI(%q) error %q quotes the URI's credentials", tc.uri, err)
		}
	}
}

func TestCheckObjectName(t *testing.T) {
	const base = "https://acct.blob.core.windows.net/assets"
	for _, tc := range []struct {
		object string
		want   codes.Code
	}{
		{"kata-assets/vmlinux", codes.OK},
		{"vmlinux", codes.OK},
		{"atespaces/readme", codes.OK},
		{"root/atespaces/team-a/actors/uid1", codes.OK},
		{"root/atespaces/Not_A_Name/tags/tag1/manifest.json", codes.OK},
		{"", codes.InvalidArgument},
		{"a//b", codes.InvalidArgument},
		{"a/b/", codes.InvalidArgument},
		{"/a", codes.InvalidArgument},
		{"a/../b", codes.InvalidArgument},
		{"a/./b", codes.InvalidArgument},
		{"a/b.", codes.InvalidArgument},
		{"a/ b", codes.InvalidArgument},
		{`a\b`, codes.InvalidArgument},
		{"substrate/atespaces/team-a/actors/uid1/snapshots/snap1/pages.img", codes.PermissionDenied},
		{"substrate/atespaces/team-a/actors/uid1/snapshots/snap1", codes.PermissionDenied},
		{"atespaces/team-a/actors/uid1/snapshots/snap1/manifest.json", codes.PermissionDenied},
		{"x/y/atespaces/ate-golden/tags/d33a8f2f-c6ce-45b7-9659-69d1fee12fa5/checkpoint.img", codes.PermissionDenied},
		{"x/atespaces/team-a/tags/tag1", codes.PermissionDenied},
	} {
		uri := base + "/" + tc.object
		err := CheckObjectName(uri, base, tc.object)
		if got := status.Code(err); got != tc.want {
			t.Errorf("CheckObjectName(%q) = %v, want %s", tc.object, err, tc.want)
		}
	}
	err := CheckObjectName(base+"/s/atespaces/a/tags/t/f", base, "s/atespaces/a/tags/t/f")
	if want := base + "/s/atespaces/a/tags/t;"; !strings.Contains(err.Error(), want) {
		t.Errorf("CheckObjectName error %q does not name the tag location %q", err, want)
	}
}

// TestSnapshotPrefixLenMatchesParseSnapshotURI checks snapshotPrefixLen
// against resources.ParseSnapshotURI run on every prefix, for every path of
// up to 7 segments drawn from the snapshot layout's keywords and an invalid
// resource name.
func TestSnapshotPrefixLenMatchesParseSnapshotURI(t *testing.T) {
	alphabet := []string{"atespaces", "actors", "tags", "snapshots", "Not_A_Name"}
	parsesAt := func(segments []string) int {
		for i := 1; i <= len(segments); i++ {
			if _, err := resources.ParseSnapshotURI("gs://" + testBucket + "/" + strings.Join(segments[:i], "/")); err == nil {
				return i
			}
		}
		return 0
	}
	var walk func(segments []string)
	walk = func(segments []string) {
		if len(segments) > 0 {
			got, want := snapshotPrefixLen(segments), parsesAt(segments)
			if (got > 0) != (want > 0) {
				t.Errorf("snapshotPrefixLen(%q) = %d, but ParseSnapshotURI accepts a prefix of length %d", segments, got, want)
			} else if got > 0 {
				if _, err := resources.ParseSnapshotURI("gs://" + testBucket + "/" + strings.Join(segments[:got], "/")); err != nil {
					t.Errorf("snapshotPrefixLen(%q) = %d, but ParseSnapshotURI rejects that prefix: %v", segments, got, err)
				}
			}
		}
		if len(segments) == 7 {
			return
		}
		for _, seg := range alphabet {
			walk(append(segments[:len(segments):len(segments)], seg))
		}
	}
	walk(nil)
}

// TestSnapshotPrefixLenIsLinear runs the check on far more segments than a
// URI can hold. A check that re-reads every prefix would not finish.
func TestSnapshotPrefixLenIsLinear(t *testing.T) {
	unit := []string{"atespaces", "team-a", "actors", "uid1", "snapshots", "Not_A_Name", "atespaces", "team-a", "atespaces"}
	segments := make([]string, 0, 1<<20+6)
	for len(segments) < 1<<20 {
		segments = append(segments, unit...)
	}
	if n := snapshotPrefixLen(segments); n != 0 {
		t.Fatalf("snapshotPrefixLen = %d, want 0", n)
	}
	segments = append(segments, "atespaces", "team-a", "tags", "tag1")
	if n, want := snapshotPrefixLen(segments), len(segments); n != want {
		t.Fatalf("snapshotPrefixLen = %d, want %d", n, want)
	}
}

// stagerFixture is a Stager with a write root, a separate staging directory
// and an existing caller file below the root.
type stagerFixture struct {
	stager  *Stager
	staging string
	dst     string
}

const untouched = "the caller's original bytes"

func newStagerFixture(t *testing.T) *stagerFixture {
	t.Helper()
	root, staging := t.TempDir(), t.TempDir()
	stager, err := NewStager(root, staging, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "static-files", "asset-download-1")
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, []byte(untouched), 0o600); err != nil {
		t.Fatal(err)
	}
	return &stagerFixture{stager: stager, staging: staging, dst: dst}
}

func (f *stagerFixture) assertDst(t *testing.T, want string) {
	t.Helper()
	got, err := os.ReadFile(f.dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("write path holds %q, want %q", got, want)
	}
	f.assertNoTempFiles(t)
}

// assertNoTempFiles checks that no file written next to the write path is
// left behind.
func (f *stagerFixture) assertNoTempFiles(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(f.dst))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".assetfetch-") {
			t.Errorf("write path's directory holds %s after the call", e.Name())
		}
	}
}

func (f *stagerFixture) assertStagingEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("staging directory holds %d entries after the call, want none", len(entries))
	}
}

// opener serves content and counts how often it is opened.
type opener struct {
	content string
	err     error
	opens   int
}

func (o *opener) open(context.Context) (io.ReadCloser, error) {
	o.opens++
	if o.err != nil {
		return nil, o.err
	}
	return io.NopCloser(strings.NewReader(o.content)), nil
}

var errBackend = errors.New("backend error")

// testStatus maps errBackend to Unavailable, a context error to its code, and
// everything else to Internal, like a plugin's own mapping.
func testStatus(err error) error {
	switch {
	case errors.Is(err, errBackend):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

func sum(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }

func TestStagerFetch(t *testing.T) {
	const kernel = "kernel bytes"
	t.Run("verified content replaces the caller's file", func(t *testing.T) {
		f := newStagerFixture(t)
		o := &opener{content: kernel}
		err := f.stager.Fetch(t.Context(), &objectstorev1.FetchAssetRequest{
			Sha256: sum(kernel), WritePath: f.dst, MaxBytes: int64(len(kernel)),
		}, "gs://bucket/vmlinux", o.open, testStatus)
		if err != nil {
			t.Fatalf("Fetch = %v, want success", err)
		}
		f.assertDst(t, kernel)
		f.assertStagingEmpty(t)
	})

	for _, tc := range []struct {
		name      string
		req       func(f *stagerFixture) *objectstorev1.FetchAssetRequest
		opener    *opener
		want      codes.Code
		wantOpens int
	}{
		{
			name:      "sha256 mismatch",
			req:       func(f *stagerFixture) *objectstorev1.FetchAssetRequest { return req(sum("other"), f.dst, 1<<20) },
			opener:    &opener{content: kernel},
			want:      codes.FailedPrecondition,
			wantOpens: 1,
		},
		{
			name: "one byte over max_bytes",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(sum(kernel), f.dst, int64(len(kernel)-1))
			},
			opener:    &opener{content: kernel},
			want:      codes.FailedPrecondition,
			wantOpens: 1,
		},
		{
			name:      "open error goes through toStatus",
			req:       func(f *stagerFixture) *objectstorev1.FetchAssetRequest { return req(sum(kernel), f.dst, 1<<20) },
			opener:    &opener{err: errBackend},
			want:      codes.Unavailable,
			wantOpens: 1,
		},
		{
			name: "upper-case sha256",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(strings.ToUpper(sum(kernel)), f.dst, 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.InvalidArgument,
		},
		{
			name:   "zero max_bytes",
			req:    func(f *stagerFixture) *objectstorev1.FetchAssetRequest { return req(sum(kernel), f.dst, 0) },
			opener: &opener{content: kernel},
			want:   codes.InvalidArgument,
		},
		{
			name: "relative write path",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(sum(kernel), "static-files/x", 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.InvalidArgument,
		},
		{
			name: "write path outside root",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(sum(kernel), filepath.Join(f.staging, "x"), 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.InvalidArgument,
		},
		{
			name: "missing write path",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(sum(kernel), f.dst+"-missing", 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.FailedPrecondition,
		},
		{
			name: "write path is a directory",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				return req(sum(kernel), filepath.Dir(f.dst), 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.FailedPrecondition,
		},
		{
			name: "write path is a symlink",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				link := f.dst + "-link"
				if err := os.Symlink(f.dst, link); err != nil {
					panic(err)
				}
				return req(sum(kernel), link, 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.FailedPrecondition,
		},
		{
			name: "write path is a FIFO",
			req: func(f *stagerFixture) *objectstorev1.FetchAssetRequest {
				fifo := f.dst + "-fifo"
				if err := syscall.Mkfifo(fifo, 0o600); err != nil {
					panic(err)
				}
				return req(sum(kernel), fifo, 1<<20)
			},
			opener: &opener{content: kernel},
			want:   codes.FailedPrecondition,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newStagerFixture(t)
			err := f.stager.Fetch(t.Context(), tc.req(f), "gs://bucket/vmlinux", tc.opener.open, testStatus)
			if got := status.Code(err); got != tc.want {
				t.Fatalf("Fetch = %v, want %s", err, tc.want)
			}
			if tc.opener.opens != tc.wantOpens {
				t.Errorf("Fetch opened the asset %d times, want %d", tc.opener.opens, tc.wantOpens)
			}
			f.assertDst(t, untouched)
			f.assertStagingEmpty(t)
		})
	}

	t.Run("canceled context", func(t *testing.T) {
		f := newStagerFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		o := &opener{err: context.Canceled}
		err := f.stager.Fetch(ctx, req(sum(kernel), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus)
		if got := status.Code(err); got != codes.Canceled {
			t.Fatalf("Fetch = %v, want %s", err, codes.Canceled)
		}
		f.assertDst(t, untouched)
		f.assertStagingEmpty(t)
	})
}

func req(sha string, writePath string, maxBytes int64) *objectstorev1.FetchAssetRequest {
	return &objectstorev1.FetchAssetRequest{Sha256: sha, WritePath: writePath, MaxBytes: maxBytes}
}

func TestNewStagerRejectsStagingBelowRoot(t *testing.T) {
	root := t.TempDir()
	for _, staging := range []string{root, filepath.Join(root, "staging"), root + "/./staging/.."} {
		if _, err := NewStager(root, staging, 1<<30); err == nil {
			t.Errorf("NewStager(root=%s, staging=%s) succeeded, want an error", root, staging)
		}
	}
	for _, tc := range []struct{ root, staging string }{{"relative", t.TempDir()}, {root, "relative"}} {
		if _, err := NewStager(tc.root, tc.staging, 1<<30); err == nil {
			t.Errorf("NewStager(root=%s, staging=%s) succeeded, want an error", tc.root, tc.staging)
		}
	}
	if _, err := NewStager(root, t.TempDir(), 1<<30); err != nil {
		t.Errorf("NewStager with a separate staging directory: %v", err)
	}
}

func TestNewStagerRejectsNonPositiveCapacity(t *testing.T) {
	for _, capacity := range []int64{0, -1} {
		if _, err := NewStager(t.TempDir(), t.TempDir(), capacity); err == nil {
			t.Errorf("NewStager(capacity=%d) succeeded, want an error", capacity)
		}
	}
}

func TestNewStagerRemovesPreviousDownloads(t *testing.T) {
	staging := t.TempDir()
	for _, name := range []string{"asset-123", "asset-456", "unrelated"} {
		if err := os.WriteFile(filepath.Join(staging, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewStager(t.TempDir(), staging, 1<<30); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "unrelated" {
		t.Errorf("staging directory holds %v after NewStager, want only unrelated", entries)
	}
}

// failingCopy writes half of src and then fails, as a full disk would.
func failingCopy(dst io.Writer, src io.Reader) (int64, error) {
	b, err := io.ReadAll(src)
	if err != nil {
		return 0, err
	}
	n, _ := dst.Write(b[:len(b)/2])
	return int64(n), errors.New("no space left on device")
}

func setCopyToTemp(t *testing.T, fn func(io.Writer, io.Reader) (int64, error)) {
	t.Helper()
	orig := copyToTemp
	copyToTemp = fn
	t.Cleanup(func() { copyToTemp = orig })
}

func TestStagerFetchReplacesWritePathAtomically(t *testing.T) {
	const kernel = "kernel bytes"

	t.Run("failed copy leaves the write path untouched", func(t *testing.T) {
		setCopyToTemp(t, failingCopy)
		f := newStagerFixture(t)
		o := &opener{content: kernel}
		err := f.stager.Fetch(t.Context(), req(sum(kernel), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus)
		if got := status.Code(err); got != codes.Internal {
			t.Fatalf("Fetch = %v, want %s", err, codes.Internal)
		}
		f.assertDst(t, untouched)
		f.assertStagingEmpty(t)
	})

	t.Run("write path removed during the copy is not recreated", func(t *testing.T) {
		f := newStagerFixture(t)
		setCopyToTemp(t, func(dst io.Writer, src io.Reader) (int64, error) {
			if err := os.Remove(f.dst); err != nil {
				return 0, err
			}
			return io.Copy(dst, src)
		})
		o := &opener{content: kernel}
		err := f.stager.Fetch(t.Context(), req(sum(kernel), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("Fetch = %v, want %s", err, codes.FailedPrecondition)
		}
		if _, err := os.Lstat(f.dst); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("write path exists after the call (%v), want it left removed", err)
		}
		f.assertNoTempFiles(t)
		f.assertStagingEmpty(t)
	})

	t.Run("keeps the write path's mode", func(t *testing.T) {
		f := newStagerFixture(t)
		if err := os.Chmod(f.dst, 0o640); err != nil {
			t.Fatal(err)
		}
		o := &opener{content: kernel}
		if err := f.stager.Fetch(t.Context(), req(sum(kernel), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus); err != nil {
			t.Fatalf("Fetch = %v, want success", err)
		}
		f.assertDst(t, kernel)
		info, err := os.Stat(f.dst)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o640 {
			t.Errorf("write path mode = %v, want %v", got, fs.FileMode(0o640))
		}
	})

	t.Run("longest file name", func(t *testing.T) {
		f := newStagerFixture(t)
		f.dst = filepath.Join(filepath.Dir(f.dst), strings.Repeat("a", 255))
		if err := os.WriteFile(f.dst, []byte(untouched), 0o600); err != nil {
			t.Fatal(err)
		}
		o := &opener{content: kernel}
		if err := f.stager.Fetch(t.Context(), req(sum(kernel), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus); err != nil {
			t.Fatalf("Fetch = %v, want success", err)
		}
		f.assertDst(t, kernel)
	})
}

// gatedOpener serves content once release is closed, after reporting each
// open on opened.
type gatedOpener struct {
	content string
	opened  chan struct{}
	release chan struct{}
}

func newGatedOpener(content string) *gatedOpener {
	return &gatedOpener{content: content, opened: make(chan struct{}, 2), release: make(chan struct{})}
}

func (o *gatedOpener) open(ctx context.Context) (io.ReadCloser, error) {
	o.opened <- struct{}{}
	select {
	case <-o.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(strings.NewReader(o.content)), nil
}

func newStagerFixtureWithCapacity(t *testing.T, capacity int64) *stagerFixture {
	t.Helper()
	f := newStagerFixture(t)
	stager, err := NewStager(f.stager.root, f.staging, capacity)
	if err != nil {
		t.Fatal(err)
	}
	f.stager = stager
	return f
}

// newCallerFile creates another existing caller file next to f.dst.
func (f *stagerFixture) newCallerFile(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(filepath.Dir(f.dst), name)
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestStagerFetchStagingCapacity(t *testing.T) {
	const kernel = "kernel bytes"
	const maxBytes = 60

	t.Run("a download waits while the capacity is reserved", func(t *testing.T) {
		f := newStagerFixtureWithCapacity(t, 100)
		first, second := newGatedOpener(kernel), newGatedOpener(kernel)
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- f.stager.Fetch(t.Context(), req(sum(kernel), f.dst, maxBytes), "gs://bucket/a", first.open, testStatus)
		}()
		<-first.opened
		secondDone := make(chan error, 1)
		go func() {
			secondDone <- f.stager.Fetch(t.Context(), req(sum(kernel), f.newCallerFile(t, "b"), maxBytes), "gs://bucket/b", second.open, testStatus)
		}()
		close(second.release)
		select {
		case <-second.opened:
			t.Fatal("second download started while the first held more than the remaining capacity")
		case <-time.After(200 * time.Millisecond):
		}
		close(first.release)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Fetch = %v", err)
		}
		if err := <-secondDone; err != nil {
			t.Fatalf("second Fetch = %v", err)
		}
		f.assertStagingEmpty(t)
	})

	t.Run("downloads that fit run together", func(t *testing.T) {
		f := newStagerFixtureWithCapacity(t, 2*maxBytes)
		o := newGatedOpener(kernel)
		done := make(chan error, 2)
		for _, name := range []string{"a", "b"} {
			p := f.newCallerFile(t, name)
			go func() {
				done <- f.stager.Fetch(t.Context(), req(sum(kernel), p, maxBytes), "gs://bucket/"+name, o.open, testStatus)
			}()
		}
		for range 2 {
			select {
			case <-o.opened:
			case <-time.After(10 * time.Second):
				t.Fatal("two downloads within the capacity did not run together")
			}
		}
		close(o.release)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatalf("Fetch = %v", err)
			}
		}
	})

	t.Run("canceled while waiting", func(t *testing.T) {
		f := newStagerFixtureWithCapacity(t, maxBytes)
		first := newGatedOpener(kernel)
		firstDone := make(chan error, 1)
		go func() {
			firstDone <- f.stager.Fetch(t.Context(), req(sum(kernel), f.newCallerFile(t, "a"), maxBytes), "gs://bucket/a", first.open, testStatus)
		}()
		<-first.opened
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		o := &opener{content: kernel}
		err := f.stager.Fetch(ctx, req(sum(kernel), f.dst, maxBytes), "gs://bucket/vmlinux", o.open, testStatus)
		if got := status.Code(err); got != codes.Canceled {
			t.Fatalf("Fetch = %v, want %s", err, codes.Canceled)
		}
		if o.opens != 0 {
			t.Errorf("Fetch opened the asset %d times while waiting, want 0", o.opens)
		}
		f.assertDst(t, untouched)
		close(first.release)
		if err := <-firstDone; err != nil {
			t.Fatalf("first Fetch = %v", err)
		}
	})

	t.Run("max_bytes above the capacity", func(t *testing.T) {
		const capacity = 16
		for _, tc := range []struct {
			name    string
			content string
			want    codes.Code
		}{
			{"asset fits", strings.Repeat("k", capacity), codes.OK},
			{"asset larger than the capacity", strings.Repeat("k", capacity+10), codes.FailedPrecondition},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newStagerFixtureWithCapacity(t, capacity)
				o := &stagingWatcher{content: tc.content, staging: f.staging}
				err := f.stager.Fetch(t.Context(), req(sum(tc.content), f.dst, 1<<20), "gs://bucket/vmlinux", o.open, testStatus)
				if got := status.Code(err); got != tc.want {
					t.Fatalf("Fetch = %v, want %s", err, tc.want)
				}
				if o.maxStaged > capacity {
					t.Errorf("staging directory held %d bytes, more than the capacity of %d", o.maxStaged, capacity)
				}
				if tc.want == codes.OK {
					f.assertDst(t, tc.content)
				} else {
					f.assertDst(t, untouched)
					if !strings.Contains(err.Error(), "staging capacity") {
						t.Errorf("Fetch error %q does not name the staging capacity", err)
					}
				}
				f.assertStagingEmpty(t)
			})
		}
	})
}

// stagingWatcher serves content a byte at a time and records the most bytes
// the staging directory held before each read.
type stagingWatcher struct {
	content   string
	staging   string
	read      int
	maxStaged int64
}

func (w *stagingWatcher) open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(w), nil
}

func (w *stagingWatcher) Read(p []byte) (int, error) {
	entries, err := os.ReadDir(w.staging)
	if err != nil {
		return 0, err
	}
	var staged int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		staged += info.Size()
	}
	w.maxStaged = max(w.maxStaged, staged)
	if w.read == len(w.content) || len(p) == 0 {
		return 0, io.EOF
	}
	p[0] = w.content[w.read]
	w.read++
	return 1, nil
}
