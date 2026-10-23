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

package objectstoreplugin

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/pkg/objectstorage"
)

// refusingObjects wraps a backend and answers 403 to every object request
// until allowed is set, the way GCS treats a token that no longer covers the
// actor's prefix.
type refusingObjects struct {
	*memObjects
	allowed  atomic.Bool
	refusals atomic.Int32
}

func (r *refusingObjects) refuse() error {
	r.refusals.Add(1)
	return &googleapi.Error{Code: http.StatusForbidden, Message: "forbidden"}
}

func (r *refusingObjects) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	if !r.allowed.Load() {
		return nil, r.refuse()
	}
	return r.memObjects.GetObject(ctx, bucket, object)
}

func (r *refusingObjects) PutObject(ctx context.Context, bucket, object string, body io.Reader) error {
	if !r.allowed.Load() {
		// GCS reads the body before it answers, so a refused upload leaves
		// the caller's file at its end.
		_, _ = io.Copy(io.Discard, body)
		return r.refuse()
	}
	return r.memObjects.PutObject(ctx, bucket, object, body)
}

// fakeCredentials hands out one client per call and records the calls. The
// forced client is allowed through, the first is not, unless stale is false.
type fakeCredentials struct {
	backend *memObjects
	stale   bool
	err     error

	mu     sync.Mutex
	forces []bool
}

func (f *fakeCredentials) Client(_ context.Context, _ string, force bool) (objectstorage.ObjectStorage, error) {
	f.mu.Lock()
	f.forces = append(f.forces, force)
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	client := &refusingObjects{memObjects: f.backend}
	client.allowed.Store(force || !f.stale)
	return client, nil
}

func (f *fakeCredentials) calls() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.forces...)
}

func newCredsPlugin(t *testing.T, creds *fakeCredentials, root string) *NodePlugin {
	t.Helper()
	p, err := NewNodePlugin(creds.backend, root, objectstorage.DefaultUploadMemory, WithCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writeFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("payload "+name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTransferRetriesOnceWithAReplacedCredential(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFiles(t, src, "a.img", "b.img", "c.img", manifestFile)
	creds := &fakeCredentials{backend: newMemObjects(), stale: true}
	p := newCredsPlugin(t, creds, root)

	_, err := p.UploadSnapshot(t.Context(), &objectstore.UploadSnapshotRequest{
		SnapshotUri: testURI, ActorJwt: "jwt", LocalPath: src, Files: []string{"a.img", "b.img", "c.img", manifestFile},
	})
	if err != nil {
		t.Fatalf("UploadSnapshot with a stale credential: %v", err)
	}
	// Three files refused on the first client force one replacement between
	// them, not one each, and the manifest then rides the replaced client.
	forces := creds.calls()
	if want := []bool{false, true}; len(forces) != 2 || forces[0] || !forces[1] {
		t.Errorf("credential calls = %v, want %v", forces, want)
	}
	if got := len(creds.backend.keys()); got != 4 {
		t.Errorf("stored %d objects, want 4", got)
	}

	dst := filepath.Join(root, "dst")
	if err := os.Mkdir(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	creds2 := &fakeCredentials{backend: creds.backend, stale: true}
	p2 := newCredsPlugin(t, creds2, root)
	if _, err := p2.FetchSnapshot(t.Context(), &objectstore.FetchSnapshotRequest{
		SnapshotUri: testURI, ActorJwt: "jwt", WritePath: dst, Files: []string{"a.img", manifestFile},
	}); err != nil {
		t.Fatalf("FetchSnapshot with a stale credential: %v", err)
	}
	if forces := creds2.calls(); len(forces) != 2 || !forces[1] {
		t.Errorf("fetch credential calls = %v, want one plain and one forced", forces)
	}
}

func TestTransferRetriesTheManifestFromItsStart(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, manifestFile)
	creds := &fakeCredentials{backend: newMemObjects(), stale: true}
	p := newCredsPlugin(t, creds, root)
	if _, err := p.UploadSnapshot(t.Context(), &objectstore.UploadSnapshotRequest{
		SnapshotUri: testURI, ActorJwt: "jwt", LocalPath: root, Files: []string{manifestFile},
	}); err != nil {
		t.Fatalf("UploadSnapshot of the manifest with a stale credential: %v", err)
	}
	got, err := io.ReadAll(mustGet(t, creds.backend, testBucket, testPrefix+"/"+manifestFile))
	if err != nil {
		t.Fatal(err)
	}
	if want := "payload " + manifestFile; string(got) != want {
		t.Fatalf("stored manifest = %q, want %q: the retry sent what was left of the file", got, want)
	}
}

func mustGet(t *testing.T, store *memObjects, bucket, object string) io.ReadCloser {
	t.Helper()
	r, err := store.GetObject(t.Context(), bucket, object)
	if err != nil {
		t.Fatalf("GetObject %s/%s: %v", bucket, object, err)
	}
	return r
}

func TestTransferReportsAnActorItCannotTransferFor(t *testing.T) {
	root := t.TempDir()
	for _, code := range []codes.Code{codes.NotFound, codes.Aborted} {
		creds := &fakeCredentials{backend: newMemObjects(), err: status.Error(code, "actor not found")}
		p := newCredsPlugin(t, creds, root)
		_, err := p.FetchSnapshot(t.Context(), &objectstore.FetchSnapshotRequest{
			SnapshotUri: testURI, ActorJwt: "jwt", WritePath: root, Files: []string{manifestFile},
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("credential source %v: FetchSnapshot = %v, want FailedPrecondition, NotFound is a missing object", code, err)
		}
	}
}

func TestTransferFailsClosedOnASecondRefusal(t *testing.T) {
	root := t.TempDir()
	creds := &fakeCredentials{backend: newMemObjects(), stale: true}
	p := newCredsPlugin(t, creds, root)
	// A forced client that still refuses: every client this source hands out
	// refuses, which is what a request outside the actor's prefixes sees.
	refuseAll := &fakeCredentials{backend: creds.backend, stale: true}
	p.creds = credentialsFunc(func(ctx context.Context, jwt string, force bool) (objectstorage.ObjectStorage, error) {
		client, err := refuseAll.Client(ctx, jwt, force)
		if err == nil {
			client.(*refusingObjects).allowed.Store(false)
		}
		return client, err
	})
	_, err := p.FetchSnapshot(t.Context(), &objectstore.FetchSnapshotRequest{
		SnapshotUri: testURI, ActorJwt: "jwt", WritePath: root, Files: []string{manifestFile},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("FetchSnapshot refused twice = %v, want PermissionDenied", err)
	}
	if forces := refuseAll.calls(); len(forces) != 2 {
		t.Errorf("credential calls = %v, want two", forces)
	}
}

type credentialsFunc func(context.Context, string, bool) (objectstorage.ObjectStorage, error)

func (f credentialsFunc) Client(ctx context.Context, jwt string, force bool) (objectstorage.ObjectStorage, error) {
	return f(ctx, jwt, force)
}

func TestTransferReturnsTheCredentialSourcesStatus(t *testing.T) {
	root := t.TempDir()
	creds := &fakeCredentials{backend: newMemObjects(), err: status.Error(codes.PermissionDenied, "actor is no longer on the token's node")}
	p := newCredsPlugin(t, creds, root)
	_, err := p.FetchSnapshot(t.Context(), &objectstore.FetchSnapshotRequest{
		SnapshotUri: testURI, ActorJwt: "jwt", WritePath: root, Files: []string{manifestFile},
	})
	if status.Code(err) != codes.PermissionDenied || !strings.Contains(status.Convert(err).Message(), "actor is no longer on the token's node") {
		t.Fatalf("err = %v, want the source's code and message", err)
	}
}

func TestAmbientCredentialsIgnoreTheJWT(t *testing.T) {
	root := t.TempDir()
	backend := newMemObjects()
	p, err := NewNodePlugin(backend, root, objectstorage.DefaultUploadMemory)
	if err != nil {
		t.Fatal(err)
	}
	writeFiles(t, root, manifestFile)
	for _, jwt := range []string{"", "anything"} {
		if _, err := p.UploadSnapshot(t.Context(), &objectstore.UploadSnapshotRequest{
			SnapshotUri: testURI, ActorJwt: jwt, LocalPath: root, Files: []string{manifestFile},
		}); err != nil {
			t.Fatalf("UploadSnapshot(actor_jwt=%q) with ambient credentials: %v", jwt, err)
		}
	}
}
