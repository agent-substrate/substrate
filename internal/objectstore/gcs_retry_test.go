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

package objectstore_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"
)

// fakeGCS is a minimal GCS JSON API (list, delete, rewrite) over one in-memory
// bucket. A request first gets the next status scripted for its key ("LIST
// <prefix>", "DELETE <object>" or "COPY <dst object>") and is served normally
// once none are left, so a test sets exactly how many errors each call sees.
type fakeGCS struct {
	t *testing.T

	mu       sync.Mutex
	objects  map[string]bool
	script   map[string][]int
	attempts map[string]int
}

// newFakeGCS starts a fake GCS holding objects and returns it with a Store
// built the way cmd/ateapi builds one.
func newFakeGCS(t *testing.T, objects ...string) (*fakeGCS, objectstore.Store) {
	t.Helper()
	fake := &fakeGCS{t: t, objects: map[string]bool{}, script: map[string][]int{}, attempts: map[string]int{}}
	for _, name := range objects {
		fake.objects[name] = true
	}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	// The storage client sends every call to the emulator host when it is set.
	t.Setenv("STORAGE_EMULATOR_HOST", srv.URL)
	client, err := storage.NewClient(t.Context())
	if err != nil {
		t.Fatalf("storage client: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return fake, objectstore.NewGCS(client)
}

// respond scripts the statuses key's next requests get.
func (f *fakeGCS) respond(key string, statuses ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script[key] = statuses
}

// attemptsOf returns how many requests key has seen.
func (f *fakeGCS) attemptsOf(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[key]
}

// names returns the bucket's object names, sorted.
func (f *fakeGCS) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.objects))
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Object names arrive path-escaped ("snap%2Ffile"), so split the path
	// before unescaping them. There is one bucket, so its name is dropped.
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), "/storage/v1/b/")
	_, rest, _ = strings.Cut(rest, "/")
	switch {
	case ok && r.Method == http.MethodGet && rest == "o":
		prefix := r.URL.Query().Get("prefix")
		if f.scripted(w, "LIST "+prefix) {
			return
		}
		var items []string
		for _, name := range f.names() {
			if strings.HasPrefix(name, prefix) {
				items = append(items, fmt.Sprintf(`{"name":%q}`, name))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))

	case ok && r.Method == http.MethodDelete && strings.HasPrefix(rest, "o/"):
		object := f.unescape(strings.TrimPrefix(rest, "o/"))
		if f.scripted(w, "DELETE "+object) {
			return
		}
		f.mu.Lock()
		found := f.objects[object]
		delete(f.objects, object)
		f.mu.Unlock()
		if !found {
			writeGCSError(w, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	case ok && r.Method == http.MethodPost && strings.Contains(rest, "/rewriteTo/b/"):
		src, dst, _ := strings.Cut(strings.TrimPrefix(rest, "o/"), "/rewriteTo/b/")
		_, dst, _ = strings.Cut(dst, "/o/")
		src, dst = f.unescape(src), f.unescape(dst)
		if f.scripted(w, "COPY "+dst) {
			return
		}
		f.mu.Lock()
		found := f.objects[src]
		if found {
			f.objects[dst] = true
		}
		f.mu.Unlock()
		if !found {
			writeGCSError(w, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"done":true,"resource":{"name":%q}}`, dst)

	default:
		f.t.Errorf("fake GCS got an unexpected request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
	}
}

// scripted counts a request for key and answers it with key's next scripted
// status, if there is one. It reports whether it answered.
func (f *fakeGCS) scripted(w http.ResponseWriter, key string) bool {
	f.mu.Lock()
	f.attempts[key]++
	var code int
	if next := f.script[key]; len(next) > 0 {
		code, f.script[key] = next[0], next[1:]
	}
	f.mu.Unlock()
	if code == 0 {
		return false
	}
	writeGCSError(w, code)
	return true
}

func (f *fakeGCS) unescape(s string) string {
	name, err := url.PathUnescape(s)
	if err != nil {
		f.t.Errorf("unescaping %q: %v", s, err)
	}
	return name
}

func writeGCSError(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":{"code":%d,"message":%q}}`, code, http.StatusText(code))
}

// callCtx bounds a call, so a retry loop that never gives up fails the test
// instead of hanging it.
func callCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// httpCode returns the HTTP status err carries, or 0 if it carries none.
func httpCode(err error) int {
	if gerr, ok := errors.AsType[*googleapi.Error](err); ok {
		return gerr.Code
	}
	return 0
}

// TestGCSDelete covers how Delete retries: transient errors until it
// succeeds or has made 5 attempts, permanent errors not at all.
func TestGCSDelete(t *testing.T) {
	tests := []struct {
		name string
		// statuses are answered before the fake serves the delete.
		statuses     []int
		wantCode     int // HTTP status of the returned error; 0 for success
		wantAttempts int
	}{
		{name: "429s, then success", statuses: []int{429, 429}, wantAttempts: 3},
		// The first delete landed but its response was lost. The retry's 404
		// still means the object is gone.
		{name: "503, then 404", statuses: []int{503, 404}, wantAttempts: 2},
		{name: "gives up after 5 attempts", statuses: slices.Repeat([]int{429}, 10), wantCode: 429, wantAttempts: 5},
		{name: "403 is not retried", statuses: []int{403}, wantCode: 403, wantAttempts: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake, store := newFakeGCS(t, "snap/file")
			fake.respond("DELETE snap/file", tc.statuses...)

			ctx := callCtx(t)
			err := store.Delete(ctx, "bucket", "snap/file")
			switch {
			case tc.wantCode == 0 && err != nil:
				t.Fatalf("Delete() = %v, want nil", err)
			case httpCode(err) != tc.wantCode:
				t.Fatalf("Delete() = %v, want HTTP %d", err, tc.wantCode)
			case ctx.Err() != nil:
				t.Fatal("Delete() ran until its context ended, want it to stop on its own")
			}
			if got := fake.attemptsOf("DELETE snap/file"); got != tc.wantAttempts {
				t.Errorf("Delete() made %d attempts, want %d", got, tc.wantAttempts)
			}
		})
	}
}

// TestGCSDeletePrefixRetries429 replays a snapshot release during a suspend
// burst on a cold bucket: every delete gets 429s and must be retried rather
// than fail the release.
func TestGCSDeletePrefixRetries429(t *testing.T) {
	uri := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
	_, prefix, err := objectstore.BucketPrefix(uri.Prefix())
	if err != nil {
		t.Fatalf("BucketPrefix() = %v", err)
	}
	var objects []string
	for i := range 50 {
		objects = append(objects, fmt.Sprintf("%sfile-%02d", prefix, i))
	}
	fake, store := newFakeGCS(t, objects...)
	// Deletes run prefixConcurrency at a time, so a few 429s per object
	// already add up to seconds of backoff.
	for _, object := range objects {
		fake.respond("DELETE "+object, 429, 429)
	}

	if err := objectstore.DeletePrefix(callCtx(t), store, uri.Prefix()); err != nil {
		t.Fatalf("DeletePrefix() = %v, want nil", err)
	}
	for _, object := range objects {
		if got := fake.attemptsOf("DELETE " + object); got != 3 {
			t.Errorf("deleting %s took %d attempts, want 3", object, got)
		}
	}
	if left := fake.names(); len(left) != 0 {
		t.Errorf("DeletePrefix() left %d objects, want 0", len(left))
	}
}

// TestGCSCopyRetries429 covers a copy through 429s. The copier takes its retry
// settings from the destination handle, so this fails if only the source has
// them.
func TestGCSCopyRetries429(t *testing.T) {
	fake, store := newFakeGCS(t, "snap/file")
	fake.respond("COPY tag/file", 429, 429)

	if err := store.Copy(callCtx(t), "bucket", "snap/file", "bucket", "tag/file"); err != nil {
		t.Fatalf("Copy() = %v, want nil", err)
	}
	if got := fake.attemptsOf("COPY tag/file"); got != 3 {
		t.Errorf("Copy() made %d attempts, want 3", got)
	}
	if diff := cmp.Diff([]string{"snap/file", "tag/file"}, fake.names()); diff != "" {
		t.Errorf("objects after Copy() differ (-want +got):\n%s", diff)
	}
}

// TestGCSListGivesUp covers a list that only ever gets 429s. The default
// policy would retry it until the context ends, and SuspendActor sets no
// deadline, so List must give up on its own.
func TestGCSListGivesUp(t *testing.T) {
	fake, store := newFakeGCS(t, "snap/file")
	fake.respond("LIST snap/", slices.Repeat([]int{429}, 10)...)

	ctx := callCtx(t)
	_, err := store.List(ctx, "bucket", "snap/")
	switch {
	case httpCode(err) != http.StatusTooManyRequests:
		t.Fatalf("List() = %v, want HTTP 429", err)
	case ctx.Err() != nil:
		t.Fatal("List() ran until its context ended, want it to give up on its own")
	}
	if got := fake.attemptsOf("LIST snap/"); got != 5 {
		t.Errorf("List() made %d attempts, want 5", got)
	}
}
