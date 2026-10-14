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

package main

import (
	"encoding/json"
	"github.com/agent-substrate/substrate/internal/e2e/probeapi"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func volumeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cached.txt", "held.txt", "unlinked.txt", "directory/old"} {
		volumeWrite(t, filepath.Join(root, name), "source-"+name)
	}
	return root
}

func volumeWrite(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
}

func volumeRequest(p *volumeStateProbe, method, target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	p.serveHTTP(w, httptest.NewRequest(method, target, nil))
	return w
}

func volumePrime(t *testing.T, p *volumeStateProbe, root string) {
	t.Helper()
	w := volumeRequest(p, http.MethodPost, "/volume-state?root="+url.QueryEscape(root)+"&challenge=capture-unique-token")
	if w.Code != http.StatusNoContent {
		t.Fatalf("prime: %d %s", w.Code, w.Body.String())
	}
	t.Cleanup(p.close)
}

func volumeObserve(t *testing.T, p *volumeStateProbe) probeapi.VolumeStateResponse {
	t.Helper()
	w := volumeRequest(p, http.MethodGet, "/volume-state")
	if w.Code != http.StatusOK {
		t.Fatalf("observe: %d %s", w.Code, w.Body.String())
	}
	var out probeapi.VolumeStateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// Ordinary host file replacement validates the probe's ability to distinguish
// path reads, open descriptors and application memory. It is not a VM restore.
func TestVolumeStateDistinguishesRetainedState(t *testing.T) {
	p := &volumeStateProbe{}
	root := volumeFixture(t)
	volumePrime(t, p, root)
	first := volumeObserve(t, p)
	if !first.Negative.NotExist {
		t.Fatalf("negative lookup: %+v", first.Negative)
	}
	if _, err := os.Stat(filepath.Join(root, "unlinked.txt")); !os.IsNotExist(err) {
		t.Fatalf("unlink: %v", err)
	}
	volumeWrite(t, filepath.Join(root, "replacement"), "target-held")
	if err := os.Rename(filepath.Join(root, "replacement"), filepath.Join(root, "held.txt")); err != nil {
		t.Fatal(err)
	}
	volumeWrite(t, filepath.Join(root, "cached.txt"), "target-cached")
	volumeWrite(t, filepath.Join(root, "negative.txt"), "target-negative")
	if err := os.Rename(filepath.Join(root, "directory"), filepath.Join(root, "old-directory")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0700); err != nil {
		t.Fatal(err)
	}
	volumeWrite(t, filepath.Join(root, "directory/new"), "target-directory")
	got := volumeObserve(t, p)
	if got.Challenge != "capture-unique-token" || got.Reads != 2 {
		t.Fatalf("state continuity: %+v", got)
	}
	if got.Cached != "source-cached.txt" || got.ByPath.Data != "target-cached" || got.ByPath.Error != "" {
		t.Fatalf("cache vs path: %+v", got)
	}
	if got.Held.Data != "source-held.txt" || got.Held.Error != "" {
		t.Fatalf("held descriptor: %+v", got.Held)
	}
	if got.Unlinked.Data != "source-unlinked.txt" || got.Unlinked.Error != "" {
		t.Fatalf("unlinked descriptor: %+v", got.Unlinked)
	}
	if got.Negative.Data != "target-negative" || got.Negative.Error != "" {
		t.Fatalf("negative lookup: %+v", got.Negative)
	}
	if got.DirectoryError != "" || strings.Join(got.Directory, ",") != "old" {
		t.Fatalf("held directory: %+v", got)
	}
}

func TestVolumeStateColdBootAndReprimeFail(t *testing.T) {
	p := &volumeStateProbe{}
	if w := volumeRequest(p, http.MethodGet, "/volume-state"); w.Code != http.StatusConflict {
		t.Fatalf("unprimed returned %d", w.Code)
	}
	root := volumeFixture(t)
	volumePrime(t, p, root)
	if w := volumeRequest(p, http.MethodPost, "/volume-state"); w.Code != http.StatusConflict {
		t.Fatalf("reprime returned %d", w.Code)
	}
	if w := volumeRequest(p, http.MethodDelete, "/volume-state"); w.Code != http.StatusNoContent {
		t.Fatalf("release returned %d", w.Code)
	}
	if w := volumeRequest(p, http.MethodGet, "/volume-state"); w.Code != http.StatusConflict {
		t.Fatalf("released returned %d", w.Code)
	}
}

func TestVolumeStatePrimeFailureClosesDescriptors(t *testing.T) {
	p := &volumeStateProbe{}
	root := volumeFixture(t)
	if err := os.Remove(filepath.Join(root, "unlinked.txt")); err != nil {
		t.Fatal(err)
	}
	w := volumeRequest(p, http.MethodPost, "/volume-state?root="+url.QueryEscape(root)+"&challenge=test")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing fixture returned %d", w.Code)
	}
	if p.held != nil || p.directory != nil || p.unlinked != nil || p.challenge != "" {
		t.Fatal("failed prime retained state")
	}
	volumeWrite(t, filepath.Join(root, "unlinked.txt"), "repaired")
	volumePrime(t, p, root)
}

func TestVolumeStateOversizeAndUnexpectedNegativeFail(t *testing.T) {
	for _, name := range []string{"cached.txt", "negative.txt"} {
		t.Run(name, func(t *testing.T) {
			root := volumeFixture(t)
			volumeWrite(t, filepath.Join(root, name), strings.Repeat("x", 4097))
			p := &volumeStateProbe{}
			defer p.close()
			if err := p.prime(root, "test"); err == nil {
				t.Fatal("invalid fixture accepted")
			}
		})
	}
}

func TestVolumeStateConcurrentReads(t *testing.T) {
	p := &volumeStateProbe{}
	volumePrime(t, p, volumeFixture(t))
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if w := volumeRequest(p, http.MethodGet, "/volume-state"); w.Code != http.StatusOK {
				t.Errorf("read returned %d", w.Code)
			}
		})
	}
	wg.Wait()
	if got := volumeObserve(t, p); got.Reads != 17 {
		t.Fatalf("reads = %d", got.Reads)
	}
}

func TestVolumeResultClassifiesWrappedIOError(t *testing.T) {
	got := volumeResult(nil, &os.PathError{Op: "read", Path: "unlinked.txt", Err: syscall.EIO})
	if got.ErrorCode != "EIO" || got.Error == "" || got.NotExist {
		t.Fatalf("unexpected observation: %+v", got)
	}
	other := volumeResult(nil, os.ErrPermission)
	if other.ErrorCode != "" {
		t.Fatalf("unrelated error classified as EIO: %+v", other)
	}
}
