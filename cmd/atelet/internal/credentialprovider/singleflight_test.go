// Copyright 2026 The Agent Substrate Authors
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

package credentialprovider

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
)

func TestKeychainDoesNotCacheEmptyOrBlankPasswordAuth(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		badResponse  string
		goodResponse string
	}{
		{
			name: "empty auth map is not cached",
			badResponse: `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Global",
  "auth": {}
}`,
			goodResponse: `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Global",
  "auth": {"gcr.io": {"username": "_token", "password": "recovered"}}
}`,
		},
		{
			name: "blank password entry is ignored and not cached",
			badResponse: `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Global",
  "auth": {"gcr.io": {"username": "_token", "password": ""}}
}`,
			goodResponse: `{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Global",
  "auth": {"gcr.io": {"username": "_token", "password": "recovered"}}
}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			requests := fakePlugin(t, dir, "fake-provider", tc.badResponse)
			kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			auth, err := kc.Resolve(repo(t, "gcr.io/proj/img:latest"))
			if err != nil {
				t.Fatalf("first Resolve: %v", err)
			}
			if auth != authn.Anonymous {
				t.Fatalf("first Resolve = %v, want authn.Anonymous", auth)
			}

			// Once the upstream token service recovers, the very next pull must
			// re-invoke the plugin instead of serving a poisoned 1m cache entry.
			if err := os.WriteFile(filepath.Join(dir, "fake-provider.response"), []byte(tc.goodResponse), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
			if got.Password != "recovered" {
				t.Fatalf("second Resolve password = %q, want %q", got.Password, "recovered")
			}
			if n := countRequests(t, requests); n != 2 {
				t.Fatalf("plugin ran %d times, want 2", n)
			}
		})
	}
}

func TestKeychainRetriesTransientThrottleEmptyAuth(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fake-provider.response"), []byte(`{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "_token", "password": "after-429"}}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPlugin(t, dir, "fake-provider", "flaky-sts-provider.sh")

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img:latest")
	if got.Password != "after-429" {
		t.Fatalf("Resolve password = %q, want %q", got.Password, "after-429")
	}
	if n := countRequests(t, filepath.Join(dir, "fake-provider.requests")); n != 3 {
		t.Fatalf("plugin ran %d times, want 3 (2 throttled + 1 recovered)", n)
	}
}

func TestIsTransientThrottleOutput(t *testing.T) {
	t.Parallel()
	for _, msg := range []string{
		"HTTP/1.1 429 Too Many Requests",
		"googleapi: Error 429: Rate Limit Exceeded, rateLimitExceeded",
		"status code: 503",
		"upstream returned HTTP 502 Bad Gateway",
		"rpc error: code = ResourceExhausted desc = QUOTA_EXCEEDED",
		"net/http: TLS handshake timeout",
	} {
		if !isTransientThrottleOutput(msg) {
			t.Errorf("isTransientThrottleOutput(%q) = false, want true", msg)
		}
	}

	for _, msg := range []string{
		"",
		"E1112 14:05:02.500123 12503 provider.go:42] unauthorized for localhost:5000/repo/img",
		"permission denied on project 1234503999",
		"HTTP/1.1 403 Forbidden",
		"invalid configuration at line 4290",
	} {
		if isTransientThrottleOutput(msg) {
			t.Errorf("isTransientThrottleOutput(%q) = true, want false", msg)
		}
	}
}

func TestKeychainSingleflightCoalescesColdMissesAndSurvivesSingleCallerCancel(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	enteredFIFO := filepath.Join(dir, "fake-provider.entered")
	gateFIFO := filepath.Join(dir, "fake-provider.gate")
	for _, p := range []string{enteredFIFO, gateFIFO} {
		if err := syscall.Mkfifo(p, 0o600); err != nil {
			t.Fatalf("Mkfifo(%q): %v", p, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "fake-provider.response"), []byte(`{
  "kind": "CredentialProviderResponse",
  "apiVersion": "credentialprovider.kubelet.k8s.io/v1",
  "cacheKeyType": "Registry",
  "auth": {"gcr.io": {"username": "_token", "password": "coalesced"}}
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPlugin(t, dir, "fake-provider", "gated-provider.sh")

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctxCancel, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()

	cancelErrCh := make(chan error, 1)
	go func() {
		_, err := kc.ResolveContext(ctxCancel, repo(t, "gcr.io/proj/img-cancel:latest"))
		cancelErrCh <- err
	}()

	enteredFile, err := os.OpenFile(enteredFIFO, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open entered FIFO: %v", err)
	}
	buf := make([]byte, 16)
	if _, err := enteredFile.Read(buf); err != nil {
		t.Fatalf("read entered FIFO: %v", err)
	}
	_ = enteredFile.Close()

	// Attach 8 live callers across distinct repositories on the same registry
	// before cancelling the first caller.
	const liveCallers = 8
	var wg sync.WaitGroup
	wg.Add(liveCallers)
	livePasswords := make([]string, liveCallers)
	liveErrs := make([]error, liveCallers)
	for i := range liveCallers {
		go func(idx int) {
			defer wg.Done()
			auth, err := kc.ResolveContext(t.Context(), repo(t, fmt.Sprintf("gcr.io/proj/img-%d:latest", idx)))
			if err != nil {
				liveErrs[idx] = err
				return
			}
			cfg, err := auth.Authorization()
			liveErrs[idx] = err
			if cfg != nil {
				livePasswords[idx] = cfg.Password
			}
		}(i)
	}

	// Wait until all 8 live joiners are registered on the flight alongside the
	// leader, then cancel the leader's context: the shared execution must keep
	// running for all 8 joiners.
	p := kc.plugins[0]
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		var w int
		if f := p.flights["reg:gcr.io"]; f != nil {
			w = f.waiters
		}
		p.mu.Unlock()
		if w == 1+liveCallers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waiters on flight, last=%d", 1+liveCallers, w)
		}
		time.Sleep(time.Millisecond)
	}

	cancelFirst()
	if err := <-cancelErrCh; err == nil {
		t.Fatal("cancelled caller got nil error")
	}

	gateFile, err := os.OpenFile(gateFIFO, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open gate FIFO: %v", err)
	}
	if _, err := gateFile.WriteString("go\n"); err != nil {
		t.Fatalf("write gate FIFO: %v", err)
	}
	_ = gateFile.Close()

	wg.Wait()
	for i := range liveCallers {
		if liveErrs[i] != nil || livePasswords[i] != "coalesced" {
			t.Fatalf("live caller %d: password=%q err=%v, want coalesced", i, livePasswords[i], liveErrs[i])
		}
	}
	if n := countRequests(t, filepath.Join(dir, "fake-provider.requests")); n != 1 {
		t.Fatalf("plugin ran %d times across 9 concurrent cold pulls, want 1", n)
	}
}

func TestKeychainAbandonedFlightDoesNotPoisonNextCaller(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	// The first plugin run lingers after it is killed: its background sleep
	// holds stdout open until exec.Cmd.WaitDelay expires, so the abandoned
	// flight is still running when caller 2 arrives.
	linkPlugin(t, dir, "fake-provider", "lingering-provider.sh")
	enteredFile := filepath.Join(dir, "fake-provider.entered")

	kc, err := New(writeConfig(t, dir, gcpLikeConfig), dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(t.Context())
	err1Ch := make(chan error, 1)
	go func() {
		_, err := kc.ResolveContext(ctx1, repo(t, "gcr.io/proj/img-1:latest"))
		err1Ch <- err
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(enteredFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first flight to start")
		}
		time.Sleep(time.Millisecond)
	}

	// Cancel the sole waiter: releaseWaiter must delete p.flights["reg:gcr.io"]
	// under p.mu so caller 2 starts a fresh flight while the first plugin's
	// pipes are still held open.
	cancel1()
	if err := <-err1Ch; err == nil {
		t.Fatal("expected cancelled error for caller 1")
	}

	got := resolvedAuth(t, kc, "gcr.io/proj/img-2:latest")
	if got.Password != "fresh-flight-token" {
		t.Fatalf("caller 2 password = %q, want %q", got.Password, "fresh-flight-token")
	}
}
