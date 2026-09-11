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

package egress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/egresspolicy"
	"github.com/agent-substrate/substrate/internal/resources"
)

var testActorRef = resources.ActorRef{Atespace: testEgressAtespace, Name: testEgressActor}

// newTestCache builds a cache over client with a clock the test advances.
func newTestCache(client *egressMockClient, ttl time.Duration) (*policyCache, *time.Time) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	c := newPolicyCache(client, ttl)
	c.now = func() time.Time { return now }
	return c, &now
}

func TestPolicyCacheServesFromCacheWithinTTL(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy()}
	c, now := newTestCache(client, 10*time.Second)

	for range 3 {
		if _, err := c.get(context.Background(), testActorRef); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if calls := client.policyCalls.Load(); calls != 1 {
		t.Fatalf("GetActorEgressPolicy calls = %d, want 1 within the TTL", calls)
	}

	*now = now.Add(10*time.Second + time.Millisecond)
	if _, err := c.get(context.Background(), testActorRef); err != nil {
		t.Fatalf("get after expiry: %v", err)
	}
	if calls := client.policyCalls.Load(); calls != 2 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 2 after the TTL elapsed", calls)
	}
}

// "No policy" is cached exactly like a policy.
func TestPolicyCacheCachesNoPolicy(t *testing.T) {
	client := &egressMockClient{}
	c, _ := newTestCache(client, 10*time.Second)

	for range 3 {
		if _, err := c.get(context.Background(), testActorRef); !errors.Is(err, errNoPolicy) {
			t.Fatalf("get = %v, want errNoPolicy", err)
		}
	}
	if calls := client.policyCalls.Load(); calls != 1 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 1", calls)
	}
}

// A failed fetch answers this request and no other: the next one tries again.
func TestPolicyCacheDoesNotCacheFetchErrors(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy(), policyErr: status.Error(codes.Unavailable, "ateapi is down")}
	c, _ := newTestCache(client, 10*time.Second)

	if _, err := c.get(context.Background(), testActorRef); status.Code(errors.Unwrap(err)) != codes.Unavailable {
		t.Fatalf("get = %v, want the Unavailable fetch error", err)
	}
	client.policyErr = nil
	if _, err := c.get(context.Background(), testActorRef); err != nil {
		t.Fatalf("get after ateapi recovered: %v", err)
	}
	if calls := client.policyCalls.Load(); calls != 2 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 2", calls)
	}
}

func TestPolicyCacheDisabledFetchesEveryTime(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy()}
	c, _ := newTestCache(client, 0)

	for range 3 {
		if _, err := c.get(context.Background(), testActorRef); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if calls := client.policyCalls.Load(); calls != 3 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 3 with the cache disabled", calls)
	}
}

// Concurrent callers on a cold entry share one fetch.
func TestPolicyCacheCollapsesConcurrentFetches(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy(), policyGate: make(chan struct{})}
	c, _ := newTestCache(client, 10*time.Second)

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.get(context.Background(), testActorRef)
			errs <- err
		}()
	}
	// Wait for the leader to be inside the fetch before releasing it, so every
	// other caller has had the chance to join rather than start its own.
	deadline := time.Now().Add(5 * time.Second)
	for client.policyCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no fetch started")
		}
		time.Sleep(time.Millisecond)
	}
	close(client.policyGate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("get: %v", err)
		}
	}
	if calls := client.policyCalls.Load(); calls != 1 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 1 for %d concurrent callers", calls, callers)
	}
}

// The leader's cancellation must not fail the callers that joined its fetch,
// and the fetch it started still lands in the cache.
func TestPolicyCacheFetchOutlivesCanceledCaller(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy(), policyGate: make(chan struct{})}
	c, _ := newTestCache(client, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.get(ctx, testActorRef)
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for client.policyCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no fetch started")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller got %v, want context.Canceled", err)
	}

	close(client.policyGate)
	if _, err := c.get(context.Background(), testActorRef); err != nil {
		t.Fatalf("get after the detached fetch completed: %v", err)
	}
	if calls := client.policyCalls.Load(); calls != 1 {
		t.Errorf("GetActorEgressPolicy calls = %d, want 1: the canceled caller's fetch should have been reused", calls)
	}
}

// An entry is stale the instant its TTL has elapsed, not a tick later, and a
// refetch after expiry returns the policy as it is now, not as it was.
func TestPolicyCacheExpiryBoundaryAndUpdateVisibility(t *testing.T) {
	client := &egressMockClient{policy: hostnamesPolicy("old.example")}
	c, now := newTestCache(client, 10*time.Second)

	before, err := c.get(context.Background(), testActorRef)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	client.policy = hostnamesPolicy("new.example")

	*now = now.Add(10*time.Second - time.Nanosecond)
	if p, _ := c.get(context.Background(), testActorRef); p != before {
		t.Fatal("entry refetched before its TTL elapsed")
	}
	*now = now.Add(time.Nanosecond)
	after, err := c.get(context.Background(), testActorRef)
	if err != nil {
		t.Fatalf("get after expiry: %v", err)
	}
	if after == before || !after.Evaluate(egresspolicy.Destination{Hostname: "new.example"}).Allowed {
		t.Error("the refetch did not pick up the updated policy")
	}
}

// Expired entries are removed once the map is large enough to matter, so a
// gateway that has seen many actors does not keep a policy for each forever.
func TestPolicyCacheRemovesExpiredEntries(t *testing.T) {
	client := &egressMockClient{policy: allowAllPolicy()}
	c, now := newTestCache(client, 10*time.Second)

	for i := range minCleanupSize {
		ref := resources.ActorRef{Atespace: "a", Name: fmt.Sprintf("actor-%d", i)}
		if _, err := c.get(context.Background(), ref); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	// The next cleanup runs when the map has doubled; until then the expired
	// entries stay, and at that point all of them go.
	*now = now.Add(11 * time.Second)
	for i := range minCleanupSize {
		ref := resources.ActorRef{Atespace: "b", Name: fmt.Sprintf("actor-%d", i)}
		if _, err := c.get(context.Background(), ref); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	c.mu.Lock()
	n := len(c.entries)
	c.mu.Unlock()
	if n != minCleanupSize {
		t.Errorf("cache holds %d entries, want only the %d live ones after the cleanup", n, minCleanupSize)
	}
}
