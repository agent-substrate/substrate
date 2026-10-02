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
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func jwtSource(lifetime int64, audiences ...string) *ateapipb.ActorJWTSource {
	return &ateapipb.ActorJWTSource{Audiences: audiences, ExpirationSeconds: lifetime}
}

// getJWT calls c.get and fails the test unless it returns want.
func getJWT(t *testing.T, c *actorJWTs, ref resources.ActorRef, src *ateapipb.ActorJWTSource, want string) {
	t.Helper()
	got, err := c.get(context.Background(), ref, src)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != want {
		t.Fatalf("get = %q, want %q", got, want)
	}
}

func TestActorJWTsReuseTokenUntilAThirdOfItsLifetimeRemains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &egressMockClient{}
		c := newActorJWTs(client, time.Hour)
		c.recordUID(testActorRef, testEgressActorUID)
		src := jwtSource(900, "https://b.example", "https://a.example")

		getJWT(t, c, testActorRef, src, "jwt-1")
		req := client.lastMint.Load()
		if req.GetActor().GetAtespace() != testEgressAtespace || req.GetActor().GetName() != testEgressActor ||
			req.GetActorUid() != testEgressActorUID || req.GetExpirationSeconds() != 900 ||
			!slices.Equal(req.GetAudience(), []string{"https://a.example", "https://b.example"}) {
			t.Errorf("MintActorJWT request = %v", req)
		}

		time.Sleep(600*time.Second - time.Nanosecond)
		getJWT(t, c, testActorRef, src, "jwt-1")
		time.Sleep(time.Nanosecond)
		getJWT(t, c, testActorRef, src, "jwt-2")
		if calls := client.actorCalls.Load(); calls != 0 {
			t.Errorf("GetActor calls = %d, want 0 with the UID recorded", calls)
		}
	})
}

func TestActorJWTsKeyOnUIDAudiencesAndLifetime(t *testing.T) {
	client := &egressMockClient{}
	c := newActorJWTs(client, time.Hour)
	c.recordUID(testActorRef, testEgressActorUID)

	getJWT(t, c, testActorRef, jwtSource(900, "a", "b"), "jwt-1")
	getJWT(t, c, testActorRef, jwtSource(900, "b", "a"), "jwt-1")
	getJWT(t, c, testActorRef, jwtSource(900, "a"), "jwt-2")
	getJWT(t, c, testActorRef, jwtSource(600, "a", "b"), "jwt-3")

	const recreatedUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	c.recordUID(testActorRef, recreatedUID)
	getJWT(t, c, testActorRef, jwtSource(900, "a", "b"), "jwt-4")
	if uid := client.lastMint.Load().GetActorUid(); uid != recreatedUID {
		t.Errorf("minted for UID %q, want the recreated actor's %q", uid, recreatedUID)
	}
}

// Without a recorded UID, the first mint looks it up and records it.
func TestActorJWTsLookUpUnrecordedUID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &egressMockClient{actor: runningActor()}
		c := newActorJWTs(client, 10*time.Second)
		src := jwtSource(900, "a")

		getJWT(t, c, testActorRef, src, "jwt-1")
		getJWT(t, c, testActorRef, src, "jwt-1")
		if calls := client.actorCalls.Load(); calls != 1 {
			t.Errorf("GetActor calls = %d, want 1 within the UID TTL", calls)
		}
		if uid := client.lastMint.Load().GetActorUid(); uid != testEgressActorUID {
			t.Errorf("minted for UID %q, want %q", uid, testEgressActorUID)
		}

		// Once the recorded UID expires it is looked up again, and the token
		// minted for it is still reused.
		time.Sleep(10 * time.Second)
		getJWT(t, c, testActorRef, src, "jwt-1")
		if calls := client.actorCalls.Load(); calls != 2 {
			t.Errorf("GetActor calls = %d, want 2 after the UID TTL", calls)
		}
	})
}

func TestActorJWTsDoNotCacheErrors(t *testing.T) {
	client := &egressMockClient{err: status.Error(codes.NotFound, "no such actor")}
	c := newActorJWTs(client, 10*time.Second)
	src := jwtSource(900, "a")

	if _, err := c.get(context.Background(), testActorRef, src); status.Code(err) != codes.NotFound {
		t.Fatalf("get = %v, want the NotFound GetActor error", err)
	}
	client.err, client.actor = nil, runningActor()
	client.mintErr = status.Error(codes.Unavailable, "ateapi is down")
	if _, err := c.get(context.Background(), testActorRef, src); status.Code(err) != codes.Unavailable {
		t.Fatalf("get = %v, want the Unavailable MintActorJWT error", err)
	}
	client.mintErr = nil
	getJWT(t, c, testActorRef, src, "jwt-2")
}

func TestActorJWTsCollapseConcurrentMints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &egressMockClient{mintGate: make(chan struct{})}
		c := newActorJWTs(client, time.Hour)
		c.recordUID(testActorRef, testEgressActorUID)

		const callers = 8
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				if _, err := c.get(context.Background(), testActorRef, jwtSource(900, "a")); err != nil {
					t.Errorf("get: %v", err)
				}
			})
		}
		synctest.Wait()
		close(client.mintGate)
		wg.Wait()
		if calls := client.mintCalls.Load(); calls != 1 {
			t.Errorf("MintActorJWT calls = %d, want 1 for %d concurrent callers", calls, callers)
		}
	})
}

// The leader's cancellation must not fail the mint it started, and the token
// still lands in the cache.
func TestActorJWTsMintOutlivesCanceledCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &egressMockClient{mintGate: make(chan struct{})}
		c := newActorJWTs(client, time.Hour)
		c.recordUID(testActorRef, testEgressActorUID)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.get(ctx, testActorRef, jwtSource(900, "a"))
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller got %v, want context.Canceled", err)
		}

		close(client.mintGate)
		synctest.Wait()
		getJWT(t, c, testActorRef, jwtSource(900, "a"), "jwt-1")
		if calls := client.mintCalls.Load(); calls != 1 {
			t.Errorf("MintActorJWT calls = %d, want 1", calls)
		}
	})
}

func TestConnectRecordsActorUID(t *testing.T) {
	ca := newTestCA(t, "actor-identity-ca")
	leaf := ca.issueActorCert(t, "spiffe://substrate-actor.local/ateom-for-actor/"+testEgressAtespace+"/"+testEgressActor, actorCertOptions{})
	client := &egressMockClient{actor: runningActor(), policy: allowAllPolicy()}
	h := New(client, ca.roots(), DefaultPolicyCacheTTL, nil, "")

	if _, err := h.HandleRequestHeaders(context.Background(), egressMetadata(xfccHeader(leaf))); err != nil {
		t.Fatalf("CONNECT: %v", err)
	}
	if _, err := h.actorJWTs.get(context.Background(), testActorRef, jwtSource(900, "a")); err != nil {
		t.Fatalf("get: %v", err)
	}
	if calls := client.actorCalls.Load(); calls != 1 {
		t.Errorf("GetActor calls = %d, want only the CONNECT's", calls)
	}
	if uid := client.lastMint.Load().GetActorUid(); uid != testEgressActorUID {
		t.Errorf("minted for UID %q, want %q", uid, testEgressActorUID)
	}
}
