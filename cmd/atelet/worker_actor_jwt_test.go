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
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchange"
)

// testActor is the actor the transfer tests act for.
var testActor = actorRef{atespace: "ate-demo", name: "bot", uid: "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"}

// fakeJWTMinter answers MintWorkerActorJwt with a fixed token and records
// what it was asked for. A restore fetches its files in parallel, and each
// fetch mints while the cache is cold, so the record is locked.
type fakeJWTMinter struct {
	ateapi.WorkerServiceClient
	jwt    string
	expiry time.Time
	err    error

	mu  sync.Mutex
	got []*ateapi.MintWorkerActorJwtRequest
}

func (m *fakeJWTMinter) MintWorkerActorJwt(_ context.Context, in *ateapi.MintWorkerActorJwtRequest, _ ...grpc.CallOption) (*ateapi.MintWorkerActorJwtResponse, error) {
	m.mu.Lock()
	m.got = append(m.got, in)
	m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	return &ateapi.MintWorkerActorJwtResponse{ActorJwt: m.jwt, ExpireTime: timestamppb.New(m.expiry)}, nil
}

// testWorkerActorJWTs is the JWT source transfer tests give an Atelet.
func testWorkerActorJWTs() *workerActorJWTs {
	return newWorkerActorJWTs(&fakeJWTMinter{jwt: "test-actor-jwt", expiry: time.Now().Add(time.Hour)})
}

func TestWorkerActorJWTs_MintsOncePerActorUntilNearExpiry(t *testing.T) {
	now := time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)
	clock := now
	minter := &fakeJWTMinter{jwt: "jwt-1", expiry: now.Add(time.Hour)}
	j := newWorkerActorJWTs(minter)
	j.now = func() time.Time { return clock }

	for range 2 {
		jwt, err := j.get(t.Context(), testActor)
		if err != nil || jwt != "jwt-1" {
			t.Fatalf("get = %q, %v", jwt, err)
		}
	}
	if len(minter.got) != 1 {
		t.Fatalf("minted %d times, want 1", len(minter.got))
	}
	req := minter.got[0]
	if req.GetActor().GetAtespace() != testActor.atespace || req.GetActor().GetName() != testActor.name || req.GetActorUid() != testActor.uid {
		t.Errorf("mint request names %v uid=%s", req.GetActor(), req.GetActorUid())
	}
	if len(req.GetAudiences()) != 1 || req.GetAudiences()[0] != exchange.ActorJWTAudience {
		t.Errorf("mint audiences = %v, want the exchange's", req.GetAudiences())
	}

	other := testActor
	other.uid = "another-uid"
	if _, err := j.get(t.Context(), other); err != nil {
		t.Fatal(err)
	}
	if len(minter.got) != 2 {
		t.Fatalf("a different actor should mint, minted %d times", len(minter.got))
	}

	// Under the refresh margin the cached JWT is not used.
	clock = now.Add(time.Hour - workerActorJWTRefreshMargin + time.Second)
	minter.jwt = "jwt-2"
	jwt, err := j.get(t.Context(), testActor)
	if err != nil || jwt != "jwt-2" {
		t.Fatalf("get near expiry = %q, %v, want a fresh mint", jwt, err)
	}
}

func TestWorkerActorJWTs_Errors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		minter *fakeJWTMinter
		want   codes.Code
	}{
		// NotFound and Aborted must not reach callers as NotFound, which they
		// read from a transfer as a missing snapshot object.
		{"not this node's actor", &fakeJWTMinter{err: status.Error(codes.NotFound, "actor not found")}, codes.FailedPrecondition},
		{"recreated actor", &fakeJWTMinter{err: status.Error(codes.Aborted, "uid changed")}, codes.FailedPrecondition},
		{"ate-api-server down", &fakeJWTMinter{err: status.Error(codes.Unavailable, "connection refused")}, codes.Unavailable},
		{"ate-api-server refuses atelet", &fakeJWTMinter{err: status.Error(codes.Unauthenticated, "bad cert")}, codes.Internal},
		{"empty token", &fakeJWTMinter{jwt: "", expiry: time.Now().Add(time.Hour)}, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newWorkerActorJWTs(tc.minter).get(t.Context(), testActor)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.want)
			}
		})
	}
	t.Run("no source configured", func(t *testing.T) {
		_, err := (&Atelet{}).workerActorJWT(t.Context(), testActor)
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want Internal", status.Code(err))
		}
	})
}

// recordingNodePlugin captures the transfer requests atelet sends.
type recordingNodePlugin struct {
	objectstore.NodeProviderClient
	fetches []*objectstore.FetchSnapshotRequest
	uploads []*objectstore.UploadSnapshotRequest
}

func (p *recordingNodePlugin) FetchSnapshot(_ context.Context, in *objectstore.FetchSnapshotRequest, _ ...grpc.CallOption) (*objectstore.FetchSnapshotResponse, error) {
	p.fetches = append(p.fetches, in)
	return &objectstore.FetchSnapshotResponse{}, nil
}

func (p *recordingNodePlugin) UploadSnapshot(_ context.Context, in *objectstore.UploadSnapshotRequest, _ ...grpc.CallOption) (*objectstore.UploadSnapshotResponse, error) {
	p.uploads = append(p.uploads, in)
	return &objectstore.UploadSnapshotResponse{}, nil
}

func TestTransfersCarryTheActorJWT(t *testing.T) {
	plugin := &recordingNodePlugin{}
	s := &Atelet{objectStorePlugin: plugin, workerActorJWTs: testWorkerActorJWTs()}
	if err := s.fetchSnapshotFiles(t.Context(), testActor, pausedSnapshotURI, t.TempDir(), []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.uploadSnapshotFiles(t.Context(), testActor, pausedSnapshotURI, t.TempDir(), []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if len(plugin.fetches) != 1 || plugin.fetches[0].GetActorJwt() != "test-actor-jwt" {
		t.Errorf("fetch carried actor_jwt %q", plugin.fetches[0].GetActorJwt())
	}
	if len(plugin.uploads) != 1 || plugin.uploads[0].GetActorJwt() != "test-actor-jwt" {
		t.Errorf("upload carried actor_jwt %q", plugin.uploads[0].GetActorJwt())
	}
}

func TestTransfersStopWhenTheMintFails(t *testing.T) {
	plugin := &recordingNodePlugin{}
	s := &Atelet{objectStorePlugin: plugin, workerActorJWTs: newWorkerActorJWTs(&fakeJWTMinter{err: status.Error(codes.NotFound, "actor not found")})}
	err := s.fetchSnapshotFiles(t.Context(), testActor, pausedSnapshotURI, t.TempDir(), []string{"a"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want FailedPrecondition for an actor this node cannot transfer for", err)
	}
	if len(plugin.fetches) != 0 {
		t.Fatal("the plugin was called without a JWT")
	}
}
