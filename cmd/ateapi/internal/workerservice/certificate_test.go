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

package workerservice

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeActorStore struct {
	store.Interface
	actors map[resources.ActorRef]*ateapipb.Actor
}

func (f *fakeActorStore) GetActor(_ context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	a, ok := f.actors[actorRef]
	if !ok {
		return nil, store.ErrNotFound
	}
	return a, nil
}

func newFakeStoreWithActor(atespace, name, uid string) (*fakeActorStore, *ateapipb.Actor) {
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: atespace,
			Name:     name,
			Uid:      uid,
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     "template-1",
		},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
		},
	}
	return &fakeActorStore{
		actors: map[resources.ActorRef]*ateapipb.Actor{
			{Atespace: atespace, Name: name}: actor,
		},
	}, actor
}

func generateTestCSR(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return csr, key
}

func newTestCAPool(t *testing.T) *localca.ConcretePool {
	t.Helper()
	ca, err := localca.GenerateCA("1", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	return &localca.ConcretePool{
		CAs:              []*localca.CA{ca},
		ActiveForSigning: "1",
	}
}

func TestMintAteomActorCertificate(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	caPool := newTestCAPool(t)
	s := New(st, caPool)

	csr, key := generateTestCSR(t)

	ctx := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	resp, err := s.MintAteomActorCertificate(ctx, &ateapipb.MintAteomActorCertificateRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: "team-a",
			Name:     "my-actor",
		},
		ActorUid:                  actor.GetMetadata().GetUid(),
		CertificateSigningRequest: csr,
	})
	if err != nil {
		t.Fatalf("MintAteomActorCertificate() failed: %v", err)
	}

	chain := resp.GetActorCertificates()
	if len(chain) == 0 {
		t.Fatal("MintAteomActorCertificate() returned empty chain")
	}

	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}
	if !key.PublicKey.Equal(leaf.PublicKey) {
		t.Error("leaf public key does not match CSR key")
	}

	wantURI := "spiffe://substrate-actor.local/ateom/actor/team-a/my-actor"
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != wantURI {
		t.Errorf("leaf URIs = %v, want [%s]", leaf.URIs, wantURI)
	}

	identity, err := substratex509.ActorIdentityFromCertificate(leaf)
	if err != nil {
		t.Fatalf("ActorIdentityFromCertificate: %v", err)
	}
	if identity != nil {
		t.Errorf("ActorIdentity = %+v, want nil (ateom certificates should not carry ActorIdentity)", identity)
	}
}

func TestMintAteomActorCertificate_Errors(t *testing.T) {
	st, actor := newFakeStoreWithActor("team-a", "my-actor", "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21")
	caPool := newTestCAPool(t)
	s := New(st, caPool)

	csr, _ := generateTestCSR(t)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.MintAteomActorCertificateRequest
		want codes.Code
	}{
		{
			name: "unauthenticated",
			ctx:  ateletauthtest.ContextWith(nil),
			req: &ateapipb.MintAteomActorCertificateRequest{
				Actor:                     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
				ActorUid:                  actor.GetMetadata().GetUid(),
				CertificateSigningRequest: csr,
			},
			want: codes.Unauthenticated,
		},
		{
			name: "missing actor",
			ctx:  authed,
			req: &ateapipb.MintAteomActorCertificateRequest{
				ActorUid:                  actor.GetMetadata().GetUid(),
				CertificateSigningRequest: csr,
			},
			want: codes.InvalidArgument,
		},
		{
			name: "missing actor uid",
			ctx:  authed,
			req: &ateapipb.MintAteomActorCertificateRequest{
				Actor:                     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
				CertificateSigningRequest: csr,
			},
			want: codes.InvalidArgument,
		},
		{
			name: "missing csr",
			ctx:  authed,
			req: &ateapipb.MintAteomActorCertificateRequest{
				Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
				ActorUid: actor.GetMetadata().GetUid(),
			},
			want: codes.InvalidArgument,
		},
		{
			name: "actor not found",
			ctx:  authed,
			req: &ateapipb.MintAteomActorCertificateRequest{
				Actor:                     &ateapipb.ObjectRef{Atespace: "team-a", Name: "nonexistent"},
				ActorUid:                  actor.GetMetadata().GetUid(),
				CertificateSigningRequest: csr,
			},
			want: codes.NotFound,
		},
		{
			name: "actor uid mismatch",
			ctx:  authed,
			req: &ateapipb.MintAteomActorCertificateRequest{
				Actor:                     &ateapipb.ObjectRef{Atespace: "team-a", Name: "my-actor"},
				ActorUid:                  "00000000-0000-0000-0000-000000000000",
				CertificateSigningRequest: csr,
			},
			want: codes.Aborted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.MintAteomActorCertificate(tc.ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}
