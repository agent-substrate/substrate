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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

func workerContext(t *testing.T, podUID string) context.Context {
	t.Helper()
	cert := workerCertificate(t, podUID, "node")
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}})
}

func TestVerifyClientOnSameNode(t *testing.T) {
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerCertificate(t, "worker-uid", "node-a")}}
	nodeA := &substratex509.PodIdentity{NodeName: "node-a", NodeUID: "node-uid"}
	if err := verifyClientOnSameNode(nodeA)(state); err != nil {
		t.Fatalf("same-node worker rejected: %v", err)
	}
	if err := verifyClientOnSameNode(&substratex509.PodIdentity{NodeName: "node-b", NodeUID: "node-uid"})(state); err == nil {
		t.Fatal("cross-node worker accepted")
	}
	if err := verifyClientOnSameNode(&substratex509.PodIdentity{NodeName: "node-a", NodeUID: "replacement-node"})(state); err == nil {
		t.Fatal("replacement node accepted")
	}
}

func workerCertificate(t *testing.T, podUID, nodeName string) *x509.Certificate {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace: "workers", ServiceAccountName: "default", ServiceAccountUID: "sa-uid",
		PodName: "worker", PodUID: podUID, NodeName: nodeName, NodeUID: "node-uid",
	}, template); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// TODO: Use bufconn + an actual server implementation.
//
// https://google.github.io/styleguide/go/best-practices.html#use-real-transports
type fakeWorkerService struct {
	ateapipb.WorkerServiceClient

	got      []*ateapipb.RegisterWorkerRequest
	mintGot  []*ateapipb.MintAteomActorCertificateRequest
	mintResp *ateapipb.MintAteomActorCertificateResponse
	err      error
}

func (s *fakeWorkerService) RegisterWorker(_ context.Context, in *ateapipb.RegisterWorkerRequest, _ ...grpc.CallOption) (*ateapipb.RegisterWorkerResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, in)
	return &ateapipb.RegisterWorkerResponse{}, nil
}

func (s *fakeWorkerService) MintAteomActorCertificate(_ context.Context, in *ateapipb.MintAteomActorCertificateRequest, _ ...grpc.CallOption) (*ateapipb.MintAteomActorCertificateResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.mintGot = append(s.mintGot, in)
	if s.mintResp != nil {
		return s.mintResp, nil
	}
	return &ateapipb.MintAteomActorCertificateResponse{}, nil
}

func TestSetWorkerCapacity(t *testing.T) {
	hw := &ateapipb.HardwareIdentity{Attributes: map[string]string{"architecture": "amd64"}}
	forwarded := func(capacity *ateapipb.WorkerResources) []*ateapipb.RegisterWorkerRequest {
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request; the hardware is atelet's own.
		return []*ateapipb.RegisterWorkerRequest{{Worker: &ateapipb.ObjectRef{Name: "pod-a"}, Capacity: capacity, Hardware: hw}}
	}

	tests := []struct {
		name string
		// unauthenticated drops the peer certificate from the context.
		unauthenticated bool
		// serviceErr is what the control plane answers with.
		serviceErr    error
		req           *ateletpb.SetWorkerCapacityRequest
		wantCode      codes.Code
		wantForwarded []*ateapipb.RegisterWorkerRequest
	}{{
		name: "records what the worker says",
		req: &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Actors: 4, Resources: &ateletpb.Resources{
			Limits: []*ateletpb.Limits{{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "4Gi"}},
		}}},
		wantForwarded: forwarded(&ateapipb.WorkerResources{Actors: 4, Resources: resources.CPUMemory(2000, 4294967296)}),
	}, {
		name:          "omits undetermined compute",
		req:           &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantForwarded: forwarded(&ateapipb.WorkerResources{Actors: 1}),
	}, {
		name:          "forwards empty resources",
		req:           &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Resources: &ateletpb.Resources{}}},
		wantForwarded: forwarded(&ateapipb.WorkerResources{Resources: &ateapipb.Resources{}}),
	}, {
		name: "rejects invalid capacity without forwarding",
		req: &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Resources: &ateletpb.Resources{
			Limits: []*ateletpb.Limits{{Name: "gpu", Quantity: "1"}},
		}}},
		wantCode: codes.InvalidArgument,
	}, {
		name:     "rejects missing capacity without forwarding",
		req:      &ateletpb.SetWorkerCapacityRequest{},
		wantCode: codes.InvalidArgument,
	}, {
		// A worker may report only what its certificate proves it is.
		name:            "requires a certificate",
		unauthenticated: true,
		req:             &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantCode:        codes.Unauthenticated,
	}, {
		// The Worker record may not exist yet. The error must reach the
		// worker so it retries: it reports once, so a swallowed failure
		// leaves the Worker with no capacity forever.
		name:       "surfaces the control plane's rejection",
		serviceErr: errors.New("no such worker"),
		req:        &ateletpb.SetWorkerCapacityRequest{Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantCode:   codes.Unknown,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workers := &fakeWorkerService{err: tt.serviceErr}
			svc := &ateomSupportServer{workers: workers, hardware: hw}
			ctx := workerContext(t, "pod-a")
			if tt.unauthenticated {
				ctx = context.Background()
			}

			_, err := svc.SetWorkerCapacity(ctx, tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("SetWorkerCapacity() code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if diff := cmp.Diff(tt.wantForwarded, workers.got, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type fakeSuspendService struct {
	ateapipb.WorkerServiceClient

	got []*ateapipb.RequestActorSuspendRequest
	err error
}

func (s *fakeSuspendService) RequestActorSuspend(_ context.Context, in *ateapipb.RequestActorSuspendRequest, _ ...grpc.CallOption) (*ateapipb.RequestActorSuspendResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, in)
	return &ateapipb.RequestActorSuspendResponse{}, nil
}

func TestRequestActorSuspendNamesTheCallingWorker(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &ateomSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	if _, err := svc.RequestActorSuspend(ctx, &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	}); err != nil {
		t.Fatalf("RequestActorSuspend() failed: %v", err)
	}

	want := []*ateapipb.RequestActorSuspendRequest{{
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request: a worker may speak for the
		// actors it hosts, and only the control plane knows which those are.
		Worker:   &ateapipb.ObjectRef{Name: "pod-a"},
		Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid: "01234567-89ab-cdef-0123-456789abcdef",
	}}
	if diff := cmp.Diff(want, workers.got, protocmp.Transform()); diff != "" {
		t.Errorf("forwarded request mismatch (-want +got):\n%s", diff)
	}
}

func TestRequestActorSuspendRequiresACertificate(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &ateomSupportServer{workers: workers}

	// No peer identity: there is no worker to attribute this to, and the
	// request names no other way to find one.
	_, err := svc.RequestActorSuspend(context.Background(), &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated request returned %v, want Unauthenticated", err)
	}
	if len(workers.got) != 0 {
		t.Errorf("unauthenticated request was still forwarded: %v", workers.got)
	}
}

// A refusal is an answer, not a transport failure: the worker needs to see
// that the control plane declined so it does not treat the actor as suspended.
func TestRequestActorSuspendSurfacesRefusal(t *testing.T) {
	workers := &fakeSuspendService{err: status.Error(codes.FailedPrecondition, "Actor is RESUMING")}
	svc := &ateomSupportServer{workers: workers}

	_, err := svc.RequestActorSuspend(workerContext(t, "pod-a"), &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Errorf("code = %v (err %v), want FailedPrecondition", got, err)
	}
}
func TestMintActorCertificateForwardsToWorkerService(t *testing.T) {
	wantCerts := [][]byte{[]byte("cert-der-bytes")}
	workers := &fakeWorkerService{
		mintResp: &ateapipb.MintAteomActorCertificateResponse{
			ActorCertificates: wantCerts,
		},
	}
	svc := &ateomSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	resp, err := svc.MintActorCertificate(ctx, &ateletpb.MintActorCertificateRequest{
		ActorAtespace:             "team-a",
		ActorName:                 "actor-1",
		ActorUid:                  "01234567-89ab-cdef-0123-456789abcdef",
		CertificateSigningRequest: []byte("csr-bytes"),
	})
	if err != nil {
		t.Fatalf("MintActorCertificate() failed: %v", err)
	}

	want := []*ateapipb.MintAteomActorCertificateRequest{{
		Actor: &ateapipb.ObjectRef{
			Atespace: "team-a",
			Name:     "actor-1",
		},
		ActorUid:                  "01234567-89ab-cdef-0123-456789abcdef",
		CertificateSigningRequest: []byte("csr-bytes"),
	}}
	if diff := cmp.Diff(want, workers.mintGot, protocmp.Transform()); diff != "" {
		t.Errorf("forwarded mint request mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantCerts, resp.GetActorCertificates()); diff != "" {
		t.Errorf("returned certificates mismatch (-want +got):\n%s", diff)
	}
}

func TestMintActorCertificateRequiresCertificate(t *testing.T) {
	workers := &fakeWorkerService{}
	svc := &ateomSupportServer{workers: workers}

	_, err := svc.MintActorCertificate(context.Background(), &ateletpb.MintActorCertificateRequest{
		ActorAtespace:             "team-a",
		ActorName:                 "actor-1",
		ActorUid:                  "01234567-89ab-cdef-0123-456789abcdef",
		CertificateSigningRequest: []byte("csr-bytes"),
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated request returned %v, want Unauthenticated", err)
	}
	if len(workers.mintGot) != 0 {
		t.Errorf("unauthenticated request still forwarded %v", workers.mintGot)
	}
}
