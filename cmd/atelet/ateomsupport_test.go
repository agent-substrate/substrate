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

	got []*ateapipb.SetWorkerCapacityRequest
	err error
}

func (s *fakeWorkerService) SetWorkerCapacity(_ context.Context, in *ateapipb.SetWorkerCapacityRequest, _ ...grpc.CallOption) (*ateapipb.SetWorkerCapacityResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, in)
	return &ateapipb.SetWorkerCapacityResponse{}, nil
}

func TestSetWorkerCapacityRecordsWhatTheWorkerSays(t *testing.T) {
	workers := &fakeWorkerService{}
	svc := &ateomSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	reported := &ateapipb.WorkerResources{
		Actors:    4,
		Resources: resources.CPUMemory(2000, 4294967296),
	}
	if _, err := svc.SetWorkerCapacity(ctx, &ateletpb.SetWorkerCapacityRequest{
		Capacity: reported,
	}); err != nil {
		t.Fatalf("SetWorkerCapacity() failed: %v", err)
	}

	want := []*ateapipb.SetWorkerCapacityRequest{{
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request.
		Worker:   &ateapipb.ObjectRef{Name: "pod-a"},
		Capacity: reported,
	}}
	if diff := cmp.Diff(want, workers.got, protocmp.Transform()); diff != "" {
		t.Errorf("recorded capacity mismatch (-want +got):\n%s", diff)
	}
}

func TestSetWorkerCapacityOmitsUndeterminedCompute(t *testing.T) {
	workers := &fakeWorkerService{}
	svc := &ateomSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	if _, err := svc.SetWorkerCapacity(ctx, &ateletpb.SetWorkerCapacityRequest{Capacity: &ateapipb.WorkerResources{Actors: 1}}); err != nil {
		t.Fatalf("SetWorkerCapacity() failed: %v", err)
	}

	if got := workers.got[0].GetCapacity().GetResources(); got != nil {
		t.Errorf("compute the worker could not determine was recorded as %v, want none", got)
	}
}

func TestSetWorkerCapacityRequiresACertificate(t *testing.T) {
	workers := &fakeWorkerService{}
	svc := &ateomSupportServer{workers: workers}

	// No peer identity: a worker may report only what its certificate proves
	// it is, so there is nothing to attribute this to.
	_, err := svc.SetWorkerCapacity(context.Background(), &ateletpb.SetWorkerCapacityRequest{Capacity: &ateapipb.WorkerResources{Actors: 1}})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated report returned %v, want Unauthenticated", err)
	}
	if len(workers.got) != 0 {
		t.Errorf("unauthenticated report still recorded %v", workers.got)
	}
}

func TestSetWorkerCapacitySurfacesRejection(t *testing.T) {
	// The Worker record may not exist yet. The error must reach the worker so
	// it retries: it reports once, so a swallowed failure leaves the Worker
	// with no capacity forever.
	workers := &fakeWorkerService{err: errors.New("no such worker")}
	svc := &ateomSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	if _, err := svc.SetWorkerCapacity(ctx, &ateletpb.SetWorkerCapacityRequest{Capacity: &ateapipb.WorkerResources{Actors: 1}}); err == nil {
		t.Error("a rejected report returned success, so the worker would not retry")
	}
}
