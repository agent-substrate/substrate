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
	"crypto/tls"
	"crypto/x509"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"go.ate.dev/substrate/cmd/benchmarking/isolate/internal/fakeworker/fakeworkertest"
	"go.ate.dev/substrate/internal/credbundle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testAteletID     = "spiffe://cluster.local/ns/ate-system/sa/atelet"
	testWorkerSyncID = "spiffe://cluster.local/ns/ate-system/sa/fake-workersync"
)

// stubEpoch is the epoch stubRelay hands out.
const stubEpoch = 9

// stubRelay is a stand-in fake-atelet relay: it records each request and
// answers with err.
type stubRelay struct {
	ateapi.UnimplementedWorkerServiceServer

	mu          sync.Mutex
	acquireGot  []*ateapi.AcquireWorkerEpochRequest
	got         []*ateapi.RegisterWorkerRequest
	runtimesGot []*ateapi.SetWorkerSandboxRuntimesRequest
	err         error
}

func (s *stubRelay) AcquireWorkerEpoch(_ context.Context, in *ateapi.AcquireWorkerEpochRequest) (*ateapi.AcquireWorkerEpochResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acquireGot = append(s.acquireGot, in)
	if s.err != nil {
		return nil, s.err
	}
	return &ateapi.AcquireWorkerEpochResponse{Epoch: stubEpoch}, nil
}

func (s *stubRelay) RegisterWorker(_ context.Context, in *ateapi.RegisterWorkerRequest) (*ateapi.RegisterWorkerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, in)
	if s.err != nil {
		return nil, s.err
	}
	return &ateapi.RegisterWorkerResponse{}, nil
}

func (s *stubRelay) SetWorkerSandboxRuntimes(_ context.Context, in *ateapi.SetWorkerSandboxRuntimesRequest) (*ateapi.SetWorkerSandboxRuntimesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimesGot = append(s.runtimesGot, in)
	if s.err != nil {
		return nil, s.err
	}
	return &ateapi.SetWorkerSandboxRuntimesResponse{}, nil
}

// relayServer serves svc with a certificate for serverID from ca, requiring
// fake-workersync's certificate from ca, and returns its address.
func relayServer(t *testing.T, ca *fakeworkertest.CA, serverID string, svc ateapi.WorkerServiceServer) string {
	t.Helper()
	return relayServerTrusting(t, ca, ca, serverID, svc)
}

// relayServerTrusting is relayServer with its certificate from ca and
// fake-workersync's required to chain to clientCA.
func relayServerTrusting(t *testing.T, ca, clientCA *fakeworkertest.CA, serverID string, svc ateapi.WorkerServiceServer) string {
	t.Helper()
	pem, err := os.ReadFile(clientCA.TrustBundle)
	if err != nil {
		t.Fatal(err)
	}
	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(pem)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		GetCertificate: credbundle.Loader(ca.Issue(t, serverID)),
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      clientCAs,
		VerifyConnection: func(cs tls.ConnectionState) error {
			return fakeworker.VerifyPeerID(cs, testWorkerSyncID)
		},
	})))
	ateapi.RegisterWorkerServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newTestRelay(t *testing.T, ca *fakeworkertest.CA) *grpcRelay {
	t.Helper()
	relay, err := newGRPCRelay(ca.Issue(t, testWorkerSyncID), ca.TrustBundle, testAteletID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(relay.Close)
	return relay
}

func testClasses() []*ateapi.WorkerSandboxClass {
	return []*ateapi.WorkerSandboxClass{{
		SandboxClass: "gvisor",
		DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: &ateapi.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes:    []*ateapi.AttributeEntry{{Key: "architecture", Value: "amd64"}},
		}},
	}}
}

func capacityReport() workerReport {
	return workerReport{
		Name:           "fake-r1-c52b9bd3-0",
		IPs:            []string{"10.0.0.7"},
		Capacity:       &ateapi.WorkerResources{Actors: 1000},
		SandboxClasses: testClasses(),
	}
}

// A Worker with no epoch acquires one, registers with it, then reports its
// runtimes under it, as a worker process does at startup.
func TestRelayClientSendsTheReport(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	svc := &stubRelay{}
	addr := relayServer(t, ca, testAteletID, svc)
	relay := newTestRelay(t, ca)
	for range 2 {
		if err := relay.Report(context.Background(), addr, capacityReport()); err != nil {
			t.Fatalf("Report: %v", err)
		}
	}
	ref := &ateapi.ObjectRef{Name: "fake-r1-c52b9bd3-0"}
	wantAcquire := &ateapi.AcquireWorkerEpochRequest{Worker: ref}
	wantRegister := &ateapi.RegisterWorkerRequest{
		Worker:   ref,
		Epoch:    stubEpoch,
		Ips:      []string{"10.0.0.7"},
		Capacity: &ateapi.WorkerResources{Actors: 1000},
	}
	wantRuntimes := &ateapi.SetWorkerSandboxRuntimesRequest{Worker: ref, Epoch: stubEpoch, SandboxClasses: testClasses()}
	if len(svc.acquireGot) != 2 || !proto.Equal(svc.acquireGot[0], wantAcquire) {
		t.Errorf("relay received acquires %v, want %v twice", svc.acquireGot, wantAcquire)
	}
	if len(svc.got) != 2 || !proto.Equal(svc.got[0], wantRegister) {
		t.Errorf("relay received registrations %v, want %v twice", svc.got, wantRegister)
	}
	if len(svc.runtimesGot) != 2 || !proto.Equal(svc.runtimesGot[0], wantRuntimes) {
		t.Errorf("relay received runtimes %v, want %v twice", svc.runtimesGot, wantRuntimes)
	}
	if len(relay.conns) != 1 {
		t.Errorf("%d connections to one relay, want 1", len(relay.conns))
	}
}

// A Worker that already has a registered epoch registers it again without
// acquiring a new one.
func TestRelayClientReusesTheGivenEpoch(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	svc := &stubRelay{}
	addr := relayServer(t, ca, testAteletID, svc)
	report := capacityReport()
	report.Epoch = 3
	if err := newTestRelay(t, ca).Report(context.Background(), addr, report); err != nil {
		t.Fatalf("Report: %v", err)
	}
	if len(svc.acquireGot) != 0 {
		t.Errorf("relay received acquires %v, want none", svc.acquireGot)
	}
	if len(svc.got) != 1 || svc.got[0].GetEpoch() != 3 {
		t.Errorf("relay received registrations %v, want one with epoch 3", svc.got)
	}
	if len(svc.runtimesGot) != 1 || svc.runtimesGot[0].GetEpoch() != 3 {
		t.Errorf("relay received runtimes %v, want one with epoch 3", svc.runtimesGot)
	}
}

// fake-workersync reports only to the atelet identity: a report sent anywhere
// else would hand a Worker's capacity to a workload that is not its atelet.
func TestRelayClientRefusesAServerThatIsNotAtelet(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	relay := newTestRelay(t, ca)
	impostor := &stubRelay{}
	if err := relay.Report(context.Background(), relayServer(t, ca, "spiffe://cluster.local/ns/benchmarking/sa/default", impostor), capacityReport()); err == nil {
		t.Error("reported to a server with another workload's identity")
	}
	other := fakeworkertest.NewCA(t)
	untrusted := &stubRelay{}
	if err := relay.Report(context.Background(), relayServer(t, other, testAteletID, untrusted), capacityReport()); err == nil {
		t.Error("reported to a server whose certificate chains to an untrusted signer")
	}
	if len(impostor.got)+len(untrusted.got) != 0 {
		t.Error("a refused server received the report")
	}
}

// A pod-identity CA rotation during a long run must not cut the relays off:
// the client reads the trust bundle at each handshake.
func TestRelayClientFollowsATrustBundleRotation(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	relay := newTestRelay(t, ca)
	next := fakeworkertest.NewCA(t)
	// The relay is certified by the new CA and still admits fake-workersync's
	// certificate from the old one.
	addr := relayServerTrusting(t, next, ca, testAteletID, &stubRelay{})
	fakeworkertest.Rotate(t, ca.TrustBundle, next)
	if err := relay.Report(context.Background(), addr, capacityReport()); err != nil {
		t.Errorf("Report to a relay certified by the rotated-in CA: %v", err)
	}
}

// A relay gone from the pod list, as when its fake-atelet restarts under a new
// IP, has its connection closed; the next report to it dials afresh.
func TestRelayClientPrunesGoneRelays(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	kept := relayServer(t, ca, testAteletID, &stubRelay{})
	gone := relayServer(t, ca, testAteletID, &stubRelay{})
	relay := newTestRelay(t, ca)
	for _, addr := range []string{kept, gone} {
		if err := relay.Report(context.Background(), addr, capacityReport()); err != nil {
			t.Fatalf("Report to %s: %v", addr, err)
		}
	}
	relay.Prune(map[string]string{"node-a": kept})
	if _, ok := relay.conns[gone]; ok || len(relay.conns) != 1 {
		t.Errorf("connections after pruning = %v, want only %s", slices.Collect(maps.Keys(relay.conns)), kept)
	}
	if err := relay.Report(context.Background(), gone, capacityReport()); err != nil {
		t.Errorf("Report after pruning: %v", err)
	}
}

// The controller tells a Worker ate-api-server has not seen yet from a
// rejected report by the status code, so the relay's must arrive intact.
func TestRelayClientCarriesAteAPIServersStatus(t *testing.T) {
	ca := fakeworkertest.NewCA(t)
	addr := relayServer(t, ca, testAteletID, &stubRelay{err: status.Error(codes.NotFound, "Worker fake-r1-c52b9bd3-0 not found")})
	err := newTestRelay(t, ca).Report(context.Background(), addr, capacityReport())
	if status.Code(err) != codes.NotFound || !strings.Contains(status.Convert(err).Message(), "not found") {
		t.Errorf("Report = %v, want NotFound with the server's message", err)
	}
}
