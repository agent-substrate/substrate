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

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker/fakeworkertest"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	testAteletID     = "spiffe://cluster.local/ns/ate-system/sa/atelet"
	testWorkerSyncID = "spiffe://cluster.local/ns/benchmark-workloads/sa/fake-workersync"
)

// stubRelay is a stand-in fake-atelet relay: it records each report and
// answers with err.
type stubRelay struct {
	ateapipb.UnimplementedWorkerServiceServer

	mu  sync.Mutex
	got []*ateapipb.RegisterWorkerRequest
	err error
}

func (s *stubRelay) RegisterWorker(_ context.Context, in *ateapipb.RegisterWorkerRequest) (*ateapipb.RegisterWorkerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, in)
	if s.err != nil {
		return nil, s.err
	}
	return &ateapipb.RegisterWorkerResponse{}, nil
}

// relayServer serves svc with a certificate for serverID from ca, requiring
// fake-workersync's certificate from ca, and returns its address.
func relayServer(t *testing.T, ca *fakeworkertest.CA, serverID string, svc ateapipb.WorkerServiceServer) string {
	t.Helper()
	return relayServerTrusting(t, ca, ca, serverID, svc)
}

// relayServerTrusting is relayServer with its certificate from ca and
// fake-workersync's required to chain to clientCA.
func relayServerTrusting(t *testing.T, ca, clientCA *fakeworkertest.CA, serverID string, svc ateapipb.WorkerServiceServer) string {
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
	ateapipb.RegisterWorkerServiceServer(srv, svc)
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

func capacityReport() *ateapipb.RegisterWorkerRequest {
	return &ateapipb.RegisterWorkerRequest{
		Worker:   &ateapipb.ObjectRef{Name: "fake-r1-c52b9bd3-0"},
		Capacity: &ateapipb.WorkerResources{Actors: 1000},
	}
}

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
	if len(svc.got) != 2 || !proto.Equal(svc.got[0], capacityReport()) {
		t.Errorf("relay received %v, want %v twice", svc.got, capacityReport())
	}
	if len(relay.conns) != 1 {
		t.Errorf("%d connections to one relay, want 1", len(relay.conns))
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
	t.Cleanup(credbundle.SetRecheckIntervalForTesting(0))
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
