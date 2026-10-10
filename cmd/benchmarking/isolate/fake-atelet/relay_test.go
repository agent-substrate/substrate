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
	"net"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker/fakeworkertest"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordingWorkerService records the last report and answers with err.
type recordingWorkerService struct {
	got *ateapipb.RegisterWorkerRequest
	err error
}

func (f *recordingWorkerService) RegisterWorker(_ context.Context, in *ateapipb.RegisterWorkerRequest, _ ...grpc.CallOption) (*ateapipb.RegisterWorkerResponse, error) {
	f.got = in
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.RegisterWorkerResponse{}, nil
}

func report() *ateapipb.RegisterWorkerRequest {
	return &ateapipb.RegisterWorkerRequest{
		Worker: &ateapipb.ObjectRef{Name: "fake-r1-c52b9bd3-0"},
		Capacity: &ateapipb.WorkerResources{
			Actors: 1000,
			Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "64"},
				{Name: "memory", Quantity: "256Gi"},
			}},
		},
	}
}

func TestRelayForwardsTheReportUnchanged(t *testing.T) {
	svc := &recordingWorkerService{}
	if _, err := (&relay{client: svc}).RegisterWorker(context.Background(), report()); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if !proto.Equal(svc.got, report()) {
		t.Errorf("forwarded %v, want %v", svc.got, report())
	}
}

// fake-workersync retries a Worker ate-api-server has not seen yet and gives up
// on a rejected report, so the relay must keep ate-api-server's status.
func TestRelayPassesThroughAteAPIServersStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.InvalidArgument, codes.PermissionDenied, codes.Unavailable, codes.Internal} {
		svc := &recordingWorkerService{err: status.Error(code, "from ate-api-server")}
		_, err := (&relay{client: svc}).RegisterWorker(context.Background(), report())
		if got := status.Code(err); got != code {
			t.Errorf("%v: code = %v", code, got)
		}
		if !strings.Contains(status.Convert(err).Message(), "from ate-api-server") {
			t.Errorf("%v: %v does not carry ate-api-server's message", code, err)
		}
	}
}

// The relay calls ate-api-server under atelet's identity, so it must forward
// nothing but capacity reports: a minted actor certificate, say, would hand
// fake-workersync an identity it does not hold.
func TestRelayServesOnlyCapacityReports(t *testing.T) {
	_, err := (&relay{client: &recordingWorkerService{}}).MintAteomActorCertificate(context.Background(), &ateapipb.MintAteomActorCertificateRequest{})
	if got := status.Code(err); got != codes.Unimplemented {
		t.Errorf("MintAteomActorCertificate code = %v, want Unimplemented", got)
	}
}

// The relay admits fake-workersync and no one else: any other workload with a
// pod certificate could otherwise set any Worker's capacity.
func TestRelayTLSAdmitsOnlyTheAllowedID(t *testing.T) {
	const allowed = "spiffe://cluster.local/ns/benchmark-workloads/sa/fake-workersync"
	ca := fakeworkertest.NewCA(t)
	serverCfg, err := relayTLSConfig(ca.Issue(t, "spiffe://cluster.local/ns/ate-system/sa/atelet"), ca.TrustBundle, allowed)
	if err != nil {
		t.Fatalf("relayTLSConfig: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverCfg)))
	ateapipb.RegisterWorkerServiceServer(srv, &relay{client: &recordingWorkerService{}})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	call := func(clientBundle string) error {
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			GetClientCertificate: credbundle.ClientLoader(clientBundle),
			InsecureSkipVerify:   true, // the server's identity is not under test here
		})))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, err = ateapipb.NewWorkerServiceClient(conn).RegisterWorker(context.Background(), report())
		return err
	}
	if err := call(ca.Issue(t, allowed)); err != nil {
		t.Errorf("fake-workersync refused: %v", err)
	}
	if err := call(ca.Issue(t, "spiffe://cluster.local/ns/benchmarking/sa/default")); err == nil {
		t.Error("another workload's certificate was admitted")
	}
	other := fakeworkertest.NewCA(t)
	if err := call(other.Issue(t, allowed)); err == nil {
		t.Error("a certificate from an untrusted signer was admitted")
	}
}

// A pod-identity CA rotation during a long run must not cut fake-workersync
// off: the relay reads the trust bundle at each handshake.
func TestRelayTLSFollowsATrustBundleRotation(t *testing.T) {
	t.Cleanup(credbundle.SetRecheckIntervalForTesting(0))
	const allowed = "spiffe://cluster.local/ns/benchmark-workloads/sa/fake-workersync"
	ca := fakeworkertest.NewCA(t)
	serverCfg, err := relayTLSConfig(ca.Issue(t, "spiffe://cluster.local/ns/ate-system/sa/atelet"), ca.TrustBundle, allowed)
	if err != nil {
		t.Fatalf("relayTLSConfig: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverCfg)))
	ateapipb.RegisterWorkerServiceServer(srv, &relay{client: &recordingWorkerService{}})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	next := fakeworkertest.NewCA(t)
	fakeworkertest.Rotate(t, ca.TrustBundle, next)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		GetClientCertificate: credbundle.ClientLoader(next.Issue(t, allowed)),
		InsecureSkipVerify:   true, // the server's identity is not under test here
	})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := ateapipb.NewWorkerServiceClient(conn).RegisterWorker(context.Background(), report()); err != nil {
		t.Errorf("a certificate from the rotated-in CA was refused: %v", err)
	}
}
