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

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
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
	ateapi.WorkerServiceClient

	got         []*ateapi.RegisterWorkerRequest
	acquireGot  []*ateapi.AcquireWorkerEpochRequest
	runtimesGot []*ateapi.SetWorkerSandboxRuntimesRequest
	epoch       int64
	mintGot     []*ateapi.MintWorkerActorCertificateRequest
	mintResp    *ateapi.MintWorkerActorCertificateResponse
	err         error
}

func (s *fakeWorkerService) AcquireWorkerEpoch(_ context.Context, in *ateapi.AcquireWorkerEpochRequest, _ ...grpc.CallOption) (*ateapi.AcquireWorkerEpochResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.acquireGot = append(s.acquireGot, in)
	return &ateapi.AcquireWorkerEpochResponse{Epoch: s.epoch}, nil
}

func (s *fakeWorkerService) RegisterWorker(_ context.Context, in *ateapi.RegisterWorkerRequest, _ ...grpc.CallOption) (*ateapi.RegisterWorkerResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, in)
	return &ateapi.RegisterWorkerResponse{}, nil
}

func (s *fakeWorkerService) SetWorkerSandboxRuntimes(_ context.Context, in *ateapi.SetWorkerSandboxRuntimesRequest, _ ...grpc.CallOption) (*ateapi.SetWorkerSandboxRuntimesResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.runtimesGot = append(s.runtimesGot, in)
	return &ateapi.SetWorkerSandboxRuntimesResponse{}, nil
}

func (s *fakeWorkerService) MintWorkerActorCertificate(_ context.Context, in *ateapi.MintWorkerActorCertificateRequest, _ ...grpc.CallOption) (*ateapi.MintWorkerActorCertificateResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.mintGot = append(s.mintGot, in)
	if s.mintResp != nil {
		return s.mintResp, nil
	}
	return &ateapi.MintWorkerActorCertificateResponse{}, nil
}

func TestAcquireWorkerEpoch(t *testing.T) {
	tests := []struct {
		name            string
		unauthenticated bool
		serviceErr      error
		wantCode        codes.Code
		wantEpoch       int64
		wantForwarded   []*ateapi.AcquireWorkerEpochRequest
	}{{
		// The Worker is named after the worker pod UID from the certificate.
		name:          "acquires for the calling worker",
		wantEpoch:     7,
		wantForwarded: []*ateapi.AcquireWorkerEpochRequest{{Worker: &ateapi.ObjectRef{Name: "pod-a"}}},
	}, {
		name:            "requires a certificate",
		unauthenticated: true,
		wantCode:        codes.Unauthenticated,
	}, {
		name:       "surfaces the control plane's rejection",
		serviceErr: status.Error(codes.NotFound, "no such worker"),
		wantCode:   codes.NotFound,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workers := &fakeWorkerService{err: tt.serviceErr, epoch: 7}
			svc := &workerSupportServer{workers: workers}
			ctx := workerContext(t, "pod-a")
			if tt.unauthenticated {
				ctx = context.Background()
			}

			resp, err := svc.AcquireWorkerEpoch(ctx, &ateletpb.AcquireWorkerEpochRequest{})
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("AcquireWorkerEpoch() code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if got := resp.GetEpoch(); got != tt.wantEpoch {
				t.Errorf("AcquireWorkerEpoch() epoch = %d, want %d", got, tt.wantEpoch)
			}
			if diff := cmp.Diff(tt.wantForwarded, workers.acquireGot, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRegisterWorker(t *testing.T) {
	ips := []string{"10.0.0.1"}
	forwarded := func(capacity *ateapi.WorkerResources) []*ateapi.RegisterWorkerRequest {
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request; epoch, IPs and capacity come
		// from what the worker reported.
		return []*ateapi.RegisterWorkerRequest{{
			Worker:   &ateapi.ObjectRef{Name: "pod-a"},
			Epoch:    3,
			Ips:      ips,
			Capacity: capacity,
		}}
	}

	tests := []struct {
		name string
		// unauthenticated drops the peer certificate from the context.
		unauthenticated bool
		// serviceErr is what the control plane answers with.
		serviceErr    error
		req           *ateletpb.RegisterWorkerRequest
		wantCode      codes.Code
		wantForwarded []*ateapi.RegisterWorkerRequest
	}{{
		name: "records what the worker says",
		req: &ateletpb.RegisterWorkerRequest{
			Epoch:    3,
			Ips:      ips,
			Capacity: &ateletpb.WorkerResources{Actors: 4, CpuMillicores: 2000, MemoryBytes: 4 << 30},
		},
		wantForwarded: forwarded(&ateapi.WorkerResources{Actors: 4, CpuMillicores: 2000, MemoryBytes: 4294967296}),
	}, {
		name:          "omits undetermined compute",
		req:           &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips, Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantForwarded: forwarded(&ateapi.WorkerResources{Actors: 1}),
	}, {
		name:          "forwards empty capacity",
		req:           &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips, Capacity: &ateletpb.WorkerResources{}},
		wantForwarded: forwarded(&ateapi.WorkerResources{}),
	}, {
		name: "rejects invalid capacity without forwarding",
		req: &ateletpb.RegisterWorkerRequest{
			Epoch:    3,
			Ips:      ips,
			Capacity: &ateletpb.WorkerResources{CpuMillicores: -1},
		},
		wantCode: codes.InvalidArgument,
	}, {
		name:     "rejects missing capacity without forwarding",
		req:      &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips},
		wantCode: codes.InvalidArgument,
	}, {
		// A worker may report only what its certificate proves it is.
		name:            "requires a certificate",
		unauthenticated: true,
		req:             &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips, Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantCode:        codes.Unauthenticated,
	}, {
		// The Worker record may not exist yet. The error must reach the
		// worker so it retries.
		name:       "surfaces the control plane's rejection",
		serviceErr: errors.New("no such worker"),
		req:        &ateletpb.RegisterWorkerRequest{Epoch: 3, Ips: ips, Capacity: &ateletpb.WorkerResources{Actors: 1}},
		wantCode:   codes.Unknown,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workers := &fakeWorkerService{err: tt.serviceErr}
			svc := &workerSupportServer{workers: workers}
			ctx := workerContext(t, "pod-a")
			if tt.unauthenticated {
				ctx = context.Background()
			}

			_, err := svc.RegisterWorker(ctx, tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("RegisterWorker() code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if diff := cmp.Diff(tt.wantForwarded, workers.got, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSetWorkerSandboxRuntimes(t *testing.T) {
	reqCompat := func(hash string) *ateletpb.VersionedSandboxCompat {
		return &ateletpb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes: []*ateletpb.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
				{Key: "sandbox_bundle_hash", Value: hash},
			},
		}
	}
	wantCompat := func(hash string) *ateapi.VersionedSandboxCompat {
		return &ateapi.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes: []*ateapi.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
				{Key: "sandbox_bundle_hash", Value: hash},
			},
		}
	}
	reqDefault, wantDefault := reqCompat("9988"), wantCompat("9988")
	// reqClasses is the worker's one class, gvisor, with the default and
	// the given restorable runtimes.
	reqClasses := func(restorable ...*ateletpb.VersionedSandboxCompat) []*ateletpb.WorkerSandboxClass {
		c := &ateletpb.WorkerSandboxClass{
			SandboxClass:   "gvisor",
			DefaultRuntime: &ateletpb.WorkerSandboxRuntime{Version: reqDefault},
		}
		for _, r := range restorable {
			c.RestorableRuntimes = append(c.RestorableRuntimes, &ateletpb.WorkerSandboxRuntime{Version: r})
		}
		return []*ateletpb.WorkerSandboxClass{c}
	}
	forwarded := func(restorable ...*ateapi.VersionedSandboxCompat) []*ateapi.SetWorkerSandboxRuntimesRequest {
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request; epoch and sandbox classes come
		// from what the worker reported.
		c := &ateapi.WorkerSandboxClass{
			SandboxClass:   "gvisor",
			DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: wantDefault},
		}
		for _, r := range restorable {
			c.RestorableRuntimes = append(c.RestorableRuntimes, &ateapi.WorkerSandboxRuntime{Version: r})
		}
		return []*ateapi.SetWorkerSandboxRuntimesRequest{{
			Worker:         &ateapi.ObjectRef{Name: "pod-a"},
			Epoch:          3,
			SandboxClasses: []*ateapi.WorkerSandboxClass{c},
		}}
	}

	tests := []struct {
		name string
		// unauthenticated drops the peer certificate from the context.
		unauthenticated bool
		// serviceErr is what the control plane answers with.
		serviceErr    error
		req           *ateletpb.SetWorkerSandboxRuntimesRequest
		wantCode      codes.Code
		wantForwarded []*ateapi.SetWorkerSandboxRuntimesRequest
	}{{
		name:          "records what the worker says",
		req:           &ateletpb.SetWorkerSandboxRuntimesRequest{Epoch: 3, SandboxClasses: reqClasses()},
		wantForwarded: forwarded(),
	}, {
		name:          "forwards restorable runtimes",
		req:           &ateletpb.SetWorkerSandboxRuntimesRequest{Epoch: 3, SandboxClasses: reqClasses(reqCompat("d547"))},
		wantForwarded: forwarded(wantCompat("d547")),
	}, {
		name:     "rejects missing sandbox classes without forwarding",
		req:      &ateletpb.SetWorkerSandboxRuntimesRequest{Epoch: 3},
		wantCode: codes.InvalidArgument,
	}, {
		name:     "rejects missing epoch without forwarding",
		req:      &ateletpb.SetWorkerSandboxRuntimesRequest{SandboxClasses: reqClasses()},
		wantCode: codes.InvalidArgument,
	}, {
		name: "rejects missing default runtime without forwarding",
		req: &ateletpb.SetWorkerSandboxRuntimesRequest{
			Epoch:          3,
			SandboxClasses: []*ateletpb.WorkerSandboxClass{{SandboxClass: "gvisor"}},
		},
		wantCode: codes.InvalidArgument,
	}, {
		// A worker runs one class until WorkerPoolSpec.sandboxClasses
		// allows more.
		name: "rejects two sandbox classes without forwarding",
		req: &ateletpb.SetWorkerSandboxRuntimesRequest{
			Epoch: 3,
			SandboxClasses: []*ateletpb.WorkerSandboxClass{
				{SandboxClass: "gvisor", DefaultRuntime: &ateletpb.WorkerSandboxRuntime{Version: reqDefault}},
				{SandboxClass: "microvm", DefaultRuntime: &ateletpb.WorkerSandboxRuntime{Version: reqDefault}},
			},
		},
		wantCode: codes.InvalidArgument,
	}, {
		// A worker may report only what its certificate proves it is.
		name:            "requires a certificate",
		unauthenticated: true,
		req:             &ateletpb.SetWorkerSandboxRuntimesRequest{Epoch: 3, SandboxClasses: reqClasses()},
		wantCode:        codes.Unauthenticated,
	}, {
		// A refused epoch must reach the worker unchanged: it is what tells
		// the worker process to stop serving.
		name:       "surfaces the control plane's refusal",
		serviceErr: status.Error(codes.FailedPrecondition, "epoch superseded"),
		req:        &ateletpb.SetWorkerSandboxRuntimesRequest{Epoch: 3, SandboxClasses: reqClasses()},
		wantCode:   codes.FailedPrecondition,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			workers := &fakeWorkerService{err: tt.serviceErr}
			svc := &workerSupportServer{workers: workers}
			ctx := workerContext(t, "pod-a")
			if tt.unauthenticated {
				ctx = context.Background()
			}

			_, err := svc.SetWorkerSandboxRuntimes(ctx, tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("SetWorkerSandboxRuntimes() code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if diff := cmp.Diff(tt.wantForwarded, workers.runtimesGot, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded requests mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

type fakeSuspendService struct {
	ateapi.WorkerServiceClient

	got        []*ateapi.RequestActorSuspendRequest
	gotCrashed []*ateapi.ReportActorCrashedRequest
	err        error
}

func (s *fakeSuspendService) ReportActorCrashed(_ context.Context, in *ateapi.ReportActorCrashedRequest, _ ...grpc.CallOption) (*ateapi.ReportActorCrashedResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.gotCrashed = append(s.gotCrashed, in)
	return &ateapi.ReportActorCrashedResponse{}, nil
}

func (s *fakeSuspendService) RequestActorSuspend(_ context.Context, in *ateapi.RequestActorSuspendRequest, _ ...grpc.CallOption) (*ateapi.RequestActorSuspendResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, in)
	return &ateapi.RequestActorSuspendResponse{}, nil
}

func TestRequestActorSuspendNamesTheCallingWorker(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &workerSupportServer{workers: workers}

	ctx := workerContext(t, "pod-a")
	if _, err := svc.RequestActorSuspend(ctx, &ateletpb.RequestActorSuspendRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
	}); err != nil {
		t.Fatalf("RequestActorSuspend() failed: %v", err)
	}

	want := []*ateapi.RequestActorSuspendRequest{{
		// The Worker is named after the worker pod UID, taken from the
		// certificate rather than the request: a worker may speak for the
		// actors it hosts, and only the control plane knows which those are.
		Worker:   &ateapi.ObjectRef{Name: "pod-a"},
		Actor:    &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid: "01234567-89ab-cdef-0123-456789abcdef",
	}}
	if diff := cmp.Diff(want, workers.got, protocmp.Transform()); diff != "" {
		t.Errorf("forwarded request mismatch (-want +got):\n%s", diff)
	}
}

func TestRequestActorSuspendRequiresACertificate(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &workerSupportServer{workers: workers}

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
	svc := &workerSupportServer{workers: workers}

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
		mintResp: &ateapi.MintWorkerActorCertificateResponse{
			ActorCertificates: wantCerts,
		},
	}
	svc := &workerSupportServer{workers: workers}

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

	want := []*ateapi.MintWorkerActorCertificateRequest{{
		// The worker comes from the caller's certificate.
		Worker: &ateapi.ObjectRef{Name: "pod-a"},
		Actor: &ateapi.ObjectRef{
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
	svc := &workerSupportServer{workers: workers}

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

type fakeEgressPolicyControl struct {
	ateapi.ControlClient

	got  []*ateapi.GetActorEgressPolicyRequest
	resp *ateapi.EgressPolicy
	err  error
}

func (c *fakeEgressPolicyControl) GetActorEgressPolicy(_ context.Context, in *ateapi.GetActorEgressPolicyRequest, _ ...grpc.CallOption) (*ateapi.EgressPolicy, error) {
	c.got = append(c.got, in)
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}

func TestGetActorEgressPolicy(t *testing.T) {
	policy := &ateapi.EgressPolicy{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "default"},
		Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{
			Hostnames: []string{"api.example.com"},
			Ports:     &ateapi.Ports{Numbers: []int32{80}},
		}}},
	}
	// The copy toAteletEgressPolicy makes; TestToAteletEgressPolicy covers it.
	copied := &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{Http: &ateletpb.EgressDestinations{
		Hostnames: []string{"api.example.com"},
		Ports:     &ateletpb.EgressPorts{Numbers: []int32{80}},
	}}}}
	req := &ateletpb.GetActorEgressPolicyRequest{ActorAtespace: "team-a", ActorName: "actor-1"}
	forwarded := []*ateapi.GetActorEgressPolicyRequest{{
		Actor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		Name:  resources.SingletonName,
	}}

	tests := []struct {
		name string
		// unauthenticated drops the peer certificate from the context.
		unauthenticated bool
		req             *ateletpb.GetActorEgressPolicyRequest
		// resp and serviceErr are what the control plane answers with.
		resp          *ateapi.EgressPolicy
		serviceErr    error
		wantCode      codes.Code
		wantForwarded []*ateapi.GetActorEgressPolicyRequest
		// wantPolicy is the policy the worker gets back; nil means unset.
		wantPolicy *ateletpb.EgressPolicy
	}{{
		name:          "returns the policy",
		req:           req,
		resp:          policy,
		wantForwarded: forwarded,
		wantPolicy:    copied,
	}, {
		name:          "leaves no policy unset",
		req:           req,
		serviceErr:    status.Error(codes.NotFound, "EgressPolicy for actor team-a/actor-1 not found"),
		wantForwarded: forwarded,
	}, {
		name:            "requires a certificate",
		unauthenticated: true,
		req:             req,
		wantCode:        codes.Unauthenticated,
	}, {
		name:     "rejects an invalid request without forwarding",
		req:      &ateletpb.GetActorEgressPolicyRequest{ActorAtespace: "team-a"},
		wantCode: codes.InvalidArgument,
	}, {
		// A failure is not "no policy": the worker must be able to tell them
		// apart, and refuses DNS until a fetch succeeds.
		name:          "surfaces the control plane's failure",
		req:           req,
		serviceErr:    status.Error(codes.Unavailable, "ateapi unavailable"),
		wantCode:      codes.Unavailable,
		wantForwarded: forwarded,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &fakeEgressPolicyControl{resp: tt.resp, err: tt.serviceErr}
			svc := &workerSupportServer{control: control}
			ctx := workerContext(t, "pod-a")
			if tt.unauthenticated {
				ctx = context.Background()
			}

			resp, err := svc.GetActorEgressPolicy(ctx, tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("GetActorEgressPolicy() code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if diff := cmp.Diff(tt.wantForwarded, control.got, protocmp.Transform()); diff != "" {
				t.Errorf("forwarded requests mismatch (-want +got):\n%s", diff)
			}
			if err != nil {
				return
			}
			if (resp.GetEgressPolicy() == nil) != (tt.wantPolicy == nil) {
				t.Fatalf("EgressPolicy = %v, want %v", resp.GetEgressPolicy(), tt.wantPolicy)
			}
			if diff := cmp.Diff(tt.wantPolicy, resp.GetEgressPolicy(), protocmp.Transform()); diff != "" {
				t.Errorf("returned policy mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestReportActorCrashedNamesTheCallingWorker(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &workerSupportServer{workers: workers}

	if _, err := svc.ReportActorCrashed(workerContext(t, "pod-a"), &ateletpb.ReportActorCrashedRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
		Message:       "cloud-hypervisor exited",
	}); err != nil {
		t.Fatalf("ReportActorCrashed() failed: %v", err)
	}
	want := []*ateapi.ReportActorCrashedRequest{{
		Worker:   &ateapi.ObjectRef{Name: "pod-a"},
		Actor:    &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid: "01234567-89ab-cdef-0123-456789abcdef",
		Message:  "cloud-hypervisor exited",
	}}
	if diff := cmp.Diff(want, workers.gotCrashed, protocmp.Transform()); diff != "" {
		t.Errorf("forwarded request mismatch (-want +got):\n%s", diff)
	}
}

func TestReportActorCrashedRequiresACertificate(t *testing.T) {
	workers := &fakeSuspendService{}
	svc := &workerSupportServer{workers: workers}

	_, err := svc.ReportActorCrashed(context.Background(), &ateletpb.ReportActorCrashedRequest{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "01234567-89ab-cdef-0123-456789abcdef",
		Message:       "cloud-hypervisor exited",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("unauthenticated report returned %v, want Unauthenticated", err)
	}
	if len(workers.gotCrashed) != 0 {
		t.Errorf("unauthenticated report was still forwarded: %v", workers.gotCrashed)
	}
}
