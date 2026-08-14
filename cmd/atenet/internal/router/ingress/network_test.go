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

package ingress

import (
	"context"
	"strings"
	"testing"

	networkextprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/network_ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// networkAuthorityAttributes builds the ProcessingRequest.Attributes map
// xds.go's buildTcpConnectFilterChain's ConnectionAttributes evaluates
// AuthorityFilterStateAttribute into, mirroring ingress_test.go's
// authorityAttributes for the HTTP leg -- both read the same filter-state
// key, just via each protocol's own attribute-evaluation mechanism.
func networkAuthorityAttributes(t *testing.T, authority string) map[string]*structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(map[string]any{AuthorityFilterStateAttribute: authority})
	if err != nil {
		t.Fatalf("build authority attributes: %v", err)
	}
	return map[string]*structpb.Struct{
		"envoy.filters.network.ext_proc": s,
	}
}

// TestNetworkExtProcHandleFirstFrameNestsMetadata locks in that
// handleFirstFrame's resolved worker address is nested under
// OriginalDstMetadataKey, matching MetadataOptions.ReceivingNamespaces in
// xds.go's buildTcpConnectFilterChain -- Envoy's network ext_proc filter only
// ingests DynamicMetadata fields whose top-level key is on that allowlist,
// silently dropping anything else (see xds.go's ReceivingNamespaces comment).
func TestNetworkExtProcHandleFirstFrameNestsMetadata(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"}}}, nil
		},
	}
	s := NewNetworkExtProcServer(clientMock)

	req := &networkextprocv3.ProcessingRequest{
		Attributes: networkAuthorityAttributes(t, testUUID+".team-a.actors.resources.substrate.ate.dev:9090"),
	}

	resp, err := s.handleFirstFrame(context.Background(), req)
	if err != nil {
		t.Fatalf("handleFirstFrame: %v", err)
	}

	const wantTarget = "10.0.0.52:443"
	got := resp.GetDynamicMetadata().GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstAddressKey].GetStringValue()
	if got != wantTarget {
		t.Errorf("dynamic metadata %s/%s = %q, want %q", OriginalDstMetadataKey, OriginalDstAddressKey, got, wantTarget)
	}
	if flat := resp.GetDynamicMetadata().GetFields()[OriginalDstAddressKey].GetStringValue(); flat != "" {
		t.Errorf("dynamic metadata unexpectedly set %s at the top level (%q): Envoy's ReceivingNamespaces allowlist only ingests %s", OriginalDstAddressKey, flat, OriginalDstMetadataKey)
	}
}

// TestNetworkExtProcHandleFirstFrameMissingAuthority locks in the failure
// mode when Envoy's ConnectionAttributes evaluation comes back empty (e.g. an
// Envoy build predating envoyproxy/envoy#46551, which added
// connection_attributes/filter-state support to NetworkExternalProcessor
// -- until then this attribute could never be populated at all): the
// connection is refused with a clear error rather than resolving some
// unrelated actor or panicking on an empty authority.
func TestNetworkExtProcHandleFirstFrameMissingAuthority(t *testing.T) {
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			t.Fatal("ResumeActor must not be called without a resolved authority")
			return nil, nil
		},
	}
	s := NewNetworkExtProcServer(clientMock)

	_, err := s.handleFirstFrame(context.Background(), &networkextprocv3.ProcessingRequest{})
	if err == nil {
		t.Fatal("expected an error for a request with no attributes")
	}
	if !strings.Contains(err.Error(), AuthorityFilterStateAttribute) {
		t.Errorf("error %q should name the missing attribute %q", err, AuthorityFilterStateAttribute)
	}
}
