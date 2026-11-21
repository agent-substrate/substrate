// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/atenet"
	atunnelingress "go.ate.dev/substrate/internal/atunnel/ingress"
)

type mockClient struct {
	ateapi.ControlClient
	resumeFn func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error)
}

func (m *mockClient) ResumeActor(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
	return m.resumeFn(ctx, in, opts...)
}

func requestMetadata(actorName, atespace string, headers ...*corev3.HeaderValue) *extproc.RequestMetadata {
	headers = append(headers,
		&corev3.HeaderValue{Key: atenet.TargetActorHeader, Value: atespace + "/" + actorName},
	)
	return extproc.NewRequestMetadata(headers, nil)
}

func TestHandleRequestHeadersAcceptsMixedCaseRoutingHeaders(t *testing.T) {
	clientMock := &mockClient{
		resumeFn: func(_ context.Context, in *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			if got, want := in.GetActor().GetName(), "actor-1"; got != want {
				t.Errorf("actor name = %q, want %q", got, want)
			}
			if got, want := in.GetActor().GetAtespace(), "team-a"; got != want {
				t.Errorf("atespace = %q, want %q", got, want)
			}
			return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{
				Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}},
			}}, nil
		},
	}
	h := New(clientMock, ParkedRequestConfig{}, nil)
	md := extproc.NewRequestMetadata([]*corev3.HeaderValue{
		{Key: "ate-target-actor", Value: "team-a/actor-1"},
	}, nil)

	if _, err := h.HandleRequestHeaders(context.Background(), md); err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v", err)
	}
}

func TestHandleRequestHeadersRejectsDuplicateOrCommaHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers []*corev3.HeaderValue
		attrs   map[string]*structpb.Struct
	}{
		{
			name: "duplicate headers with different values",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "demo/a"},
				{Key: atenet.TargetActorHeader, Value: "secret/x"},
			},
		},
		{
			name: "duplicate headers with identical values",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "demo/a"},
				{Key: atenet.TargetActorHeader, Value: "demo/a"},
			},
		},
		{
			name: "single header with comma-joined value",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "demo/a,secret/x"},
			},
		},
		{
			name: "filter state attribute with comma-joined value",
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.TargetActorFilterStateAttribute: structpb.NewStringValue("demo/a,secret/x"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resumeCalls int32
			clientMock := &mockClient{
				resumeFn: func(_ context.Context, _ *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
					atomic.AddInt32(&resumeCalls, 1)
					return nil, errors.New("should not be called")
				},
			}
			h := New(clientMock, ParkedRequestConfig{}, nil)
			md := extproc.NewRequestMetadata(tt.headers, tt.attrs)

			_, err := h.HandleRequestHeaders(context.Background(), md)
			if err == nil {
				t.Fatalf("HandleRequestHeaders() succeeded, want 404 error")
			}
			var reqErr *extproc.ReqError
			if !errors.As(err, &reqErr) {
				t.Fatalf("error = %v, want *extproc.ReqError", err)
			}
			if reqErr.StatusCode != int(envoy_type.StatusCode_NotFound) {
				t.Errorf("StatusCode = %d, want %d", reqErr.StatusCode, envoy_type.StatusCode_NotFound)
			}
			if !strings.Contains(reqErr.Error(), "invalid actor reference") {
				t.Errorf("error string = %q, want containing %q", reqErr.Error(), "invalid actor reference")
			}
			if calls := atomic.LoadInt32(&resumeCalls); calls != 0 {
				t.Errorf("ResumeActor called %d times, want 0", calls)
			}
		})
	}
}

func TestHandleRequestHeadersConnectAttributeOverridesInnerHeader(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	var gotActorRef string
	clientMock := &mockClient{
		resumeFn: func(_ context.Context, in *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			gotActorRef = in.GetActor().GetAtespace() + "/" + in.GetActor().GetName()
			return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{
				Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}},
			}}, nil
		},
	}
	h := New(clientMock, ParkedRequestConfig{}, nil)
	md := extproc.NewRequestMetadata(
		[]*corev3.HeaderValue{
			{Key: atenet.TargetActorHeader, Value: "secret/x"},
		},
		map[string]*structpb.Struct{
			"envoy.filters.http.ext_proc": {
				Fields: map[string]*structpb.Value{
					extproc.TargetActorFilterStateAttribute: structpb.NewStringValue("team-a/" + testUUID),
				},
			},
		},
	)

	res, err := h.HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v", err)
	}

	if got, want := gotActorRef, "team-a/"+testUUID; got != want {
		t.Errorf("ResumeActor called with %q, want %q", got, want)
	}

	mutation := res.Response.GetResponse().GetHeaderMutation()
	gotMutations := map[string]string{}
	for _, headerOption := range mutation.GetSetHeaders() {
		gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
	}
	if got, want := gotMutations[strings.ToLower(atenet.TargetActorHeader)], "team-a/"+testUUID; got != want {
		t.Errorf("actor target mutation = %q, want %q", got, want)
	}
}

// dynamicMetadataTarget extracts the resolved worker address
// HandleRequestHeaders reports via OriginalDstMetadataKey/OriginalDstAddressKey.
func dynamicMetadataTarget(dynamicMetadata *structpb.Struct) string {
	return dynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstAddressKey].GetStringValue()
}

// dynamicMetadataPort extracts the target port HandleRequestHeaders reports
// via OriginalDstMetadataKey/OriginalDstPortKey.
func dynamicMetadataPort(dynamicMetadata *structpb.Struct) string {
	return dynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstPortKey].GetStringValue()
}

// testWorkerSPIFFEID is the SPIFFE ID the test assignments give their worker.
const testWorkerSPIFFEID = "spiffe://cluster.local/ns/workers/sa/substrate-worker"

// dynamicMetadataWorkerSPIFFEID extracts the SPIFFE ID HandleRequestHeaders
// requires of atunnel.
func dynamicMetadataWorkerSPIFFEID(dynamicMetadata *structpb.Struct) string {
	return dynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[WorkerSPIFFEIDKey].GetStringValue()
}

func TestHandleRequestHeadersDoesNotLogSensitiveData(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	const secret = "do-not-log-me"
	authority := testUUID + ".team-a.actors.resources.substrate.ate.dev"

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	h := New(&mockClient{
		resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}}}}, nil
		},
	}, ParkedRequestConfig{}, nil)

	md := requestMetadata(testUUID, "team-a",
		&corev3.HeaderValue{Key: ":path", Value: "/api/v1/reset?token=" + secret},
		&corev3.HeaderValue{Key: ":authority", Value: authority},
		&corev3.HeaderValue{Key: ":method", Value: "POST"},
		&corev3.HeaderValue{Key: "authorization", Value: "Bearer " + secret},
		&corev3.HeaderValue{Key: "cookie", Value: "session=" + secret},
	)

	res, err := h.HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	if strings.Contains(out, secret) {
		t.Errorf("router log leaked sensitive value: %s", out)
	}
	if !strings.Contains(out, testUUID) {
		t.Errorf("router log missing actor/host routing context: %s", out)
	}

	// The mux records every handled request on the status page; the metadata the
	// handler was given must not carry the secret into it either.
	rec := extproc.NewQueryRecorder(10)
	rec.AddRouterRequest(time.Now(), time.Millisecond, "Route ok", res.Target, md)
	for _, q := range rec.Get() {
		if blob, _ := json.Marshal(q); strings.Contains(string(blob), secret) {
			t.Errorf("recorder/statusz retained sensitive value: %s", blob)
		}
	}
}

func TestHandleRequestHeaders(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"

	tests := []struct {
		name               string
		actorName          string
		atespace           string
		authority          string
		resumeResp         *ateapi.ResumeActorResponse
		resumeErr          error
		expectErr          bool
		expectedErrStr     string
		expectedStatus     envoy_type.StatusCode
		expectedTarget     string
		expectedTargetPort string
	}{
		{
			name:           "invalid actor header returns 404",
			actorName:      "INVALID",
			atespace:       "team-a",
			authority:      "invalid-host.com",
			expectErr:      true,
			expectedErrStr: `invalid actor reference`,
			expectedStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:           "non-gRPC resume error collapses to 500 without leaking detail",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      errors.New("resume failed with sensitive detail"),
			expectErr:      true,
			expectedErrStr: `error resuming actor team-a/123e4567-e89b-12d3-a456-426614174000`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:           "FailedPrecondition maps to 503 with preserved desc",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.FailedPrecondition, "no worker has room for the actor"),
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 unavailable: no worker has room for the actor`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "NotFound maps to 404",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.NotFound, "actor missing"),
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 not found`,
			expectedStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:           "Unavailable maps to 503",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.Unavailable, "control-plane down"),
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 unavailable`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "DeadlineExceeded maps to 504",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.DeadlineExceeded, "deadline"),
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 request timed out`,
			expectedStatus: envoy_type.StatusCode_GatewayTimeout,
		},
		{
			name:      "Bad Actor IP from resume returns 500 without leaking IP",
			authority: testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeResp: &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"invalid-ip"}}},
				},
			},
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:      "No Actor IPs from resume returns 500",
			authority: testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeResp: &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{}},
				},
			},
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:      "No worker SPIFFE ID from resume returns 500",
			authority: testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeResp: &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerPodIps: []string{"10.0.0.52"}}},
				},
			},
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:      "successful resume ignores host and port for actor routing",
			authority: "127.0.0.1:44681",
			resumeResp: &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}},
				},
			},
			expectErr:          false,
			expectedTarget:     "10.0.0.52:443",
			expectedTargetPort: "80",
		},
		{
			name:      "dual-stack resume routes to the first IP",
			authority: "127.0.0.1:44681",
			resumeResp: &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"fd00::52", "10.0.0.52"}}},
				},
			},
			expectErr:          false,
			expectedTarget:     "[fd00::52]:443",
			expectedTargetPort: "80",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actorName := tc.actorName
			if actorName == "" {
				actorName = testUUID
			}
			atespace := tc.atespace
			if atespace == "" {
				atespace = "team-a"
			}
			clientMock := &mockClient{
				resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
					if in.GetActor().GetName() != testUUID {
						t.Errorf("unexpected identifier parsed in test context: %s", in.GetActor().GetName())
					}
					if tc.resumeErr != nil {
						return nil, tc.resumeErr
					}
					return tc.resumeResp, nil
				},
			}

			// Parking disabled: these cases assert fail-fast mapping of resume
			// errors (e.g. FailedPrecondition -> immediate 503). Parking behavior
			// is covered separately in TestHandleRequestHeaders_ParkingLotFull and
			// resumer_test.go.
			h := New(clientMock, ParkedRequestConfig{}, nil)

			md := requestMetadata(actorName, atespace,
				&corev3.HeaderValue{Key: ":path", Value: "/v1/actors/invoke"},
				&corev3.HeaderValue{Key: ":authority", Value: tc.authority},
				&corev3.HeaderValue{Key: ":method", Value: "POST"},
			)

			res, err := h.HandleRequestHeaders(context.Background(), md)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if tc.expectedErrStr != "" && err.Error() != tc.expectedErrStr {
					t.Errorf("client body mismatch:\n  got:  %q\n  want: %q", err.Error(), tc.expectedErrStr)
				}
				var reqErr *extproc.ReqError
				if !errors.As(err, &reqErr) {
					t.Fatalf("expected *extproc.ReqError, got %T (%v)", err, err)
				}
				if got, want := reqErr.StatusCode, int(tc.expectedStatus); got != want {
					t.Errorf("HTTP status code = %d, want %d", got, want)
				}
				if tc.resumeErr != nil && !errors.Is(err, tc.resumeErr) {
					t.Errorf("original resume error must be preserved in chain for logs; errors.Is(err, resumeErr) = false")
				}
				return
			}

			if err != nil {
				t.Fatalf("ext_proc processing error: %v", err)
			}
			if res.Target != tc.expectedTarget {
				t.Errorf("expected target %q, got %q", tc.expectedTarget, res.Target)
			}

			mutation := res.Response.GetResponse().GetHeaderMutation()
			if len(mutation.GetSetHeaders()) != 1 {
				t.Fatalf("expected actor routing header, found: %v", mutation.GetSetHeaders())
			}

			gotMutations := map[string]string{}
			for _, headerOption := range mutation.GetSetHeaders() {
				gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
			}
			if _, ok := gotMutations[strings.ToLower(atunnelingress.TargetPortHeader)]; ok {
				t.Errorf("target port must be emitted only as dynamic metadata")
			}
			if got, want := gotMutations[strings.ToLower(atenet.TargetActorHeader)], "team-a/"+testUUID; got != want {
				t.Errorf("actor target mutation = %q, want %q", got, want)
			}
			if got := dynamicMetadataTarget(res.DynamicMetadata); got != tc.expectedTarget {
				t.Errorf("invalid destination mapping found: %s, expected: %s", got, tc.expectedTarget)
			}
			if got := dynamicMetadataPort(res.DynamicMetadata); got != tc.expectedTargetPort {
				t.Errorf("dynamic metadata port = %q, want %q", got, tc.expectedTargetPort)
			}
			if got, want := dynamicMetadataWorkerSPIFFEID(res.DynamicMetadata), testWorkerSPIFFEID; got != want {
				t.Errorf("dynamic metadata worker SPIFFE ID = %q, want %q", got, want)
			}
		})
	}
}

// TestHandleRequestHeadersHandlesConnectMethod locks in that a CONNECT request
// (used for atenet-router's arbitrary-port ingress support -- the target port
// travels in :authority, e.g. "<actor-dns>:9090") resolves the actor,
// produces the same "<workerIP>:443" original-dst mutation as an ordinary
// request (the router only ever dials the worker's atunnel server), and
// reports the arbitrary port itself in dynamic metadata for Envoy to write to
// atunnelingress.TargetPortHeader.
func TestHandleRequestHeadersHandlesConnectMethod(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	authority := testUUID + ".team-a.actors.resources.substrate.ate.dev:9090"

	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}}}}, nil
		},
	}
	h := New(clientMock, ParkedRequestConfig{}, nil)

	// CONNECT requests carry no :path; the request-target lives in :authority.
	md := requestMetadata(testUUID, "team-a",
		&corev3.HeaderValue{Key: ":authority", Value: authority},
		&corev3.HeaderValue{Key: ":method", Value: "CONNECT"},
	)

	res, err := h.HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("ext_proc processing error for CONNECT: %v", err)
	}

	const wantTarget = "10.0.0.52:443"
	if res.Target != wantTarget {
		t.Errorf("target = %q, want %q", res.Target, wantTarget)
	}
	if got := dynamicMetadataTarget(res.DynamicMetadata); got != wantTarget {
		t.Errorf("invalid destination mapping found: %s, expected: %s", got, wantTarget)
	}
	if got := dynamicMetadataPort(res.DynamicMetadata); got != "9090" {
		t.Errorf("dynamic metadata port = %q, want %q", got, "9090")
	}
}

func TestHandleRequestHeadersUsesRetainedConnectAuthorityForPort(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	clientMock := &mockClient{
		resumeFn: func(context.Context, *ateapi.ResumeActorRequest, ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{
				Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}},
			}}, nil
		},
	}
	attrs := map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
				extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue("team-a/" + testUUID),
			},
		},
	}
	md := extproc.NewRequestMetadata([]*corev3.HeaderValue{
		{Key: ":authority", Value: "inner.example"},
	}, attrs)

	res, err := New(clientMock, ParkedRequestConfig{}, nil).HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("HandleRequestHeaders() error = %v", err)
	}
	if got, want := dynamicMetadataPort(res.DynamicMetadata), "9090"; got != want {
		t.Errorf("target port = %q, want %q", got, want)
	}

	mutation := res.Response.GetResponse().GetHeaderMutation()
	gotMutations := map[string]string{}
	for _, headerOption := range mutation.GetSetHeaders() {
		gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
	}
	if got, want := gotMutations[strings.ToLower(atenet.TargetActorHeader)], "team-a/"+testUUID; got != want {
		t.Errorf("actor target mutation = %q, want %q", got, want)
	}
}

func TestHandleRequestHeadersConnectRequiresOuterTargetActor(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	tests := []struct {
		name           string
		headers        []*corev3.HeaderValue
		attrs          map[string]*structpb.Struct
		expectErr      bool
		wantStatusCode int
		wantActorRef   string
	}{
		{
			name: "connect request with no outer target actor and inner header is rejected",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "team-a/" + testUUID},
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
					},
				},
			},
			expectErr:      true,
			wantStatusCode: int(envoy_type.StatusCode_NotFound),
		},
		{
			name: "connect request with empty outer target actor and inner header is rejected",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "team-a/" + testUUID},
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
						extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue(""),
					},
				},
			},
			expectErr:      true,
			wantStatusCode: int(envoy_type.StatusCode_NotFound),
		},
		{
			name: "connect request with no outer target actor and no inner header is rejected",
			headers: []*corev3.HeaderValue{
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
					},
				},
			},
			expectErr:      true,
			wantStatusCode: int(envoy_type.StatusCode_NotFound),
		},
		{
			name: "connect request with comma-separated outer target actor is rejected",
			headers: []*corev3.HeaderValue{
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
						extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue("team-a/a,team-b/b"),
					},
				},
			},
			expectErr:      true,
			wantStatusCode: int(envoy_type.StatusCode_NotFound),
		},
		{
			name: "connect request with outer target actor resolves even if inner header is omitted",
			headers: []*corev3.HeaderValue{
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
						extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue("team-a/" + testUUID),
					},
				},
			},
			expectErr:    false,
			wantActorRef: "team-a/" + testUUID,
		},
		{
			name: "connect request with outer target actor X overrides inner header Y",
			headers: []*corev3.HeaderValue{
				{Key: atenet.TargetActorHeader, Value: "team-b/untrusted-actor"},
				{Key: ":authority", Value: "inner.example"},
			},
			attrs: map[string]*structpb.Struct{
				"envoy.filters.http.ext_proc": {
					Fields: map[string]*structpb.Value{
						extproc.ConnectAuthorityFilterStateAttribute: structpb.NewStringValue("unrelated.example:9090"),
						extproc.TargetActorFilterStateAttribute:      structpb.NewStringValue("team-a/" + testUUID),
					},
				},
			},
			expectErr:    false,
			wantActorRef: "team-a/" + testUUID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotActorRef string
			var resumeCalls int32
			clientMock := &mockClient{
				resumeFn: func(_ context.Context, in *ateapi.ResumeActorRequest, _ ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
					atomic.AddInt32(&resumeCalls, 1)
					gotActorRef = in.GetActor().GetAtespace() + "/" + in.GetActor().GetName()
					return &ateapi.ResumeActorResponse{Actor: &ateapi.Actor{
						Status: &ateapi.ActorStatus{WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.52"}}},
					}}, nil
				},
			}

			h := New(clientMock, ParkedRequestConfig{}, nil)
			md := extproc.NewRequestMetadata(tt.headers, tt.attrs)

			res, err := h.HandleRequestHeaders(context.Background(), md)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("HandleRequestHeaders() succeeded, want error")
				}
				var reqErr *extproc.ReqError
				if !errors.As(err, &reqErr) {
					t.Fatalf("error = %v, want *extproc.ReqError", err)
				}
				if reqErr.StatusCode != tt.wantStatusCode {
					t.Errorf("StatusCode = %d, want %d", reqErr.StatusCode, tt.wantStatusCode)
				}
				if calls := atomic.LoadInt32(&resumeCalls); calls != 0 {
					t.Errorf("ResumeActor called %d times, want 0", calls)
				}
				return
			}

			if err != nil {
				t.Fatalf("HandleRequestHeaders() error = %v", err)
			}
			if got, want := gotActorRef, tt.wantActorRef; got != want {
				t.Errorf("ResumeActor called with %q, want %q", got, want)
			}
			mutation := res.Response.GetResponse().GetHeaderMutation()
			gotMutations := map[string]string{}
			for _, headerOption := range mutation.GetSetHeaders() {
				gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
			}
			if got, want := gotMutations[strings.ToLower(atenet.TargetActorHeader)], tt.wantActorRef; got != want {
				t.Errorf("actor target mutation = %q, want %q", got, want)
			}
		})
	}
}

// TestHandleRequestHeaders_FullLotServesRunningActor pins the design guarantee
// from docs/request-parking.md: a saturated parking lot cannot starve requests
// to already-running actors, at any lot size. A request whose resume resolves
// on the first attempt never occupies a slot, so it is served even with the
// lot at capacity (issue #1081).
func TestHandleRequestHeaders_FullLotServesRunningActor(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	authority := testUUID + ".team-a.actors.resources.substrate.ate.dev"

	var resumeCalled bool
	clientMock := &mockClient{
		resumeFn: func(
			ctx context.Context,
			in *ateapi.ResumeActorRequest,
			opts ...grpc.CallOption,
		) (*ateapi.ResumeActorResponse, error) {
			resumeCalled = true
			return &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Status: &ateapi.ActorStatus{
						State:            ateapi.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapi.WorkerAssignment{WorkerSpiffeId: testWorkerSPIFFEID, WorkerPodIps: []string{"10.0.0.1"}},
					},
				},
			}, nil
		},
	}

	// A 1-slot lot with the slot already occupied deterministically simulates a
	// full lot without needing a concurrent in-flight request.
	h := New(clientMock, ParkedRequestConfig{Budget: time.Second, Max: 1}, nil)
	release, ok := h.parking.enter(context.Background(), ateattr.RouterOutcomeUnavailable)
	if !ok {
		t.Fatal("priming enter should be admitted")
	}
	defer release(parkOutcomeServed)

	md := requestMetadata(testUUID, "team-a",
		&corev3.HeaderValue{Key: ":authority", Value: authority},
	)

	res, err := h.HandleRequestHeaders(context.Background(), md)
	if err != nil {
		t.Fatalf("a running actor must be served despite a full lot, got: %v", err)
	}
	if !resumeCalled {
		t.Error("the resume lookup must still run when the lot is full")
	}
	const wantTarget = "10.0.0.1:443"
	if res.Target != wantTarget {
		t.Errorf("target = %q, want %q", res.Target, wantTarget)
	}
	if got := h.parking.activeCount(); got != 1 {
		t.Errorf("a first-attempt resolution must not occupy a slot; active = %d, want 1 (the priming entry)", got)
	}
}

// TestHandleRequestHeaders_FailedPreconditionFailsFast completes the full-lot
// trio: a FailedPrecondition from ResumeActor neither parks nor is shed. The
// request fails after its single attempt with the resume error, and the full
// lot never sees it.
func TestHandleRequestHeaders_FailedPreconditionFailsFast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const testUUID = "123e4567-e89b-12d3-a456-426614174000"
		authority := testUUID + ".team-a.actors.resources.substrate.ate.dev"
		const desc = "AssignWorker prerequisite not met for Actor: team-a/" + testUUID + " (got: ACTOR_STATE_CRASHED, want ACTOR_STATE_SUSPENDED)"

		var resumeCalls atomic.Int32
		clientMock := &mockClient{
			resumeFn: func(context.Context, *ateapi.ResumeActorRequest, ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
				resumeCalls.Add(1)
				return nil, status.Error(codes.FailedPrecondition, desc)
			},
		}

		parkMetrics, reader := newTestParkingMetrics(t)
		h := New(clientMock, ParkedRequestConfig{Budget: 500 * time.Millisecond, Max: 1}, parkMetrics)
		// With the only slot taken, a request that parked would be shed with
		// "router at capacity" instead of the resume error.
		release, ok := h.parking.enter(context.Background(), ateattr.RouterOutcomeUnavailable)
		if !ok {
			t.Fatal("priming enter should be admitted")
		}
		defer release(parkOutcomeServed)

		md := requestMetadata(testUUID, "team-a",
			&corev3.HeaderValue{Key: ":authority", Value: authority},
		)

		start := time.Now()
		_, err := h.HandleRequestHeaders(context.Background(), md)

		// Never parked: no wait, one attempt, the lot untouched.
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("request waited %v, want no parked wait", elapsed)
		}
		if got := resumeCalls.Load(); got != 1 {
			t.Errorf("resume attempts = %d, want 1", got)
		}
		if got := rejectedByOutcome(t, reader); len(got) != 0 {
			t.Errorf("parking.rejected = %v, want none", got)
		}

		// The caller gets the resume error, not the shed.
		var reqErr *extproc.ReqError
		if !errors.As(err, &reqErr) {
			t.Fatalf("expected *extproc.ReqError, got %T (%v)", err, err)
		}
		if reqErr.StatusCode != int(envoy_type.StatusCode_ServiceUnavailable) {
			t.Errorf("status code = %d, want %d (503)", reqErr.StatusCode, envoy_type.StatusCode_ServiceUnavailable)
		}
		if want := "actor team-a/" + testUUID + " unavailable: " + desc; reqErr.Error() != want {
			t.Errorf("error body = %q, want %q", reqErr.Error(), want)
		}
	})
}

// TestHandleRequestHeaders_FullLotShedsParkedRequest verifies the other half
// of lot admission: when the lot is full, a request whose resume actually
// parks (first retryable failure) is shed with 503 "router at capacity" — at
// the park transition, after its single initial attempt, not before any.
//
// The shed keeps a ResourceExhausted retry failure as its cause, so the route
// metric reports no_capacity. It drops any other retry failure, so the route
// metric reports unavailable. parking.rejected carries the same outcome.
func TestHandleRequestHeaders_FullLotShedsParkedRequest(t *testing.T) {
	tests := []struct {
		name        string
		resumeErr   error
		wantCode    codes.Code
		wantOutcome string
	}{
		{
			name:        "ResourceExhausted is kept as the cause",
			resumeErr:   status.Error(codes.ResourceExhausted, "no worker has room for the actor"),
			wantCode:    codes.ResourceExhausted,
			wantOutcome: ateattr.RouterOutcomeNoCapacity,
		},
		{
			name:        "Aborted is dropped",
			resumeErr:   status.Error(codes.Aborted, "another operation is in progress"),
			wantCode:    codes.Unknown,
			wantOutcome: ateattr.RouterOutcomeUnavailable,
		},
		{
			name:        "Unavailable is dropped",
			resumeErr:   status.Error(codes.Unavailable, "ateapi restarting"),
			wantCode:    codes.Unknown,
			wantOutcome: ateattr.RouterOutcomeUnavailable,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const testUUID = "123e4567-e89b-12d3-a456-426614174000"
				authority := testUUID + ".team-a.actors.resources.substrate.ate.dev"

				var resumeCalls atomic.Int32
				clientMock := &mockClient{
					resumeFn: func(
						ctx context.Context,
						in *ateapi.ResumeActorRequest,
						opts ...grpc.CallOption,
					) (*ateapi.ResumeActorResponse, error) {
						resumeCalls.Add(1)
						return nil, tc.resumeErr
					},
				}

				parkMetrics, reader := newTestParkingMetrics(t)
				h := New(clientMock, ParkedRequestConfig{Budget: 500 * time.Millisecond, Max: 1}, parkMetrics)
				release, ok := h.parking.enter(context.Background(), ateattr.RouterOutcomeUnavailable)
				if !ok {
					t.Fatal("priming enter should be admitted")
				}
				defer release(parkOutcomeServed)

				md := requestMetadata(testUUID, "team-a",
					&corev3.HeaderValue{Key: ":authority", Value: authority},
				)

				_, err := h.HandleRequestHeaders(context.Background(), md)
				if err == nil {
					t.Fatal("expected error when a parked request finds the lot full")
				}
				var reqErr *extproc.ReqError
				if !errors.As(err, &reqErr) {
					t.Fatalf("expected *extproc.ReqError, got %T (%v)", err, err)
				}
				if reqErr.StatusCode != int(envoy_type.StatusCode_ServiceUnavailable) {
					t.Errorf("status code = %d, want %d (503)", reqErr.StatusCode, envoy_type.StatusCode_ServiceUnavailable)
				}
				if !strings.Contains(reqErr.Error(), "router at capacity") {
					t.Errorf("error body = %q, want it to mention capacity", reqErr.Error())
				}
				if got := status.Code(err); got != tc.wantCode {
					t.Errorf("status.Code(err) = %v, want %v", got, tc.wantCode)
				}
				if got := rejectedByOutcome(t, reader); len(got) != 1 || got[tc.wantOutcome] != 1 {
					t.Errorf("parking.rejected by outcome = %v, want {%s: 1}", got, tc.wantOutcome)
				}
				// Shedding happens at the park transition: exactly one attempt has run
				// when the caller is turned away.
				if got := resumeCalls.Load(); got != 1 {
					t.Errorf("expected the caller shed after exactly 1 attempt, got %d", got)
				}

				// The abandoned flight retries on until its budget, like any flight
				// whose callers left; sleep (fake time) past the budget so it exits
				// before the bubble does.
				time.Sleep(600 * time.Millisecond)
			})
		})
	}
}

// TestHandleRequestHeaders_ResponseHeadersDropWarmRoute routes a request, feeds
// the handler a response for it, and routes again to see whether the cached
// route survived.
func TestHandleRequestHeaders_ResponseHeadersDropWarmRoute(t *testing.T) {
	const localReply = "upstream_reset_before_response_started{connection_failure}"
	tests := []struct {
		name            string
		upstreamConnect bool
		status          string
		staleMarker     bool
		codeDetails     string
		wantDropped     bool
		// wantStatus is the status the handler answers with in the response's
		// place; 0 means the response goes through.
		wantStatus envoy_type.StatusCode
	}{
		{name: "actor response", status: "200", codeDetails: "via_upstream"},
		{name: "actor error", status: "503", codeDetails: "via_upstream"},
		{name: "actor's own 421", status: "421", codeDetails: "via_upstream"},
		{name: "worker refused the request", status: "421", staleMarker: true, codeDetails: "via_upstream", wantDropped: true, wantStatus: envoy_type.StatusCode_ServiceUnavailable},
		{name: "worker refused the request, no attributes", status: "421", staleMarker: true, wantDropped: true, wantStatus: envoy_type.StatusCode_ServiceUnavailable},
		{name: "marked 421 from inside a tunnel is the actor's", upstreamConnect: true, status: "421", staleMarker: true, codeDetails: "via_upstream"},
		{name: "Envoy could not reach the worker", upstreamConnect: true, status: "503", codeDetails: localReply, wantDropped: true},
		{name: "Envoy could not reach the worker, direct", status: "503", codeDetails: localReply, wantDropped: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			mock := &mockClient{
				resumeFn: func(context.Context, *ateapi.ResumeActorRequest, ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
					calls.Add(1)
					return &ateapi.ResumeActorResponse{Actor: warmRouteTestActor()}, nil
				},
			}
			h := New(mock, DefaultParkedRequestConfig(), nil, WithUpstreamConnect(tc.upstreamConnect), WithWarmRouteCacheTTL(30*time.Second))
			route := func() extproc.Result {
				t.Helper()
				res, err := h.HandleRequestHeaders(t.Context(), extproc.NewRequestMetadata([]*corev3.HeaderValue{
					{Key: atenet.TargetActorHeader, RawValue: []byte("team-a/actor")},
				}, nil))
				if err != nil {
					t.Fatalf("HandleRequestHeaders: %v", err)
				}
				return res
			}

			// First call populates the cache via ateapi; fresh resolves skip
			// response-header inspection to avoid an extra ext_proc round trip.
			first := route()
			if first.Resume != string(ResumeOutcomeNone) {
				t.Errorf("first Resume = %q, want %q", first.Resume, ResumeOutcomeNone)
			}
			if first.OnResponseHeaders != nil {
				t.Error("fresh resolve set OnResponseHeaders, want nil")
			}

			// Second call is served from the warm route cache and installs the
			// response-header eviction hook.
			res := route()
			if res.Resume != string(ResumeOutcomeCached) {
				t.Errorf("cached Resume = %q, want %q", res.Resume, ResumeOutcomeCached)
			}
			if res.OnResponseHeaders == nil {
				t.Fatal("cached hit OnResponseHeaders = nil, want hook")
			}
			uid := res.DynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[ActorUIDMetadataKey].GetStringValue()
			if uid != "uid-1" {
				t.Errorf("dynamic metadata actor UID = %q, want %q", uid, "uid-1")
			}

			headers := []*corev3.HeaderValue{{Key: ":status", RawValue: []byte(tc.status)}}
			if tc.staleMarker {
				headers = append(headers, &corev3.HeaderValue{Key: atenet.StaleAssignmentHeader, RawValue: []byte("true")})
			}
			var attrs map[string]*structpb.Struct
			if tc.codeDetails != "" {
				attrs = map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
					ResponseCodeDetailsAttribute: structpb.NewStringValue(tc.codeDetails),
				}}}
			}
			answer := res.OnResponseHeaders(extproc.NewRequestMetadata(headers, attrs))
			if got := answer.GetImmediateResponse().GetStatus().GetCode(); got != tc.wantStatus {
				t.Errorf("answered with status %v, want %v", got, tc.wantStatus)
			}

			route()
			if got, want := calls.Load() == 2, tc.wantDropped; got != want {
				t.Errorf("route dropped = %t, want %t (ateapi ResumeActor calls = %d)", got, want, calls.Load())
			}
		})
	}
}

func TestHandleRequestHeaders_StaleResponseDoesNotDropNewerPlacement(t *testing.T) {
	var calls atomic.Int32
	mock := &mockClient{
		resumeFn: func(context.Context, *ateapi.ResumeActorRequest, ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			actor := warmRouteTestActor()
			if calls.Add(1) >= 2 {
				actor.Metadata.Uid = "uid-2"
				actor.Status.WorkerAssignment.WorkerPodIps = []string{"10.0.0.53"}
			}
			return &ateapi.ResumeActorResponse{Actor: actor}, nil
		},
	}
	h := New(mock, DefaultParkedRequestConfig(), nil, WithWarmRouteCacheTTL(30*time.Second))
	route := func() extproc.Result {
		t.Helper()
		res, err := h.HandleRequestHeaders(t.Context(), extproc.NewRequestMetadata([]*corev3.HeaderValue{
			{Key: atenet.TargetActorHeader, RawValue: []byte("team-a/actor")},
		}, nil))
		if err != nil {
			t.Fatalf("HandleRequestHeaders: %v", err)
		}
		return res
	}
	localReply := extproc.NewRequestMetadata(
		[]*corev3.HeaderValue{{Key: ":status", RawValue: []byte("503")}},
		map[string]*structpb.Struct{"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
			ResponseCodeDetailsAttribute: structpb.NewStringValue("upstream_reset_before_response_started{connection_failure}"),
		}}},
	)

	// Populate uid-1, grab two cached hits referencing uid-1.
	route()
	hit1 := route()
	lateHit1 := route()

	// hit1 fails and evicts uid-1; next route resolves and caches uid-2.
	hit1.OnResponseHeaders(localReply)
	route()
	if calls.Load() != 2 {
		t.Fatalf("calls after re-resolve = %d, want 2", calls.Load())
	}

	// Late failure for uid-1 must leave uid-2 in the cache.
	lateHit1.OnResponseHeaders(localReply)
	after := route()
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2: late failure for uid-1 evicted uid-2", calls.Load())
	}
	if after.Resume != string(ResumeOutcomeCached) {
		t.Errorf("Resume = %q, want %q", after.Resume, ResumeOutcomeCached)
	}
	if got := dynamicMetadataTarget(after.DynamicMetadata); got != "10.0.0.53:443" {
		t.Errorf("target = %q, want 10.0.0.53:443", got)
	}
}
