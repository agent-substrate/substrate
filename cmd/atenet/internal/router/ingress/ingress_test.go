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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type mockClient struct {
	ateapipb.ControlClient
	resumeFn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *mockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	return m.resumeFn(ctx, in, opts...)
}

// authorityAttributes builds the ProcessingRequest.Attributes map the mux
// hands the handler -- the forwarded filter_state['dev.ate.authority']
// CEL attribute that xds.go's buildHcm (backed by authorityFilterStateFilter,
// or for CONNECT, connect_terminate's own capture) requests from Envoy. It
// replaces the :authority header as the source of routing truth; tests still
// set the header too, since RequestMetadata still logs it.
func authorityAttributes(t *testing.T, authority string) map[string]*structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(map[string]any{AuthorityFilterStateAttribute: authority})
	if err != nil {
		t.Fatalf("build authority attributes: %v", err)
	}
	return map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": s,
	}
}

// requestMetadata builds the metadata the ext_proc mux would hand the handler
// for a request with these headers, with authority forwarded via filter-state
// attribute the way Envoy actually delivers it (see authorityAttributes).
func requestMetadata(t *testing.T, authority string, headers ...*corev3.HeaderValue) *extproc.RequestMetadata {
	t.Helper()
	return extproc.NewRequestMetadata(headers, authorityAttributes(t, authority))
}

// dynamicMetadataTarget extracts the resolved worker address
// HandleRequestHeaders reports via OriginalDstMetadataKey/OriginalDstAddressKey.
func dynamicMetadataTarget(dynamicMetadata *structpb.Struct) string {
	return dynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstAddressKey].GetStringValue()
}

// dynamicMetadataPort extracts the target port HandleRequestHeaders reports
// via OriginalDstMetadataKey/OriginalDstPortKey. xds.go's buildRoutes derives
// a real atunnel.TargetPortHeader from this at the route level via a
// %DYNAMIC_METADATA(...)% format string; HandleRequestHeaders also sets that
// same header directly (see its own doc comment for why).
func dynamicMetadataPort(dynamicMetadata *structpb.Struct) string {
	return dynamicMetadata.GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstPortKey].GetStringValue()
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
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"}}}, nil
		},
	}, ParkedRequestConfig{}, nil)

	md := requestMetadata(t, authority,
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
		authority          string
		resumeResp         *ateapipb.ResumeActorResponse
		resumeErr          error
		expectErr          bool
		expectedErrStr     string
		expectedStatus     envoy_type.StatusCode
		expectedTarget     string
		expectedTargetPort string
	}{
		{
			name:           "invalid host returns 404 identifying the host",
			authority:      "invalid-host.com",
			expectErr:      true,
			expectedErrStr: `invalid host "invalid-host.com": invalid actor DNS name: must end with actors.resources.substrate.ate.dev, got "invalid-host.com"`,
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
			resumeErr:      status.Error(codes.FailedPrecondition, "no free workers available"),
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 unavailable: no free workers available`,
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
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "invalid-ip"},
				},
			},
			expectErr:      true,
			expectedErrStr: `actor team-a/123e4567-e89b-12d3-a456-426614174000 routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:      "Successful resume",
			authority: testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"},
				},
			},
			expectErr:          false,
			expectedTarget:     "10.0.0.52:443",
			expectedTargetPort: "80",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientMock := &mockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
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

			md := requestMetadata(t, tc.authority,
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
				t.Fatalf("expected exactly one header option (TargetPortHeader), found: %v", mutation.GetSetHeaders())
			}

			gotMutations := map[string]string{}
			for _, headerOption := range mutation.GetSetHeaders() {
				gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
			}
			if got := gotMutations[strings.ToLower(atunnel.TargetPortHeader)]; got != tc.expectedTargetPort {
				t.Errorf("target port mutation = %q, want %q", got, tc.expectedTargetPort)
			}
			if got := dynamicMetadataTarget(res.DynamicMetadata); got != tc.expectedTarget {
				t.Errorf("invalid destination mapping found: %s, expected: %s", got, tc.expectedTarget)
			}
			if got := dynamicMetadataPort(res.DynamicMetadata); got != tc.expectedTargetPort {
				t.Errorf("dynamic metadata port = %q, want %q", got, tc.expectedTargetPort)
			}
		})
	}
}

// TestHandleRequestHeadersHandlesConnectMethod locks in that a CONNECT request
// (used for atenet-router's arbitrary-port ingress support -- the target port
// travels in :authority, e.g. "<actor-dns>:9090") resolves the actor,
// produces the same "<workerIP>:443" original-dst mutation as an ordinary
// request (the router only ever dials the worker's atunnel server), and
// reports the arbitrary port itself via
// OriginalDstMetadataKey/OriginalDstPortKey, which xds.go's buildRoutes turns
// into atunnel.TargetPortHeader for atunnel.
func TestHandleRequestHeadersHandlesConnectMethod(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	authority := testUUID + ".team-a.actors.resources.substrate.ate.dev:9090"

	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"}}}, nil
		},
	}
	h := New(clientMock, ParkedRequestConfig{}, nil)

	// CONNECT requests carry no :path; the request-target lives in :authority.
	md := requestMetadata(t, authority,
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

// TestHandleRequestHeaders_ParkingLotFull verifies that when the parking lot is at capacity
// the request is shed with a 503 before any resume is attempted.
func TestHandleRequestHeaders_ParkingLotFull(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	authority := testUUID + ".team-a.actors.resources.substrate.ate.dev"

	var resumeCalled bool
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			resumeCalled = true
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"}}}, nil
		},
	}

	// A 1-slot lot with the slot already occupied deterministically simulates a
	// full lot without needing a concurrent in-flight request.
	h := New(clientMock, ParkedRequestConfig{Budget: time.Second, Max: 1}, nil)
	release, ok := h.parking.enter(context.Background())
	if !ok {
		t.Fatal("priming enter should be admitted")
	}
	defer release(parkOutcomeServed)

	md := requestMetadata(t, authority,
		&corev3.HeaderValue{Key: ":authority", Value: authority},
	)

	_, err := h.HandleRequestHeaders(context.Background(), md)
	if err == nil {
		t.Fatal("expected error when parking lot is full")
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
	if resumeCalled {
		t.Error("resume must not be attempted for a shed request")
	}
}
