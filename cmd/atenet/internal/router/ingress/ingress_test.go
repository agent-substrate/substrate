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
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// requestMetadata builds the metadata the ext_proc mux would hand the handler
// for a request with these headers.
func requestMetadata(headers ...*corev3.HeaderValue) *extproc.RequestMetadata {
	return extproc.NewRequestMetadata(headers)
}

func TestHandleRequestHeadersDoesNotLogSensitiveData(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	const secret = "do-not-log-me"

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	h := New(&mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"}}}, nil
		},
	}, ParkedRequestConfig{}, nil, false)

	md := requestMetadata(
		&corev3.HeaderValue{Key: ":path", Value: "/api/v1/reset?token=" + secret},
		&corev3.HeaderValue{Key: ":authority", Value: testUUID + ".team-a.actors.resources.substrate.ate.dev"},
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
		name           string
		authority      string
		resumeResp     *ateapipb.ResumeActorResponse
		resumeErr      error
		expectErr      bool
		expectedErrStr string
		expectedStatus envoy_type.StatusCode
		expectedTarget string
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
			expectErr:      false,
			expectedTarget: "10.0.0.52:443",
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
			h := New(clientMock, ParkedRequestConfig{}, nil, false)

			md := requestMetadata(
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
			if len(mutation.GetSetHeaders()) != 2 {
				t.Fatalf("expected exactly two header options, found: %v", mutation.GetSetHeaders())
			}

			gotMutations := map[string]string{}
			for _, headerOption := range mutation.GetSetHeaders() {
				gotMutations[strings.ToLower(headerOption.Header.Key)] = string(headerOption.Header.RawValue)
			}
			if got := gotMutations[OriginalDstHeader]; got != tc.expectedTarget {
				t.Errorf("destination mutation = %q, want %q", got, tc.expectedTarget)
			}
			if got := gotMutations[strings.ToLower(atunnel.OriginalHostHeader)]; got != tc.authority {
				t.Errorf("original host mutation = %q, want %q", got, tc.authority)
			}

		})
	}
}

// TestHandleRequestHeaders_ParkingLotFull verifies that when the parking lot is at capacity
// the request is shed with a 503 before any resume is attempted.
func TestHandleRequestHeaders_ParkingLotFull(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"

	var resumeCalled bool
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			resumeCalled = true
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.1"}}}, nil
		},
	}

	// A 1-slot lot with the slot already occupied deterministically simulates a
	// full lot without needing a concurrent in-flight request.
	h := New(clientMock, ParkedRequestConfig{Budget: time.Second, Max: 1}, nil, false)
	release, ok := h.parking.enter(context.Background())
	if !ok {
		t.Fatal("priming enter should be admitted")
	}
	defer release(parkOutcomeServed)

	md := requestMetadata(
		&corev3.HeaderValue{Key: ":authority", Value: testUUID + ".team-a.actors.resources.substrate.ate.dev"},
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

func TestAddRoutingMutationsViaAuthority(t *testing.T) {
	mutation := &extprocv3.HeaderMutation{}
	addRoutingMutations("10.0.0.52:443", "actor-1.team-a.actors.resources.substrate.ate.dev", true, mutation)

	got := map[string]string{}
	for _, option := range mutation.GetSetHeaders() {
		if option.GetAppendAction() != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
			t.Errorf("mutation %q append action = %v, want overwrite", option.GetHeader().GetKey(), option.GetAppendAction())
		}
		got[strings.ToLower(option.GetHeader().GetKey())] = string(option.GetHeader().GetRawValue())
	}
	if got[OriginalDstHeader] != "10.0.0.52:443" {
		t.Errorf("%s = %q", OriginalDstHeader, got[OriginalDstHeader])
	}
	if got[strings.ToLower(atunnel.OriginalHostHeader)] != "actor-1.team-a.actors.resources.substrate.ate.dev" {
		t.Errorf("%s = %q", atunnel.OriginalHostHeader, got[strings.ToLower(atunnel.OriginalHostHeader)])
	}
	if got[extproc.AuthorityHeader] != "10.0.0.52:443" {
		t.Errorf("%s = %q", extproc.AuthorityHeader, got[extproc.AuthorityHeader])
	}
}
