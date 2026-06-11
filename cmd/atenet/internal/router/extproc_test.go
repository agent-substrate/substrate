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

package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockClient struct {
	ateapipb.ControlClient
	resumeFn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *mockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	return m.resumeFn(ctx, in, opts...)
}

func TestExtProcHeadersEvaluation(t *testing.T) {
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
			expectedErrStr: `invalid host "invalid-host.com": invalid actor_id: must end with actors.resources.substrate.ate.dev, got "invalid-host.com"`,
			expectedStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:           "non-gRPC resume error collapses to 500 without leaking detail",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      errors.New("resume failed with sensitive detail"),
			expectErr:      true,
			expectedErrStr: `error resuming actor "123e4567-e89b-12d3-a456-426614174000"`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:           "FailedPrecondition maps to 503 with preserved desc",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.FailedPrecondition, "no free workers available"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" unavailable: no free workers available`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "NotFound maps to 404",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.NotFound, "actor missing"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" not found`,
			expectedStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:           "Unavailable maps to 503",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.Unavailable, "control-plane down"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" unavailable`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "DeadlineExceeded maps to 504",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.DeadlineExceeded, "deadline"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" request timed out`,
			expectedStatus: envoy_type.StatusCode_GatewayTimeout,
		},
		{
			name:      "Bad Actor IP from resume returns 500 without leaking IP",
			authority: testUUID + ".actors.resources.substrate.ate.dev",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "invalid-ip",
				},
			},
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:      "Successful resume",
			authority: testUUID + ".actors.resources.substrate.ate.dev",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "10.0.0.52",
				},
			},
			expectErr:      false,
			expectedTarget: "10.0.0.52:80",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientMock := &mockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					if in.GetActorId() != testUUID {
						t.Errorf("unexpected identifier parsed in test context: %s", in.GetActorId())
					}
					if tc.resumeErr != nil {
						return nil, tc.resumeErr
					}
					return tc.resumeResp, nil
				},
			}

			// Parking disabled: these cases assert fail-fast mapping of resume
			// errors (e.g. FailedPrecondition -> immediate 503). Parking behavior
			// is covered separately in TestExtProc_ParkingLotFull and resumer_test.go.
			s := NewExtProcServer(50051, clientMock, nil, parkingConfig{}, nil)

			reqHeaders := &extprocv3.HttpHeaders{
				Headers: &corev3.HeaderMap{
					Headers: []*corev3.HeaderValue{
						{Key: ":path", Value: "/v1/actors/invoke"},
						{Key: ":authority", Value: tc.authority},
						{Key: ":method", Value: "POST"},
					},
				},
			}

			res, metadata, target, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if tc.expectedErrStr != "" && err.Error() != tc.expectedErrStr {
					t.Errorf("client body mismatch:\n  got:  %q\n  want: %q", err.Error(), tc.expectedErrStr)
				}
				var reqErr *reqError
				if !errors.As(err, &reqErr) {
					t.Fatalf("expected *reqError, got %T (%v)", err, err)
				}
				if got, want := reqErr.statusCode, int(tc.expectedStatus); got != want {
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
			if target != tc.expectedTarget {
				t.Errorf("expected target %q, got %q", tc.expectedTarget, target)
			}

			mutation := res.Response.GetHeaderMutation()
			if len(mutation.GetSetHeaders()) != 1 {
				t.Fatalf("expected exactly one Header option set, found: %v", mutation.GetSetHeaders())
			}

			headerOption := mutation.GetSetHeaders()[0]
			if strings.ToLower(headerOption.Header.Key) != ":authority" {
				t.Errorf("invalid resulting dynamic parameter key: %s", headerOption.Header.Key)
			}

			if string(headerOption.Header.RawValue) != tc.expectedTarget {
				t.Errorf("invalid destination mapping found: %s, expected: %s", headerOption.Header.RawValue, tc.expectedTarget)
			}

			// Confirm that query logs recorded metric trace details
			s.recorder.AddRouterRequest(time.Now(), 10*time.Millisecond, "Route ok", tc.expectedTarget, metadata)
			queries := s.recorder.Get()
			if len(queries) != 1 {
				t.Errorf("expected query trace entries, got: %v", queries)
			}
		})
	}
}

// TestExtProc_ParkingLotFull verifies that when the parking lot is at capacity
// the request is shed with a 503 before any resume is attempted.
func TestExtProc_ParkingLotFull(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"

	var resumeCalled bool
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			resumeCalled = true
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{AteomPodIp: "10.0.0.1"}}, nil
		},
	}

	// maxParked == 0 with parking enabled rejects every admission, deterministically
	// simulating a full lot without needing concurrent in-flight requests.
	s := NewExtProcServer(50051, clientMock, nil, parkingConfig{enabled: true, maxWait: time.Second, maxParked: 0}, nil)

	reqHeaders := &extprocv3.HttpHeaders{
		Headers: &corev3.HeaderMap{
			Headers: []*corev3.HeaderValue{
				{Key: ":authority", Value: testUUID + ".actors.resources.substrate.ate.dev"},
			},
		},
	}

	_, _, _, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders)
	if err == nil {
		t.Fatal("expected error when parking lot is full")
	}
	var reqErr *reqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("expected *reqError, got %T (%v)", err, err)
	}
	if reqErr.statusCode != int(envoy_type.StatusCode_ServiceUnavailable) {
		t.Errorf("status code = %d, want %d (503)", reqErr.statusCode, envoy_type.StatusCode_ServiceUnavailable)
	}
	if !strings.Contains(reqErr.Error(), "router at capacity") {
		t.Errorf("error body = %q, want it to mention capacity", reqErr.Error())
	}
	if resumeCalled {
		t.Error("resume must not be attempted for a shed request")
	}
}
