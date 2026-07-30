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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

func TestHandleRequestHeadersDoesNotLogSensitiveData(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	const secret = "do-not-log-me"

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	s := NewExtProcServer(50051, &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{AteomPodIp: "10.0.0.52"}}, nil
		},
	}, nil, ParkedRequestConfig{}, nil, false)

	reqHeaders := &extprocv3.HttpHeaders{
		Headers: &corev3.HeaderMap{
			Headers: []*corev3.HeaderValue{
				{Key: ":path", Value: "/api/v1/reset?token=" + secret},
				{Key: ":authority", Value: testUUID + ".team-a.actors.resources.substrate.ate.dev"},
				{Key: ":method", Value: "POST"},
				{Key: "authorization", Value: "Bearer " + secret},
				{Key: "cookie", Value: "session=" + secret},
			},
		},
	}

	_, metadata, target, _, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders)
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

	s.recorder.AddRouterRequest(time.Now(), time.Millisecond, "Route ok", target, metadata)
	for _, q := range s.recorder.Get() {
		if blob, _ := json.Marshal(q); strings.Contains(string(blob), secret) {
			t.Errorf("recorder/statusz retained sensitive value: %s", blob)
		}
	}
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
					AteomPodIp: "invalid-ip",
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
					AteomPodIp: "10.0.0.52",
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
			// is covered separately in TestExtProc_ParkingLotFull and resumer_test.go.
			s := NewExtProcServer(50051, clientMock, nil, ParkedRequestConfig{}, nil, false)

			reqHeaders := &extprocv3.HttpHeaders{
				Headers: &corev3.HeaderMap{
					Headers: []*corev3.HeaderValue{
						{Key: ":path", Value: "/v1/actors/invoke"},
						{Key: ":authority", Value: tc.authority},
						{Key: ":method", Value: "POST"},
					},
				},
			}

			res, metadata, target, _, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders)
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

	// A 1-slot lot with the slot already occupied deterministically simulates a
	// full lot without needing a concurrent in-flight request.
	s := NewExtProcServer(50051, clientMock, nil, ParkedRequestConfig{Budget: time.Second, Max: 1}, nil, false)
	release, ok := s.parking.enter(context.Background())
	if !ok {
		t.Fatal("priming enter should be admitted")
	}
	defer release(parkOutcomeServed)

	reqHeaders := &extprocv3.HttpHeaders{
		Headers: &corev3.HeaderMap{
			Headers: []*corev3.HeaderValue{
				{Key: ":authority", Value: testUUID + ".team-a.actors.resources.substrate.ate.dev"},
			},
		},
	}

	_, _, _, _, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders)
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

func TestClassifyOutcome(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error maps to ok",
			err:      nil,
			expected: "ok",
		},
		{
			name:     "context Canceled maps to cancelled",
			err:      context.Canceled,
			expected: "cancelled",
		},
		{
			name:     "context DeadlineExceeded maps to timeout",
			err:      context.DeadlineExceeded,
			expected: "timeout",
		},
		{
			name:     "FailedPrecondition gRPC code maps to no_capacity",
			err:      status.Error(codes.FailedPrecondition, "capacity full"),
			expected: "no_capacity",
		},
		{
			name:     "Aborted gRPC code maps to lock_conflict",
			err:      status.Error(codes.Aborted, "lock conflict"),
			expected: "lock_conflict",
		},
		{
			name:     "NotFound gRPC code maps to not_found",
			err:      status.Error(codes.NotFound, "missing"),
			expected: "not_found",
		},
		{
			name:     "Unavailable gRPC code maps to unavailable",
			err:      status.Error(codes.Unavailable, "control-plane down"),
			expected: "unavailable",
		},
		{
			name:     "ResourceExhausted gRPC code maps to rate_limited",
			err:      status.Error(codes.ResourceExhausted, "rate limit exceeded"),
			expected: "rate_limited",
		},
		{
			name:     "StatusCode_NotFound reqError maps to not_found",
			err:      newReqError(envoy_type.StatusCode_NotFound, "missing"),
			expected: "not_found",
		},
		{
			name:     "StatusCode_ServiceUnavailable reqError maps to no_capacity",
			err:      newReqError(envoy_type.StatusCode_ServiceUnavailable, "no free workers"),
			expected: "no_capacity",
		},
		{
			name:     "StatusCode_TooManyRequests reqError maps to rate_limited",
			err:      newReqError(envoy_type.StatusCode_TooManyRequests, "rate limited"),
			expected: "rate_limited",
		},
		{
			name:     "Unknown error maps to resume_error",
			err:      errors.New("internal storage glitch"),
			expected: "resume_error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyOutcome(tc.err); got != tc.expected {
				t.Errorf("classifyOutcome(%v) = %q, want %q", tc.err, got, tc.expected)
			}
		})
	}
}

func TestRecordRouteDuration_Attributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	h, err := mp.Meter("atenet-router").Float64Histogram(routeDurationMetricName)
	if err != nil {
		t.Fatalf("failed to create histogram: %v", err)
	}

	s := NewExtProcServer(50051, nil, h, ParkedRequestConfig{}, nil, false)
	s.recordRouteDuration(context.Background(), 10*time.Millisecond, "team-a-ns", "tmpl-a", classifyOutcome(nil), string(ResumeOutcomeTriggered))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	dp := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64]).DataPoints[0]
	wantAttrs := map[string]string{
		"ate.template.namespace": "team-a-ns",
		"ate.template.name":      "tmpl-a",
		"ate.router.outcome":     "ok",
		"ate.router.resume":      "triggered",
	}

	for k, want := range wantAttrs {
		val, exists := dp.Attributes.Value(attribute.Key(k))
		if !exists {
			t.Errorf("missing metric attribute %q", k)
		} else if val.AsString() != want {
			t.Errorf("attribute %q = %q, want %q", k, val.AsString(), want)
		}
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
	if got[":authority"] != "10.0.0.52:443" {
		t.Errorf(":authority = %q", got[":authority"])
	}
}
