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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
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
	}, nil)

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

	_, metadata, target, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders, nil)
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

// tlsAttrs builds the attributes Envoy attaches to a ProcessingRequest when the
// downstream TLS version is requested. An empty version yields no attributes at
// all, which is what Envoy sends for a plaintext connection.
func tlsAttrs(version string) map[string]*structpb.Struct {
	if version == "" {
		return nil
	}
	return map[string]*structpb.Struct{
		extProcFilterName: {
			Fields: map[string]*structpb.Value{
				tlsVersionAttribute: structpb.NewStringValue(version),
			},
		},
	}
}

func TestExtProcHeadersEvaluation(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"

	tests := []struct {
		name      string
		authority string
		// tlsVersion is the value of the connection.tls_version attribute Envoy
		// reports; "" stands for the attribute being absent altogether, which is
		// what a plaintext connection produces.
		tlsVersion string
		// extraHeaders are merged into the request on top of the base set, to
		// check that caller-supplied headers cannot influence routing.
		extraHeaders   map[string]string
		resumeResp     *ateapipb.ResumeActorResponse
		resumeErr      error
		expectErr      bool
		expectedErrStr string
		expectedStatus envoy_type.StatusCode
		expectedTarget string
		// expectedSNI is the value expected in SNIHeader; "" means the header
		// must not be set at all.
		expectedSNI string
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
			expectedErrStr: `error resuming actor "123e4567-e89b-12d3-a456-426614174000"`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:           "FailedPrecondition maps to 503 with preserved desc",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.FailedPrecondition, "no free workers available"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" unavailable: no free workers available`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "NotFound maps to 404",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.NotFound, "actor missing"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" not found`,
			expectedStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:           "Unavailable maps to 503",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.Unavailable, "control-plane down"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" unavailable`,
			expectedStatus: envoy_type.StatusCode_ServiceUnavailable,
		},
		{
			name:           "DeadlineExceeded maps to 504",
			authority:      testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeErr:      status.Error(codes.DeadlineExceeded, "deadline"),
			expectErr:      true,
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" request timed out`,
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
			expectedErrStr: `actor "123e4567-e89b-12d3-a456-426614174000" routing failed`,
			expectedStatus: envoy_type.StatusCode_InternalServerError,
		},
		{
			name:       "Successful resume",
			authority:  testUUID + ".team-a.actors.resources.substrate.ate.dev",
			tlsVersion: "",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "10.0.0.52",
				},
			},
			expectErr:      false,
			expectedTarget: "10.0.0.52:80",
		},
		{
			name:       "https ingress is re-originated to port 443 with the actor mesh name as SNI",
			authority:  testUUID + ".team-a.actors.resources.substrate.ate.dev",
			tlsVersion: "TLSv1.3",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "10.0.0.52",
				},
			},
			expectedTarget: "10.0.0.52:443",
			expectedSNI:    testUUID + ".team-a.actors.resources.substrate.ate.dev",
		},
		{
			name:      "a missing tls version attribute falls back to plaintext",
			authority: testUUID + ".team-a.actors.resources.substrate.ate.dev",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "10.0.0.52",
				},
			},
			expectedTarget: "10.0.0.52:80",
		},
		{
			name:       "caller-supplied proto and SNI headers cannot upgrade a plaintext request",
			authority:  testUUID + ".team-a.actors.resources.substrate.ate.dev",
			tlsVersion: "",
			extraHeaders: map[string]string{
				"x-forwarded-proto": "https",
				SNIHeader:           "evil.example",
			},
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "10.0.0.52",
				},
			},
			expectedTarget: "10.0.0.52:80",
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

			s := NewExtProcServer(50051, clientMock, nil)

			headers := []*corev3.HeaderValue{
				{Key: ":path", Value: "/v1/actors/invoke"},
				{Key: ":authority", Value: tc.authority},
				{Key: ":method", Value: "POST"},
			}
			for k, v := range tc.extraHeaders {
				headers = append(headers, &corev3.HeaderValue{Key: k, Value: v})
			}
			reqHeaders := &extprocv3.HttpHeaders{
				Headers: &corev3.HeaderMap{Headers: headers},
			}

			res, metadata, target, _, _, err := s.handleRequestHeaders(context.Background(), reqHeaders, tlsAttrs(tc.tlsVersion))
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
			set := map[string]*corev3.HeaderValueOption{}
			for _, h := range mutation.GetSetHeaders() {
				set[strings.ToLower(h.GetHeader().GetKey())] = h
			}

			wantHeaders := 1
			if tc.expectedSNI != "" {
				wantHeaders = 2
			}
			if len(set) != wantHeaders {
				t.Fatalf("expected %d Header options set, found: %v", wantHeaders, mutation.GetSetHeaders())
			}

			authority, ok := set[":authority"]
			if !ok {
				t.Fatalf("no :authority mutation found in: %v", mutation.GetSetHeaders())
			}
			if got := string(authority.GetHeader().GetRawValue()); got != tc.expectedTarget {
				t.Errorf("invalid destination mapping found: %s, expected: %s", got, tc.expectedTarget)
			}

			sni, ok := set[SNIHeader]
			if tc.expectedSNI == "" {
				if ok {
					t.Errorf("plaintext request must not set %s, got %q", SNIHeader, sni.GetHeader().GetRawValue())
				}
			} else {
				if !ok {
					t.Fatalf("no %s mutation found in: %v", SNIHeader, mutation.GetSetHeaders())
				}
				if got := string(sni.GetHeader().GetRawValue()); got != tc.expectedSNI {
					t.Errorf("SNI = %q, want %q", got, tc.expectedSNI)
				}
				// Anything but an overwrite would keep a caller-supplied value
				// alongside ours, letting it reach the upstream handshake.
				if got := sni.GetAppendAction(); got != corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD {
					t.Errorf("SNI append action = %v, want OVERWRITE_IF_EXISTS_OR_ADD", got)
				}
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
