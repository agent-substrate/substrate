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
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

// mockResolver is a test double for RouteResolver.
type mockResolver struct {
	result RouteResolution
}

func (m *mockResolver) ResolveRoute(_ context.Context, _ RouteRequest) RouteResolution {
	return m.result
}

// mockClient satisfies ateapipb.ControlClient for use in resolver construction
// tests that still exercise the full ActorRouteResolver path.
type mockClient struct {
	ateapipb.ControlClient
	resumeFn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *mockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	return m.resumeFn(ctx, in, opts...)
}

func makeReqHeaders(authority, path string, extra ...[2]string) *extprocv3.HttpHeaders {
	headers := []*corev3.HeaderValue{
		{Key: ":path", Value: path},
		{Key: ":authority", Value: authority},
		{Key: ":method", Value: "POST"},
	}
	for _, kv := range extra {
		headers = append(headers, &corev3.HeaderValue{Key: kv[0], Value: kv[1]})
	}
	return &extprocv3.HttpHeaders{
		Headers: &corev3.HeaderMap{Headers: headers},
	}
}

// TestHandleRequestHeadersDoesNotLogSensitiveData verifies that the ExtProc
// adapter layer does not leak secrets (auth tokens, cookies, query params) to
// logs or the query recorder, while still logging routing context (actor ID).
func TestHandleRequestHeadersDoesNotLogSensitiveData(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	const secret = "do-not-log-me"

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	resolver := &mockResolver{result: RouteResolution{Success: &RouteSuccess{
		ActorID: testUUID,
		Backend: Backend{IP: "10.0.0.52", Port: 80},
		TemplateRef: ActorTemplateRef{},
	}}}
	s := NewExtProcServer(50051, resolver, nil)

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

	_, metadata, res, err := s.handleRequestHeaders(context.Background(), reqHeaders)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	if strings.Contains(out, secret) {
		t.Errorf("router log leaked sensitive value: %s", out)
	}

	target := res.Success.Backend.IP
	s.recorder.AddRouterRequest(time.Now(), time.Millisecond, "Route ok", target, metadata)
	for _, q := range s.recorder.Get() {
		if blob, _ := json.Marshal(q); strings.Contains(string(blob), secret) {
			t.Errorf("recorder/statusz retained sensitive value: %s", blob)
		}
	}
}

// TestExtProcAdapterMapping verifies that the ExtProc adapter correctly maps
// RouteResolver outcomes to the extproc protocol (header mutation on success,
// immediate response on denial).
func TestExtProcAdapterMapping(t *testing.T) {
	const workerIP = "10.0.0.52"
	const actorID = "123e4567-e89b-12d3-a456-426614174000"

	tests := []struct {
		name           string
		resolution     RouteResolution
		wantErr        bool
		wantStatus     envoy_type.StatusCode
		wantMsgContain string
		wantTarget     string
	}{
		{
			name: "success maps to :authority header mutation",
			resolution: RouteResolution{Success: &RouteSuccess{
				ActorID: actorID,
				Backend: Backend{IP: workerIP, Port: 80},
				TemplateRef: ActorTemplateRef{Namespace: "ns", Name: "tmpl"},
			}},
			wantTarget: workerIP + ":80",
		},
		{
			name: "404 denial becomes reqError with correct status",
			resolution: RouteResolution{Denial: &RouteDenial{
				HTTPStatus:  http.StatusNotFound,
				Message:     "actor not found",
				OutcomeCode: "not_found",
			}},
			wantErr:        true,
			wantStatus:     envoy_type.StatusCode_NotFound,
			wantMsgContain: "actor not found",
		},
		{
			name: "503 denial becomes reqError with correct status",
			resolution: RouteResolution{Denial: &RouteDenial{
				HTTPStatus:  http.StatusServiceUnavailable,
				Message:     "actor unavailable: no free workers",
				OutcomeCode: "error",
			}},
			wantErr:        true,
			wantStatus:     envoy_type.StatusCode_ServiceUnavailable,
			wantMsgContain: "no free workers",
		},
		{
			name: "500 denial becomes reqError with correct status",
			resolution: RouteResolution{Denial: &RouteDenial{
				HTTPStatus:  http.StatusInternalServerError,
				Message:     "actor routing failed",
				OutcomeCode: "error",
			}},
			wantErr:        true,
			wantStatus:     envoy_type.StatusCode_InternalServerError,
			wantMsgContain: "routing failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewExtProcServer(50051, &mockResolver{result: tc.resolution}, nil)
			reqHeaders := makeReqHeaders(actorID+".team-a.actors.resources.substrate.ate.dev", "/v1/invoke")

			hResp, _, res, err := s.handleRequestHeaders(context.Background(), reqHeaders)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				var re *reqError
				if !isReqError(err, &re) {
					t.Fatalf("expected *reqError, got %T: %v", err, err)
				}
				if re.statusCode != int(tc.wantStatus) {
					t.Errorf("statusCode = %d, want %d", re.statusCode, tc.wantStatus)
				}
				if !strings.Contains(err.Error(), tc.wantMsgContain) {
					t.Errorf("error message %q does not contain %q", err.Error(), tc.wantMsgContain)
				}
				_ = res
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			mutation := hResp.Response.GetHeaderMutation()
			if len(mutation.GetSetHeaders()) != 1 {
				t.Fatalf("expected 1 header mutation, got %d", len(mutation.GetSetHeaders()))
			}
			hv := mutation.GetSetHeaders()[0].Header
			if strings.ToLower(hv.Key) != ":authority" {
				t.Errorf("mutation key = %q, want :authority", hv.Key)
			}
			if string(hv.RawValue) != tc.wantTarget {
				t.Errorf("mutation value = %q, want %q", hv.RawValue, tc.wantTarget)
			}
		})
	}
}

func isReqError(err error, out **reqError) bool {
	if err == nil {
		return false
	}
	if re, ok := err.(*reqError); ok {
		if out != nil {
			*out = re
		}
		return true
	}
	return false
}
