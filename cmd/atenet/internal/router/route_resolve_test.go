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
	"net/http"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testActorUUID = "123e4567-e89b-12d3-a456-426614174000"

func newTestResolver(fn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)) *ActorRouteResolver {
	client := &mockClient{resumeFn: fn}
	return NewActorRouteResolver(NewActorResumer(client))
}

func TestResolveRoute_InvalidHost(t *testing.T) {
	t.Parallel()
	r := newTestResolver(nil)
	res := r.ResolveRoute(context.Background(), RouteRequest{Authority: "invalid-host.com"})
	if res.Denial == nil {
		t.Fatal("expected Denial, got Success")
	}
	if res.Denial.HTTPStatus != http.StatusNotFound {
		t.Errorf("HTTPStatus = %d, want 404", res.Denial.HTTPStatus)
	}
	want := `invalid host "invalid-host.com": invalid actor DNS name: must end with actors.resources.substrate.ate.dev, got "invalid-host.com"`
	if res.Denial.Message != want {
		t.Errorf("Message = %q, want %q", res.Denial.Message, want)
	}
	if res.Denial.OutcomeCode != "not_found" {
		t.Errorf("OutcomeCode = %q, want not_found", res.Denial.OutcomeCode)
	}
}

func TestResolveRoute_InvalidHostPreservesCause(t *testing.T) {
	t.Parallel()
	r := newTestResolver(nil)
	res := r.ResolveRoute(context.Background(), RouteRequest{Authority: "no-suffix.example.com"})
	if res.Denial == nil {
		t.Fatal("expected Denial")
	}
	if res.Denial.Cause == nil {
		t.Error("Cause should be non-nil for log inspection")
	}
}

func TestResolveRoute_ResumeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resumeErr   error
		wantStatus  int
		wantMsgHas  string
		wantCode    string
		causeIsOrig bool
	}{
		{
			name:        "NotFound maps to 404",
			resumeErr:   status.Error(codes.NotFound, "actor missing"),
			wantStatus:  http.StatusNotFound,
			wantMsgHas:  `actor "` + testActorUUID + `" not found`,
			wantCode:    "not_found",
			causeIsOrig: true,
		},
		{
			name:        "FailedPrecondition maps to 503 with preserved desc",
			resumeErr:   status.Error(codes.FailedPrecondition, "no free workers available"),
			wantStatus:  http.StatusServiceUnavailable,
			wantMsgHas:  `actor "` + testActorUUID + `" unavailable: no free workers available`,
			wantCode:    "error",
			causeIsOrig: true,
		},
		{
			name:        "Unavailable maps to 503",
			resumeErr:   status.Error(codes.Unavailable, "control-plane down"),
			wantStatus:  http.StatusServiceUnavailable,
			wantMsgHas:  `actor "` + testActorUUID + `" unavailable`,
			wantCode:    "error",
			causeIsOrig: true,
		},
		{
			name:        "DeadlineExceeded maps to 504",
			resumeErr:   status.Error(codes.DeadlineExceeded, "deadline"),
			wantStatus:  http.StatusGatewayTimeout,
			wantMsgHas:  `actor "` + testActorUUID + `" request timed out`,
			wantCode:    "cancelled",
			causeIsOrig: true,
		},
		{
			name:        "PermissionDenied maps to 403",
			resumeErr:   status.Error(codes.PermissionDenied, "denied"),
			wantStatus:  http.StatusForbidden,
			wantMsgHas:  `actor "` + testActorUUID + `" access denied`,
			wantCode:    "error",
			causeIsOrig: true,
		},
		{
			name:        "Unauthenticated maps to 401",
			resumeErr:   status.Error(codes.Unauthenticated, "no creds"),
			wantStatus:  http.StatusUnauthorized,
			wantMsgHas:  `actor "` + testActorUUID + `" authentication required`,
			wantCode:    "error",
			causeIsOrig: true,
		},
		{
			name:        "ResourceExhausted maps to 429",
			resumeErr:   status.Error(codes.ResourceExhausted, "quota"),
			wantStatus:  http.StatusTooManyRequests,
			wantMsgHas:  `actor "` + testActorUUID + `" rate limited`,
			wantCode:    "error",
			causeIsOrig: true,
		},
		{
			name:        "non-gRPC error collapses to 500 without leaking detail",
			resumeErr:   errors.New("resume failed with sensitive detail"),
			wantStatus:  http.StatusInternalServerError,
			wantMsgHas:  `error resuming actor "` + testActorUUID + `"`,
			wantCode:    "error",
			causeIsOrig: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newTestResolver(func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				if in.GetActorRef().GetName() != testActorUUID {
					t.Errorf("unexpected actor ID: %q", in.GetActorRef().GetName())
				}
				if in.GetActorRef().GetAtespace() != "team-a" {
					t.Errorf("unexpected atespace: %q", in.GetActorRef().GetAtespace())
				}
				return nil, tc.resumeErr
			})

			res := r.ResolveRoute(context.Background(), RouteRequest{
				Authority: testActorUUID + ".team-a.actors.resources.substrate.ate.dev",
			})

			if res.Denial == nil {
				t.Fatal("expected Denial, got Success")
			}
			d := res.Denial
			if d.HTTPStatus != tc.wantStatus {
				t.Errorf("HTTPStatus = %d, want %d", d.HTTPStatus, tc.wantStatus)
			}
			if tc.wantMsgHas != "" && d.Message != tc.wantMsgHas {
				t.Errorf("Message = %q, want %q", d.Message, tc.wantMsgHas)
			}
			if d.OutcomeCode != tc.wantCode {
				t.Errorf("OutcomeCode = %q, want %q", d.OutcomeCode, tc.wantCode)
			}
			if tc.causeIsOrig && !errors.Is(d, tc.resumeErr) {
				t.Errorf("errors.Is(denial, original) = false; original error must be preserved for logs")
			}
		})
	}
}

func TestResolveRoute_BadWorkerIP(t *testing.T) {
	t.Parallel()
	r := newTestResolver(func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
		return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{AteomPodIp: "invalid-ip"}}, nil
	})

	res := r.ResolveRoute(context.Background(), RouteRequest{
		Authority: testActorUUID + ".team-a.actors.resources.substrate.ate.dev",
	})

	if res.Denial == nil {
		t.Fatal("expected Denial for invalid IP, got Success")
	}
	if res.Denial.HTTPStatus != http.StatusInternalServerError {
		t.Errorf("HTTPStatus = %d, want 500", res.Denial.HTTPStatus)
	}
	want := `actor "` + testActorUUID + `" routing failed`
	if res.Denial.Message != want {
		t.Errorf("Message = %q, want %q", res.Denial.Message, want)
	}
}

func TestResolveRoute_Success(t *testing.T) {
	t.Parallel()
	const workerIP = "10.0.0.52"
	const tmplNs = "ate-system"
	const tmplName = "my-template"

	r := newTestResolver(func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
		return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{
			AteomPodIp:             workerIP,
			ActorTemplateNamespace: tmplNs,
			ActorTemplateName:      tmplName,
		}}, nil
	})

	res := r.ResolveRoute(context.Background(), RouteRequest{
		Authority: testActorUUID + ".team-a.actors.resources.substrate.ate.dev",
	})

	if res.Success == nil {
		t.Fatalf("expected Success, got Denial: %+v", res.Denial)
	}
	s := res.Success
	if s.ActorID != testActorUUID {
		t.Errorf("ActorID = %q, want %q", s.ActorID, testActorUUID)
	}
	if s.Backend.IP != workerIP {
		t.Errorf("Backend.IP = %q, want %q", s.Backend.IP, workerIP)
	}
	if s.Backend.Port != 80 {
		t.Errorf("Backend.Port = %d, want 80", s.Backend.Port)
	}
	if s.TemplateRef.Namespace != tmplNs {
		t.Errorf("TemplateRef.Namespace = %q, want %q", s.TemplateRef.Namespace, tmplNs)
	}
	if s.TemplateRef.Name != tmplName {
		t.Errorf("TemplateRef.Name = %q, want %q", s.TemplateRef.Name, tmplName)
	}
}

func TestMapResumeDenial_NilError(t *testing.T) {
	t.Parallel()
	if got := mapResumeDenial("x", nil); got != nil {
		t.Errorf("mapResumeDenial(_, nil) = %v, want nil", got)
	}
}

func TestMapResumeDenial_PreservesCause(t *testing.T) {
	t.Parallel()
	orig := status.Error(codes.NotFound, "missing")
	d := mapResumeDenial("actor-1", orig)
	if !errors.Is(d, orig) {
		t.Error("errors.Is(denial, original) = false; cause must be preserved for log inspection")
	}
}
