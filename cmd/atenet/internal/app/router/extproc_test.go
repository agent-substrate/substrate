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
	"google.golang.org/grpc"
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
		expectedErr    error
		expectedErrStr string
		expectedTarget string
	}{
		{
			name:        "invalid host",
			authority:   "invalid-host.com",
			expectErr:   true,
			expectedErr: notFoundErr,
		},
		{
			name:           "Error resuming actor",
			authority:      testUUID + ".actors.resources.substrate.ate.dev",
			resumeErr:      errors.New("resume failed"),
			expectErr:      true,
			expectedErrStr: "error resuming actor 123e4567-e89b-12d3-a456-426614174000: resume failed",
		},
		{
			name:      "Bad Actor IP from resume",
			authority: testUUID + ".actors.resources.substrate.ate.dev",
			resumeResp: &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{
					AteomPodIp: "invalid-ip",
				},
			},
			expectErr:      true,
			expectedErrStr: "actor \"123e4567-e89b-12d3-a456-426614174000\" did not have a valid IP \"invalid-ip\"",
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

			s := NewExtProcServer(50051, clientMock)

			reqHeaders := &extprocv3.HttpHeaders{
				Headers: &corev3.HeaderMap{
					Headers: []*corev3.HeaderValue{
						{Key: ":path", Value: "/v1/actors/invoke"},
						{Key: ":authority", Value: tc.authority},
						{Key: ":method", Value: "POST"},
					},
				},
			}

			res, metadata, target, err := s.handleRequestHeaders(context.Background(), reqHeaders)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if tc.expectedErr != nil && !errors.Is(err, tc.expectedErr) {
					t.Errorf("expected error %v, got %v", tc.expectedErr, err)
				}
				if tc.expectedErrStr != "" && err.Error() != tc.expectedErrStr {
					t.Errorf("expected error string %q, got %q", tc.expectedErrStr, err.Error())
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
