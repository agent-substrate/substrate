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

package cmd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeEgressPolicyGetter records the request it received and answers with a
// configured policy or error.
type fakeEgressPolicyGetter struct {
	req    *ateapipb.GetActorEgressPolicyRequest
	policy *ateapipb.EgressPolicy
	err    error
}

func (f *fakeEgressPolicyGetter) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.policy, nil
}

func TestGetEgressPolicyRunner_Run(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinTime(t, now)

	actor := &ateapipb.ObjectRef{Atespace: "team-a", Name: "c1"}
	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "team-a",
			Name:       "default",
			Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
			Version:    1,
			CreateTime: timestamppb.New(now.Add(-5 * time.Minute)), // table row prints AGE 5m
		},
		Rules: []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}},
	}

	tests := []struct {
		name       string
		outputFmt  string
		getter     *fakeEgressPolicyGetter
		wantReq    *ateapipb.GetActorEgressPolicyRequest
		wantOut    string
		wantErrOut string
		wantErr    string
	}{
		{
			name:      "table by default",
			outputFmt: "table",
			getter:    &fakeEgressPolicyGetter{policy: policy},
			wantReq:   &ateapipb.GetActorEgressPolicyRequest{Actor: actor},
			wantOut: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c1      1       1         5m
`,
		},
		{
			name:      "yaml",
			outputFmt: "yaml",
			getter:    &fakeEgressPolicyGetter{policy: policy},
			wantReq:   &ateapipb.GetActorEgressPolicyRequest{Actor: actor},
			wantOut: `metadata:
  atespace: team-a
  createTime: "2026-01-01T11:55:00Z"
  name: default
  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
  version: "1"
rules:
- hostnames:
    patterns:
    - api.example.com
`,
		},
		{
			name:       "not found writes note to stderr and succeeds",
			outputFmt:  "yaml",
			getter:     &fakeEgressPolicyGetter{err: status.Error(codes.NotFound, "EgressPolicy not found")},
			wantReq:    &ateapipb.GetActorEgressPolicyRequest{Actor: actor},
			wantErrOut: "actor \"c1\" in atespace \"team-a\" has no egress policy; all egress is denied\n",
		},
		{
			name:      "other error wraps",
			outputFmt: "table",
			getter:    &fakeEgressPolicyGetter{err: status.Error(codes.Unavailable, "api-server down")},
			wantReq:   &ateapipb.GetActorEgressPolicyRequest{Actor: actor},
			wantErr:   `failed to get egress policy for actor "c1": rpc error: code = Unavailable desc = api-server down`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			runner := &GetEgressPolicyRunner{
				getter:    test.getter,
				actor:     actor,
				outputFmt: test.outputFmt,
				out:       &out,
				errOut:    &errOut,
			}
			err := runner.Run(context.Background())
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != test.wantErr {
				t.Fatalf("Run() error = %q, want %q", gotErr, test.wantErr)
			}
			if diff := cmp.Diff(test.wantReq, test.getter.req, protocmp.Transform()); diff != "" {
				t.Errorf("request mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(test.wantOut, out.String()); diff != "" {
				t.Errorf("stdout mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(test.wantErrOut, errOut.String()); diff != "" {
				t.Errorf("stderr mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
