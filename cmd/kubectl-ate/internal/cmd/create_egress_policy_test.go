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

// fakeEgressPolicyCreator records the request it received and answers with a
// configured policy or error.
type fakeEgressPolicyCreator struct {
	req    *ateapipb.CreateActorEgressPolicyRequest
	policy *ateapipb.EgressPolicy
	err    error
}

func (f *fakeEgressPolicyCreator) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error) {
	f.req = req
	if f.err != nil {
		return nil, f.err
	}
	return f.policy, nil
}

func TestCreateEgressPolicyRunner_Run(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinTime(t, now)

	actor := &ateapipb.ObjectRef{Atespace: "team-a", Name: "c1"}
	rules := []*ateapipb.EgressRule{{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}}}
	// A manifest cloned from another actor still carries that actor's
	// server-managed fields; the CLI sends them as is and the server scrubs them.
	manifest := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Uid: "old-uid", Version: 7},
		Rules:    rules,
	}
	created := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "team-a",
			Name:       "default",
			Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
			Version:    1,
			CreateTime: timestamppb.New(now),
		},
		Rules: rules,
	}
	wantReq := &ateapipb.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: manifest}

	tests := []struct {
		name      string
		outputFmt string
		creator   *fakeEgressPolicyCreator
		wantOut   string
		wantErr   string
	}{
		{
			name:      "table by default",
			outputFmt: "table",
			creator:   &fakeEgressPolicyCreator{policy: created},
			wantOut: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c1      1       1         0s
`,
		},
		{
			name:      "yaml prints the created policy",
			outputFmt: "yaml",
			creator:   &fakeEgressPolicyCreator{policy: created},
			wantOut: `metadata:
  atespace: team-a
  createTime: "2026-01-01T12:00:00Z"
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
			name:      "already exists wraps",
			outputFmt: "table",
			creator:   &fakeEgressPolicyCreator{err: status.Error(codes.AlreadyExists, "EgressPolicy already exists")},
			wantErr:   `failed to create egress policy for actor "c1": rpc error: code = AlreadyExists desc = EgressPolicy already exists`,
		},
		{
			name:      "missing actor wraps",
			outputFmt: "table",
			creator:   &fakeEgressPolicyCreator{err: status.Error(codes.FailedPrecondition, "parent Actor does not exist")},
			wantErr:   `failed to create egress policy for actor "c1": rpc error: code = FailedPrecondition desc = parent Actor does not exist`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			runner := &CreateEgressPolicyRunner{
				creator:   test.creator,
				actor:     actor,
				policy:    manifest,
				outputFmt: test.outputFmt,
				out:       &out,
			}
			err := runner.Run(context.Background())
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != test.wantErr {
				t.Fatalf("Run() error = %q, want %q", gotErr, test.wantErr)
			}
			if diff := cmp.Diff(wantReq, test.creator.req, protocmp.Transform()); diff != "" {
				t.Errorf("request mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(test.wantOut, out.String()); diff != "" {
				t.Errorf("stdout mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
