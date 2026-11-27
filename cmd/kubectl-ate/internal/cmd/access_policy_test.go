// Copyright 2026 The Agent Substrate Authors
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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

func TestScopeFromFlags(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		atespace string
		global   bool
		want     accessPolicyScope
		wantErr  string
	}{
		{name: "atespace", atespace: "team-a", want: accessPolicyScope{atespace: "team-a"}},
		{name: "global", global: true, want: accessPolicyScope{}},
		{name: "neither", wantErr: "one of --atespace or --global is required"},
		{name: "both", atespace: "team-a", global: true, wantErr: "mutually exclusive"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := scopeFromFlags(tc.atespace, tc.global)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("scopeFromFlags() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("scopeFromFlags() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("scopeFromFlags() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLoadAccessPolicyManifest(t *testing.T) {
	t.Parallel()
	ownerAlice := []*ateapi.Binding{{Role: "owner", Members: []string{"user:alice"}}}
	tests := []struct {
		name     string
		manifest string
		want     *ateapi.AccessPolicy
		wantErr  string
	}{
		{
			name:     "metadata omitted is filled in",
			manifest: "bindings:\n- role: owner\n  members: [\"user:alice\"]\n",
			want:     &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: "default"}, Bindings: ownerAlice},
		},
		{
			name: "preconditions preserved",
			manifest: `metadata: {name: default, uid: u-1, version: "3"}
bindings: [{role: owner, members: ["user:alice"]}]
`,
			want: &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 3}, Bindings: ownerAlice},
		},
		{
			name:     "no bindings",
			manifest: "metadata: {name: default}\n",
			want:     &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: "default"}},
		},
		{name: "wrong name", manifest: "metadata: {name: other}\n", wantErr: `must be "default"`},
		{name: "atespace set", manifest: "metadata: {atespace: team-a}\n", wantErr: "metadata.atespace must be empty"},
		{name: "unknown field", manifest: "bindingz: []\n", wantErr: "invalid AccessPolicy"},
		{name: "empty", manifest: "", wantErr: "manifest is empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := loadAccessPolicyManifest(strings.NewReader(tc.manifest), "-")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("loadAccessPolicyManifest() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadAccessPolicyManifest() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("loadAccessPolicyManifest() (-want +got):\n%s", diff)
			}
		})
	}
}

// fakeAccessPolicyClient records the last RPC and answers with policy or err.
type fakeAccessPolicyClient struct {
	policy      *ateapi.AccessPolicy
	err         error
	atespaceErr error

	method string
	req    any
}

func (f *fakeAccessPolicyClient) answer(method string, req any) (*ateapi.AccessPolicy, error) {
	f.method, f.req = method, req
	return f.policy, f.err
}

func (f *fakeAccessPolicyClient) GetGlobalAccessPolicy(_ context.Context, req *ateapi.GetGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("GetGlobalAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) CreateGlobalAccessPolicy(_ context.Context, req *ateapi.CreateGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("CreateGlobalAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) UpdateGlobalAccessPolicy(_ context.Context, req *ateapi.UpdateGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("UpdateGlobalAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) GetAtespaceAccessPolicy(_ context.Context, req *ateapi.GetAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("GetAtespaceAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) CreateAtespaceAccessPolicy(_ context.Context, req *ateapi.CreateAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("CreateAtespaceAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) UpdateAtespaceAccessPolicy(_ context.Context, req *ateapi.UpdateAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("UpdateAtespaceAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) DeleteAtespaceAccessPolicy(_ context.Context, req *ateapi.DeleteAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	return f.answer("DeleteAtespaceAccessPolicy", req)
}

func (f *fakeAccessPolicyClient) GetAtespace(_ context.Context, req *ateapi.GetAtespaceRequest, _ ...grpc.CallOption) (*ateapi.Atespace, error) {
	if f.atespaceErr != nil {
		return nil, f.atespaceErr
	}
	return &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: req.GetAtespace().GetName()}}, nil
}

func TestAccessPolicyRunner(t *testing.T) {
	t.Parallel()
	stored := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 2},
		Bindings: []*ateapi.Binding{{Role: "owner", Members: []string{"user:alice"}}},
	}
	edit := &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 2}}
	teamA := accessPolicyScope{atespace: "team-a"}
	global := accessPolicyScope{}
	notFound := status.Error(codes.NotFound, "not found")

	tests := []struct {
		name        string
		scope       accessPolicyScope
		client      *fakeAccessPolicyClient
		run         func(context.Context, *accessPolicyRunner) error
		wantMethod  string
		wantReq     any
		wantStdout  string
		wantStderr  string
		wantErr     string
		wantNoCalls bool
	}{
		{
			name:       "get atespace",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{policy: stored},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.get(ctx) },
			wantMethod: "GetAtespaceAccessPolicy",
			wantReq:    &ateapi.GetAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: resources.SingletonName},
			wantStdout: "team-a   owner   user:alice",
		},
		{
			name:       "get global",
			scope:      global,
			client:     &fakeAccessPolicyClient{policy: stored},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.get(ctx) },
			wantMethod: "GetGlobalAccessPolicy",
			wantReq:    &ateapi.GetGlobalAccessPolicyRequest{Name: resources.SingletonName},
			wantStdout: "<global>   owner   user:alice",
		},
		{
			name:       "get atespace without a policy",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: notFound},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.get(ctx) },
			wantMethod: "GetAtespaceAccessPolicy",
			wantStderr: `atespace "team-a" has no access policy`,
		},
		{
			name:       "get missing atespace",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: notFound, atespaceErr: notFound},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.get(ctx) },
			wantMethod: "GetAtespaceAccessPolicy",
			wantErr:    `atespace "team-a" not found`,
		},
		{
			name:       "get permission denied",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: status.Error(codes.PermissionDenied, "denied")},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.get(ctx) },
			wantMethod: "GetAtespaceAccessPolicy",
			wantErr:    "PermissionDenied",
		},
		{
			name:       "create atespace",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{policy: stored},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.create(ctx, edit) },
			wantMethod: "CreateAtespaceAccessPolicy",
			wantReq:    &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, AccessPolicy: edit},
			wantStdout: "team-a   owner   user:alice",
		},
		{
			name:       "create global",
			scope:      global,
			client:     &fakeAccessPolicyClient{policy: stored},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.create(ctx, edit) },
			wantMethod: "CreateGlobalAccessPolicy",
			wantReq:    &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: edit},
		},
		{
			name:       "create existing",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: status.Error(codes.AlreadyExists, "exists")},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.create(ctx, edit) },
			wantMethod: "CreateAtespaceAccessPolicy",
			wantErr:    "kubectl ate update access-policy",
		},
		{
			name:       "update global",
			scope:      global,
			client:     &fakeAccessPolicyClient{policy: stored},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.update(ctx, edit) },
			wantMethod: "UpdateGlobalAccessPolicy",
			wantReq:    &ateapi.UpdateGlobalAccessPolicyRequest{AccessPolicy: edit},
		},
		{
			name:       "update stale",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: status.Error(codes.Aborted, "version mismatch")},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.update(ctx, edit) },
			wantMethod: "UpdateAtespaceAccessPolicy",
			wantErr:    "re-run",
		},
		{
			name:       "update without a policy",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: notFound},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.update(ctx, edit) },
			wantMethod: "UpdateAtespaceAccessPolicy",
			wantErr:    "kubectl ate create access-policy",
		},
		{
			name:   "delete",
			scope:  teamA,
			client: &fakeAccessPolicyClient{policy: stored},
			run: func(ctx context.Context, r *accessPolicyRunner) error {
				return r.delete(ctx, &ateapi.DeleteOptions{Version: 2})
			},
			wantMethod: "DeleteAtespaceAccessPolicy",
			wantReq:    &ateapi.DeleteAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: resources.SingletonName, Options: &ateapi.DeleteOptions{Version: 2}},
			wantStdout: `access policy for atespace "team-a" deleted`,
		},
		{
			name:       "delete without a policy",
			scope:      teamA,
			client:     &fakeAccessPolicyClient{err: notFound},
			run:        func(ctx context.Context, r *accessPolicyRunner) error { return r.delete(ctx, nil) },
			wantMethod: "DeleteAtespaceAccessPolicy",
			wantErr:    `atespace "team-a" has no access policy`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			r := &accessPolicyRunner{client: tc.client, scope: tc.scope, outputFmt: "table", stdout: &stdout, stderr: &stderr}
			err := tc.run(t.Context(), r)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("error = %v", err)
			}
			if tc.client.method != tc.wantMethod {
				t.Errorf("called %q, want %q", tc.client.method, tc.wantMethod)
			}
			if tc.wantReq != nil {
				if diff := cmp.Diff(tc.wantReq, tc.client.req, protocmp.Transform()); diff != "" {
					t.Errorf("request (-want +got):\n%s", diff)
				}
			}
			if !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tc.wantStdout)
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

// fakeAccessPolicyApplier records every RPC made by accessPolicyRunner.apply
// and answers each verb independently.
type fakeAccessPolicyApplier struct {
	getPolicy    *ateapi.AccessPolicy
	getErr       error
	createPolicy *ateapi.AccessPolicy
	createErr    error
	updatePolicy *ateapi.AccessPolicy
	updateErr    error
	atespaceErr  error

	calls       []string
	getReq      any
	createReq   any
	updateReq   any
	atespaceReq *ateapi.GetAtespaceRequest
}

func (f *fakeAccessPolicyApplier) GetGlobalAccessPolicy(_ context.Context, req *ateapi.GetGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "GetGlobalAccessPolicy")
	f.getReq = req
	return f.getPolicy, f.getErr
}

func (f *fakeAccessPolicyApplier) CreateGlobalAccessPolicy(_ context.Context, req *ateapi.CreateGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "CreateGlobalAccessPolicy")
	f.createReq = req
	return f.createPolicy, f.createErr
}

func (f *fakeAccessPolicyApplier) UpdateGlobalAccessPolicy(_ context.Context, req *ateapi.UpdateGlobalAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "UpdateGlobalAccessPolicy")
	f.updateReq = req
	return f.updatePolicy, f.updateErr
}

func (f *fakeAccessPolicyApplier) GetAtespaceAccessPolicy(_ context.Context, req *ateapi.GetAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "GetAtespaceAccessPolicy")
	f.getReq = req
	return f.getPolicy, f.getErr
}

func (f *fakeAccessPolicyApplier) CreateAtespaceAccessPolicy(_ context.Context, req *ateapi.CreateAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "CreateAtespaceAccessPolicy")
	f.createReq = req
	return f.createPolicy, f.createErr
}

func (f *fakeAccessPolicyApplier) UpdateAtespaceAccessPolicy(_ context.Context, req *ateapi.UpdateAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "UpdateAtespaceAccessPolicy")
	f.updateReq = req
	return f.updatePolicy, f.updateErr
}

func (f *fakeAccessPolicyApplier) DeleteAtespaceAccessPolicy(_ context.Context, _ *ateapi.DeleteAtespaceAccessPolicyRequest, _ ...grpc.CallOption) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "DeleteAtespaceAccessPolicy")
	return nil, status.Error(codes.Unimplemented, "unexpected delete")
}

func (f *fakeAccessPolicyApplier) GetAtespace(_ context.Context, req *ateapi.GetAtespaceRequest, _ ...grpc.CallOption) (*ateapi.Atespace, error) {
	f.calls = append(f.calls, "GetAtespace")
	f.atespaceReq = req
	if f.atespaceErr != nil {
		return nil, f.atespaceErr
	}
	return &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: req.GetAtespace().GetName()}}, nil
}

func TestAccessPolicyRunner_Apply(t *testing.T) {
	t.Parallel()

	teamA := accessPolicyScope{atespace: "team-a"}
	global := accessPolicyScope{}
	teamAGetReq := &ateapi.GetAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, Name: resources.SingletonName}
	globalGetReq := &ateapi.GetGlobalAccessPolicyRequest{Name: resources.SingletonName}
	teamAAtespaceReq := &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}}
	notFound := status.Error(codes.NotFound, "not found")

	tests := []struct {
		name            string
		scope           accessPolicyScope
		outputFmt       string
		policy          *ateapi.AccessPolicy
		client          *fakeAccessPolicyApplier
		wantCalls       []string
		wantGetReq      any
		wantCreateReq   any
		wantUpdateReq   any
		wantAtespaceReq *ateapi.GetAtespaceRequest
		wantStdout      string
		wantStderr      string
		wantErr         string
		wantErrCode     codes.Code
	}{
		{
			name:            "atespace create when missing (table)",
			scope:           teamA,
			outputFmt:       "table",
			policy:          testAccessPolicy(),
			client:          &fakeAccessPolicyApplier{getErr: notFound, createPolicy: testAccessPolicy(withMeta("u-1", 1))},
			wantCalls:       []string{"GetAtespaceAccessPolicy", "GetAtespace", "CreateAtespaceAccessPolicy"},
			wantGetReq:      teamAGetReq,
			wantAtespaceReq: teamAAtespaceReq,
			wantCreateReq:   &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, AccessPolicy: testAccessPolicy()},
			wantStdout:      "team-a   owner   user:alice",
			wantStderr:      "access policy for atespace \"team-a\" created\n",
		},
		{
			name:          "global create when missing (yaml keeps stderr empty)",
			scope:         global,
			outputFmt:     "yaml",
			policy:        testAccessPolicy(),
			client:        &fakeAccessPolicyApplier{getErr: notFound, createPolicy: testAccessPolicy(withMeta("u-1", 1))},
			wantCalls:     []string{"GetGlobalAccessPolicy", "CreateGlobalAccessPolicy"},
			wantGetReq:    globalGetReq,
			wantCreateReq: &ateapi.CreateGlobalAccessPolicyRequest{AccessPolicy: testAccessPolicy()},
			wantStdout:    "uid: u-1",
		},
		{
			name:       "global unchanged uses natural global status label (table)",
			scope:      global,
			outputFmt:  "table",
			policy:     testAccessPolicy(),
			client:     &fakeAccessPolicyApplier{getPolicy: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls:  []string{"GetGlobalAccessPolicy"},
			wantGetReq: globalGetReq,
			wantStdout: "<global>   owner   user:alice",
			wantStderr: "global access policy unchanged\n",
		},
		{
			name:       "atespace unchanged (json keeps stderr empty)",
			scope:      teamA,
			outputFmt:  "json",
			policy:     testAccessPolicy(),
			client:     &fakeAccessPolicyApplier{getPolicy: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls:  []string{"GetAtespaceAccessPolicy"},
			wantGetReq: teamAGetReq,
			wantStdout: `"uid": "u-1"`,
		},
		{
			name:          "atespace update copies stored uid and version (table)",
			scope:         teamA,
			outputFmt:     "table",
			policy:        testAccessPolicy(withMember("user:bob")),
			client:        &fakeAccessPolicyApplier{getPolicy: testAccessPolicy(withMeta("u-1", 2)), updatePolicy: testAccessPolicy(withMeta("u-1", 3), withMember("user:bob"))},
			wantCalls:     []string{"GetAtespaceAccessPolicy", "UpdateAtespaceAccessPolicy"},
			wantGetReq:    teamAGetReq,
			wantUpdateReq: &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: &ateapi.ObjectRef{Name: "team-a"}, AccessPolicy: testAccessPolicy(withMeta("u-1", 2), withMember("user:bob"))},
			wantStdout:    "team-a   owner   user:bob",
			wantStderr:    "access policy for atespace \"team-a\" updated\n",
		},
		{
			name:      "global update clears bindings (table)",
			scope:     global,
			outputFmt: "table",
			policy:    testAccessPolicy(func(p *ateapi.AccessPolicy) { p.Bindings = []*ateapi.Binding{} }),
			client: &fakeAccessPolicyApplier{
				getPolicy:    testAccessPolicy(withMeta("u-1", 2)),
				updatePolicy: testAccessPolicy(withMeta("u-1", 3), func(p *ateapi.AccessPolicy) { p.Bindings = nil }),
			},
			wantCalls:     []string{"GetGlobalAccessPolicy", "UpdateGlobalAccessPolicy"},
			wantGetReq:    globalGetReq,
			wantUpdateReq: &ateapi.UpdateGlobalAccessPolicyRequest{AccessPolicy: &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 2}}},
			wantStdout:    "<global>   <none>   <none>",
			wantStderr:    "global access policy updated\n",
		},
		{
			name:            "missing atespace fails before create",
			scope:           teamA,
			outputFmt:       "table",
			policy:          testAccessPolicy(),
			client:          &fakeAccessPolicyApplier{getErr: notFound, atespaceErr: notFound},
			wantCalls:       []string{"GetAtespaceAccessPolicy", "GetAtespace"},
			wantGetReq:      teamAGetReq,
			wantAtespaceReq: teamAAtespaceReq,
			wantErr:         `atespace "team-a" not found`,
		},
		{
			name:        "get error wraps gRPC status",
			scope:       teamA,
			outputFmt:   "table",
			policy:      testAccessPolicy(),
			client:      &fakeAccessPolicyApplier{getErr: status.Error(codes.PermissionDenied, "denied")},
			wantCalls:   []string{"GetAtespaceAccessPolicy"},
			wantGetReq:  teamAGetReq,
			wantErr:     "failed to get",
			wantErrCode: codes.PermissionDenied,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			origPolicy := proto.CloneOf(tc.policy)
			var stdout, stderr bytes.Buffer
			r := &accessPolicyRunner{client: tc.client, scope: tc.scope, outputFmt: tc.outputFmt, stdout: &stdout, stderr: &stderr}
			err := r.apply(t.Context(), tc.policy)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("apply() error = %v, want it to contain %q", err, tc.wantErr)
				}
				if tc.wantErrCode != codes.OK && status.Code(err) != tc.wantErrCode {
					t.Errorf("status.Code(err) = %v, want %v", status.Code(err), tc.wantErrCode)
				}
			} else if err != nil {
				t.Fatalf("apply() unexpected error = %v", err)
			}
			if diff := cmp.Diff(origPolicy, tc.policy, protocmp.Transform()); diff != "" {
				t.Errorf("input policy was mutated (-before +after):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantCalls, tc.client.calls); diff != "" {
				t.Errorf("calls (-want +got):\n%s", diff)
			}
			if tc.wantGetReq != nil {
				if diff := cmp.Diff(tc.wantGetReq, tc.client.getReq, protocmp.Transform()); diff != "" {
					t.Errorf("get request (-want +got):\n%s", diff)
				}
			}
			if tc.wantCreateReq != nil {
				if diff := cmp.Diff(tc.wantCreateReq, tc.client.createReq, protocmp.Transform()); diff != "" {
					t.Errorf("create request (-want +got):\n%s", diff)
				}
			}
			if tc.wantUpdateReq != nil {
				if diff := cmp.Diff(tc.wantUpdateReq, tc.client.updateReq, protocmp.Transform()); diff != "" {
					t.Errorf("update request (-want +got):\n%s", diff)
				}
			}
			if diff := cmp.Diff(tc.wantAtespaceReq, tc.client.atespaceReq, protocmp.Transform()); diff != "" {
				t.Errorf("atespace request (-want +got):\n%s", diff)
			}
			if tc.wantStdout != "" && !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Errorf("stdout = %q, want it to contain %q", stdout.String(), tc.wantStdout)
			}
			if tc.wantStdout == "" && tc.wantErr != "" && stdout.String() != "" {
				t.Errorf("stdout = %q, want empty on error", stdout.String())
			}
			if diff := cmp.Diff(tc.wantStderr, stderr.String()); diff != "" {
				t.Errorf("stderr (-want +got):\n%s", diff)
			}
		})
	}
}
