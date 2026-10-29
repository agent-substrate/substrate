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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
