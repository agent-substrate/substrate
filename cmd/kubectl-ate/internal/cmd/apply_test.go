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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestValidateApplyPreconditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		meta       *ateapi.ResourceMetadata
		wantPinned bool
		wantErr    string
	}{
		{name: "nil metadata"},
		{name: "both omitted", meta: &ateapi.ResourceMetadata{Name: "default"}},
		{name: "both set", meta: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 1}, wantPinned: true},
		{name: "uid only", meta: &ateapi.ResourceMetadata{Uid: "u-1"}, wantErr: "must either both be omitted or both be set"},
		{name: "version only", meta: &ateapi.ResourceMetadata{Version: 2}, wantErr: "must either both be omitted or both be set"},
		{name: "negative version", meta: &ateapi.ResourceMetadata{Version: -1}, wantErr: "must be at least 1, got -1"},
		{name: "uid with negative version", meta: &ateapi.ResourceMetadata{Uid: "u-1", Version: -3}, wantErr: "must be at least 1, got -3"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotPinned, err := validateApplyPreconditions(tc.meta)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("validateApplyPreconditions() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateApplyPreconditions() unexpected error = %v", err)
			}
			if gotPinned != tc.wantPinned {
				t.Errorf("validateApplyPreconditions() pinned = %t, want %t", gotPinned, tc.wantPinned)
			}
		})
	}
}

func TestRequireStoredPreconditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		meta        *ateapi.ResourceMetadata
		wantUID     string
		wantVersion int64
		wantErr     bool
	}{
		{name: "valid", meta: &ateapi.ResourceMetadata{Uid: "u-1", Version: 2}, wantUID: "u-1", wantVersion: 2},
		{name: "nil metadata", wantErr: true},
		{name: "missing uid", meta: &ateapi.ResourceMetadata{Version: 2}, wantErr: true},
		{name: "zero version", meta: &ateapi.ResourceMetadata{Uid: "u-1"}, wantErr: true},
		{name: "negative version", meta: &ateapi.ResourceMetadata{Uid: "u-1", Version: -1}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			uid, version, err := requireStoredPreconditions(tc.meta)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("requireStoredPreconditions() = (%q, %d, nil), want error", uid, version)
				}
				return
			}
			if err != nil {
				t.Fatalf("requireStoredPreconditions() unexpected error = %v", err)
			}
			if uid != tc.wantUID || version != tc.wantVersion {
				t.Errorf("requireStoredPreconditions() = (%q, %d), want (%q, %d)", uid, version, tc.wantUID, tc.wantVersion)
			}
		})
	}
}

// unknownFieldBytes encodes a synthetic protobuf varint field (field number 999)
// to simulate a field added by a newer server.
func unknownFieldBytes() []byte {
	var b []byte
	b = protowire.AppendTag(b, 999, protowire.VarintType)
	b = protowire.AppendVarint(b, 1)
	return b
}

func TestHasUnknownFields(t *testing.T) {
	t.Parallel()

	topUnknown := &ateapi.AccessPolicy{}
	topUnknown.ProtoReflect().SetUnknown(unknownFieldBytes())

	nestedBinding := &ateapi.Binding{Role: "owner", Members: []string{"user:alice"}}
	nestedBinding.ProtoReflect().SetUnknown(unknownFieldBytes())
	nestedAccess := &ateapi.AccessPolicy{Bindings: []*ateapi.Binding{nestedBinding}}

	nestedHTTP := &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}
	nestedHTTP.ProtoReflect().SetUnknown(unknownFieldBytes())
	nestedEgress := &ateapi.EgressPolicy{Rules: []*ateapi.EgressRule{{Http: nestedHTTP}}}

	var nilPolicy *ateapi.AccessPolicy

	tests := []struct {
		name string
		msg  proto.Message
		want bool
	}{
		{name: "nil typed message", msg: nilPolicy},
		{name: "clean message", msg: &ateapi.AccessPolicy{Bindings: []*ateapi.Binding{{Role: "owner", Members: []string{"user:alice"}}}}},
		{name: "top-level unknown field", msg: topUnknown, want: true},
		{name: "nested list element unknown field", msg: nestedAccess, want: true},
		{name: "deeply nested message unknown field", msg: nestedEgress, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := hasUnknownFields(tc.msg.ProtoReflect()); got != tc.want {
				t.Errorf("hasUnknownFields() = %t, want %t", got, tc.want)
			}
		})
	}
}

// fakeSingletonApplier records calls to the shared applySingleton state machine.
type fakeSingletonApplier struct {
	getResp    *ateapi.AccessPolicy
	getErr     error
	createResp *ateapi.AccessPolicy
	createErr  error
	updateResp *ateapi.AccessPolicy
	updateErr  error
	parentErr  error

	calls         []string
	createdWith   *ateapi.AccessPolicy
	updatedWith   *ateapi.AccessPolicy
	printedPolicy *ateapi.AccessPolicy
	printedAction applyAction
}

func (f *fakeSingletonApplier) label() string      { return `access policy for atespace "team-a"` }
func (f *fakeSingletonApplier) subcommand() string { return "access-policy" }

func (f *fakeSingletonApplier) getPolicy(_ context.Context) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "get")
	return f.getResp, f.getErr
}

func (f *fakeSingletonApplier) createPolicy(_ context.Context, p *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "create")
	f.createdWith = proto.CloneOf(p)
	return f.createResp, f.createErr
}

func (f *fakeSingletonApplier) updatePolicy(_ context.Context, p *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error) {
	f.calls = append(f.calls, "update")
	f.updatedWith = proto.CloneOf(p)
	return f.updateResp, f.updateErr
}

func (f *fakeSingletonApplier) requireParent(_ context.Context) error {
	f.calls = append(f.calls, "requireParent")
	return f.parentErr
}

func (f *fakeSingletonApplier) printApplied(p *ateapi.AccessPolicy, action applyAction) error {
	f.calls = append(f.calls, "print")
	f.printedPolicy = proto.CloneOf(p)
	f.printedAction = action
	return nil
}

func (f *fakeSingletonApplier) metadata(p *ateapi.AccessPolicy) *ateapi.ResourceMetadata {
	return p.Metadata
}

func (f *fakeSingletonApplier) setMetadata(p *ateapi.AccessPolicy, meta *ateapi.ResourceMetadata) {
	p.Metadata = meta
}

func testAccessPolicy(mods ...func(*ateapi.AccessPolicy)) *ateapi.AccessPolicy {
	p := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default"},
		Bindings: []*ateapi.Binding{{Role: "owner", Members: []string{"user:alice"}}},
	}
	for _, mod := range mods {
		mod(p)
	}
	return p
}

func withMeta(uid string, version int64) func(*ateapi.AccessPolicy) {
	return func(p *ateapi.AccessPolicy) {
		p.Metadata.Uid = uid
		p.Metadata.Version = version
	}
}

func withMember(member string) func(*ateapi.AccessPolicy) {
	return func(p *ateapi.AccessPolicy) {
		p.Bindings = []*ateapi.Binding{{Role: "owner", Members: []string{member}}}
	}
}

func TestApplySingleton(t *testing.T) {
	t.Parallel()

	notFound := status.Error(codes.NotFound, "not found")
	parentMissing := errors.New(`atespace "team-a" not found`)

	tests := []struct {
		name        string
		desired     *ateapi.AccessPolicy
		applier     *fakeSingletonApplier
		wantCalls   []string
		wantAction  applyAction
		wantUpdate  *ateapi.AccessPolicy
		wantErr     string
		wantErrCode codes.Code
	}{
		{
			name:    "nil desired policy fails before any RPC",
			desired: nil,
			applier: &fakeSingletonApplier{},
			wantErr: "policy is nil",
		},
		{
			name:    "partial preconditions fail before any RPC",
			desired: testAccessPolicy(withMeta("u-1", 0)),
			applier: &fakeSingletonApplier{},
			wantErr: "must either both be omitted or both be set",
		},
		{
			name:       "create when missing",
			desired:    testAccessPolicy(),
			applier:    &fakeSingletonApplier{getErr: notFound, createResp: testAccessPolicy(withMeta("u-1", 1))},
			wantCalls:  []string{"get", "requireParent", "create", "print"},
			wantAction: applyCreated,
		},
		{
			name:      "missing parent fails before create",
			desired:   testAccessPolicy(),
			applier:   &fakeSingletonApplier{getErr: notFound, parentErr: parentMissing},
			wantCalls: []string{"get", "requireParent"},
			wantErr:   `atespace "team-a" not found`,
		},
		{
			name:      "missing policy with pinned preconditions fails before create",
			desired:   testAccessPolicy(withMeta("u-1", 2)),
			applier:   &fakeSingletonApplier{getErr: notFound},
			wantCalls: []string{"get", "requireParent"},
			wantErr:   "remove metadata.uid and metadata.version",
		},
		{
			name:        "concurrent create fails loudly",
			desired:     testAccessPolicy(),
			applier:     &fakeSingletonApplier{getErr: notFound, createErr: status.Error(codes.AlreadyExists, "exists")},
			wantCalls:   []string{"get", "requireParent", "create"},
			wantErr:     `created concurrently; re-run "kubectl ate apply access-policy"`,
			wantErrCode: codes.AlreadyExists,
		},
		{
			name:        "create error wraps",
			desired:     testAccessPolicy(),
			applier:     &fakeSingletonApplier{getErr: notFound, createErr: status.Error(codes.InvalidArgument, "bad binding")},
			wantCalls:   []string{"get", "requireParent", "create"},
			wantErr:     "failed to create",
			wantErrCode: codes.InvalidArgument,
		},
		{
			name:        "get error wraps",
			desired:     testAccessPolicy(),
			applier:     &fakeSingletonApplier{getErr: status.Error(codes.PermissionDenied, "denied")},
			wantCalls:   []string{"get"},
			wantErr:     "failed to get",
			wantErrCode: codes.PermissionDenied,
		},
		{
			name:      "nil server response fails loudly",
			desired:   testAccessPolicy(),
			applier:   &fakeSingletonApplier{getResp: nil},
			wantCalls: []string{"get"},
			wantErr:   "server returned empty response",
		},
		{
			name:      "stored policy missing uid fails loudly when unpinned",
			desired:   testAccessPolicy(withMember("user:bob")),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("", 2))},
			wantCalls: []string{"get"},
			wantErr:   "server response lacks metadata.uid or metadata.version",
		},
		{
			name:      "stored policy missing uid fails loudly when pinned",
			desired:   testAccessPolicy(withMeta("u-1", 2)),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("", 2))},
			wantCalls: []string{"get"},
			wantErr:   "server response lacks metadata.uid or metadata.version",
		},
		{
			name:      "stored policy missing version fails loudly",
			desired:   testAccessPolicy(withMember("user:bob")),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 0))},
			wantCalls: []string{"get"},
			wantErr:   "server response lacks metadata.uid or metadata.version",
		},
		{
			name:       "unchanged skips update",
			desired:    testAccessPolicy(),
			applier:    &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls:  []string{"get", "print"},
			wantAction: applyUnchanged,
		},
		{
			name:    "unchanged empty bindings (empty slice vs nil) skips update",
			desired: testAccessPolicy(func(p *ateapi.AccessPolicy) { p.Bindings = []*ateapi.Binding{} }),
			applier: &fakeSingletonApplier{
				getResp: testAccessPolicy(withMeta("u-1", 2), func(p *ateapi.AccessPolicy) { p.Bindings = nil }),
			},
			wantCalls:  []string{"get", "print"},
			wantAction: applyUnchanged,
		},
		{
			name:    "unchanged ignores unknown fields on server metadata",
			desired: testAccessPolicy(),
			applier: &fakeSingletonApplier{
				getResp: testAccessPolicy(withMeta("u-1", 2), func(p *ateapi.AccessPolicy) {
					p.Metadata.ProtoReflect().SetUnknown(unknownFieldBytes())
				}),
			},
			wantCalls:  []string{"get", "print"},
			wantAction: applyUnchanged,
		},
		{
			name:       "unchanged with matching pinned preconditions skips update",
			desired:    testAccessPolicy(withMeta("u-1", 2)),
			applier:    &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls:  []string{"get", "print"},
			wantAction: applyUnchanged,
		},
		{
			name:      "stale pinned version fails even when body matches",
			desired:   testAccessPolicy(withMeta("u-1", 1)),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls: []string{"get"},
			wantErr:   `preconditions do not match the stored access policy for atespace "team-a"; run "kubectl ate get access-policy -o yaml" and reapply`,
		},
		{
			name:      "stale pinned uid fails even when version and body match",
			desired:   testAccessPolicy(withMeta("u-other", 2)),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2))},
			wantCalls: []string{"get"},
			wantErr:   "preconditions do not match",
		},
		{
			name:    "unknown fields on stored policy body fail loudly",
			desired: testAccessPolicy(),
			applier: &fakeSingletonApplier{
				getResp: testAccessPolicy(withMeta("u-1", 2), func(p *ateapi.AccessPolicy) {
					p.ProtoReflect().SetUnknown(unknownFieldBytes())
				}),
			},
			wantCalls: []string{"get"},
			wantErr:   "unknown to this kubectl-ate version",
		},
		{
			name:       "update copies stored uid and version when omitted",
			desired:    testAccessPolicy(withMember("user:bob")),
			applier:    &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateResp: testAccessPolicy(withMeta("u-1", 3), withMember("user:bob"))},
			wantCalls:  []string{"get", "update", "print"},
			wantAction: applyUpdated,
			wantUpdate: testAccessPolicy(withMeta("u-1", 2), withMember("user:bob")),
		},
		{
			name:       "update initializes metadata when desired metadata is nil",
			desired:    testAccessPolicy(withMember("user:bob"), func(p *ateapi.AccessPolicy) { p.Metadata = nil }),
			applier:    &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateResp: testAccessPolicy(withMeta("u-1", 3), withMember("user:bob"))},
			wantCalls:  []string{"get", "update", "print"},
			wantAction: applyUpdated,
			wantUpdate: &ateapi.AccessPolicy{
				Metadata: &ateapi.ResourceMetadata{Uid: "u-1", Version: 2},
				Bindings: []*ateapi.Binding{{Role: "owner", Members: []string{"user:bob"}}},
			},
		},
		{
			name:       "update with matching pinned preconditions preserves manifest metadata",
			desired:    testAccessPolicy(withMeta("u-1", 2), withMember("user:bob")),
			applier:    &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateResp: testAccessPolicy(withMeta("u-1", 3), withMember("user:bob"))},
			wantCalls:  []string{"get", "update", "print"},
			wantAction: applyUpdated,
			wantUpdate: testAccessPolicy(withMeta("u-1", 2), withMember("user:bob")),
		},
		{
			name:        "concurrent update race when unpinned fails without retry",
			desired:     testAccessPolicy(withMember("user:bob")),
			applier:     &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: status.Error(codes.Aborted, "conflict")},
			wantCalls:   []string{"get", "update"},
			wantErr:     `changed concurrently; re-run "kubectl ate apply access-policy"`,
			wantErrCode: codes.Aborted,
		},
		{
			name:        "concurrent update race when pinned points back at get",
			desired:     testAccessPolicy(withMeta("u-1", 2), withMember("user:bob")),
			applier:     &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: status.Error(codes.Aborted, "conflict")},
			wantCalls:   []string{"get", "update"},
			wantErr:     `run "kubectl ate get access-policy -o yaml" and reapply`,
			wantErrCode: codes.Aborted,
		},
		{
			name:        "concurrent delete during unpinned update suggests re-apply",
			desired:     testAccessPolicy(withMember("user:bob")),
			applier:     &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: notFound},
			wantCalls:   []string{"get", "update", "requireParent"},
			wantErr:     `deleted concurrently; re-run "kubectl ate apply access-policy"`,
			wantErrCode: codes.NotFound,
		},
		{
			name:        "concurrent delete during pinned update tells user to remove preconditions",
			desired:     testAccessPolicy(withMeta("u-1", 2), withMember("user:bob")),
			applier:     &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: notFound},
			wantCalls:   []string{"get", "update", "requireParent"},
			wantErr:     "remove metadata.uid and metadata.version from the manifest before reapplying to create it",
			wantErrCode: codes.NotFound,
		},
		{
			name:      "parent deleted concurrently before update",
			desired:   testAccessPolicy(withMember("user:bob")),
			applier:   &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: notFound, parentErr: parentMissing},
			wantCalls: []string{"get", "update", "requireParent"},
			wantErr:   `atespace "team-a" not found`,
		},
		{
			name:        "update error wraps",
			desired:     testAccessPolicy(withMember("user:bob")),
			applier:     &fakeSingletonApplier{getResp: testAccessPolicy(withMeta("u-1", 2)), updateErr: status.Error(codes.Unavailable, "down")},
			wantCalls:   []string{"get", "update"},
			wantErr:     "failed to update",
			wantErrCode: codes.Unavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			orig := proto.CloneOf(tc.desired)
			err := applySingleton(t.Context(), tc.applier, tc.desired)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("applySingleton() error = %v, want substring %q", err, tc.wantErr)
				}
				if tc.wantErrCode != codes.OK && status.Code(err) != tc.wantErrCode {
					t.Errorf("status.Code(err) = %v, want %v", status.Code(err), tc.wantErrCode)
				}
			} else if err != nil {
				t.Fatalf("applySingleton() unexpected error = %v", err)
			}
			if diff := cmp.Diff(orig, tc.desired, protocmp.Transform()); diff != "" {
				t.Errorf("desired policy was mutated (-before +after):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantCalls, tc.applier.calls); diff != "" {
				t.Errorf("calls (-want +got):\n%s", diff)
			}
			if tc.wantAction != "" && tc.applier.printedAction != tc.wantAction {
				t.Errorf("printed action = %q, want %q", tc.applier.printedAction, tc.wantAction)
			}
			if tc.wantUpdate != nil {
				if diff := cmp.Diff(tc.wantUpdate, tc.applier.updatedWith, protocmp.Transform()); diff != "" {
					t.Errorf("update payload (-want +got):\n%s", diff)
				}
			}
		})
	}
}

func TestApplyRejectsPartialPreconditionsBeforeDialing(t *testing.T) {
	dir := t.TempDir()
	accessManifest := filepath.Join(dir, "access.yaml")
	if err := os.WriteFile(accessManifest, []byte("metadata: {name: default, uid: u-1}\nbindings: []\n"), 0o600); err != nil {
		t.Fatalf("write access manifest: %v", err)
	}
	egressManifest := filepath.Join(dir, "egress.yaml")
	if err := os.WriteFile(egressManifest, []byte("metadata: {atespace: team-a, name: default, version: \"2\"}\nrules: []\n"), 0o600); err != nil {
		t.Fatalf("write egress manifest: %v", err)
	}

	tests := []struct {
		name  string
		args  []string
		stdin string
	}{
		{name: "access-policy uid without version", args: []string{"apply", "access-policy", "-a", "team-a", "-f", accessManifest}},
		{name: "access-policy stdin", args: []string{"apply", "access-policy", "--global", "-f", "-"}, stdin: "metadata: {uid: u-1}\nbindings: []\n"},
		{name: "egress-policy version without uid", args: []string{"apply", "egress-policy", "c1", "-a", "team-a", "-f", egressManifest}},
		{name: "egress-policy stdin", args: []string{"apply", "egress-policy", "c1", "-a", "team-a", "-f", "-"}, stdin: "metadata: {version: \"2\"}\nrules: []\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.stdin != "" {
				rootCmd.SetIn(strings.NewReader(tc.stdin))
				t.Cleanup(func() { rootCmd.SetIn(nil) })
			}
			stdout, stderr, err := runRootCLI(t, tc.args...)
			if err == nil {
				t.Fatalf("expected error for %v, got nil", tc.args)
			}
			if stdout != "" {
				t.Errorf("stdout = %q, want empty", stdout)
			}
			if !strings.Contains(stderr, "must either both be omitted or both be set") {
				t.Errorf("stderr = %q, want partial precondition error", stderr)
			}
			if strings.Contains(stderr, "Usage:") {
				t.Errorf("stderr = %q, did not expect Usage on manifest error", stderr)
			}
		})
	}
}
