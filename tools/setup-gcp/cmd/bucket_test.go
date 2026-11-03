// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
)

func TestValidateBucketFlags(t *testing.T) {
	origGetProject := getProjectFn
	defer func() { getProjectFn = origGetProject }()

	getProjectFn = func(ctx context.Context, name string) (*resourcemanagerpb.Project, error) {
		return &resourcemanagerpb.Project{
			Name:      "projects/123456",
			ProjectId: "test-project",
		}, nil
	}

	tooLong := strings.Repeat("b", 57)
	tooLongErr := `bucket name "` + tooLong + `" is too long: the asset bucket "` + tooLong + `-assets" would have 64 characters, and GCS allows 63`

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "missing bucket name",
			cfg:  Config{ProjectID: "test-project"},
			want: "--name is required",
		},
		{
			name: "bucket name too long for the asset bucket",
			cfg:  Config{ProjectID: "test-project", BucketName: tooLong},
			want: tooLongErr,
		},
		{
			name: "longest bucket name the asset bucket allows",
			cfg:  Config{ProjectID: "test-project", BucketName: strings.Repeat("b", 56)},
		},
		{
			name: "missing project id",
			cfg:  Config{BucketName: "test-bucket"},
			want: "--project-id is required",
		},
		{
			name: "missing project id when only project number provided",
			cfg:  Config{BucketName: "test-bucket", ProjectNumber: "123456"},
			want: "--project-id is required",
		},
		{
			name: "all required flags present with both id and number",
			cfg:  Config{ProjectID: "test-project", ProjectNumber: "123456", BucketName: "test-bucket"},
		},
		{
			name: "all required flags present with only project id",
			cfg:  Config{ProjectID: "test-project", BucketName: "test-bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateBucketFlags(t.Context(), &cfgCopy)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateBucketFlags() = %v; want nil", err)
				}
				if cfgCopy.ProjectNumber != "123456" {
					t.Errorf("cfgCopy.ProjectNumber = %q; want %q", cfgCopy.ProjectNumber, "123456")
				}
				return
			}
			if err == nil {
				t.Fatalf("validateBucketFlags() = nil; want %q", tt.want)
			}
			if err.Error() != tt.want {
				t.Errorf("validateBucketFlags() = %q; want %q", err.Error(), tt.want)
			}
		})
	}
}

// TestBucketGrantsPerMember pins what each member may reach: atelet reads
// the asset bucket only, and the transfer account, like ate-api-server, may
// read and write the snapshot bucket only.
func TestBucketGrantsPerMember(t *testing.T) {
	cfg := &Config{ProjectID: "proj", ProjectNumber: "123", BucketName: "snaps", TransferGSAName: "ate-snapshot-transfer"}
	grantsOf := func(member string) []bucketGrant {
		var got []bucketGrant
		for _, g := range bucketGrants(cfg) {
			if g.member == member {
				got = append(got, g)
			}
		}
		return got
	}
	atelet := workloadIdentityPrincipal(cfg, "atelet")
	transfer := "serviceAccount:ate-snapshot-transfer@proj.iam.gserviceaccount.com"
	for _, tc := range []struct {
		member string
		want   []bucketGrant
	}{
		{atelet, []bucketGrant{{bucket: "snaps-assets", member: atelet, role: "roles/storage.objectViewer"}}},
		{transfer, []bucketGrant{
			{bucket: "snaps", member: transfer, role: "roles/storage.objectAdmin"},
			{bucket: "snaps", member: transfer, role: "roles/storage.bucketViewer"},
		}},
	} {
		if got := grantsOf(tc.member); !slices.Equal(got, tc.want) {
			t.Errorf("grants for %s = %v, want %v", tc.member, got, tc.want)
		}
	}
}

func TestAddGrantsOnlyTouchesTheNamedBucket(t *testing.T) {
	cfg := &Config{ProjectID: "proj", ProjectNumber: "123", BucketName: "snaps"}
	member := func(sa string) string {
		return "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/proj.svc.id.goog/subject/ns/ate-system/sa/" + sa
	}
	policy := &iam.Policy{InternalProto: &iampb.Policy{}}
	if !addGrants(policy, cfg, "snaps-assets", bucketGrants(cfg)) {
		t.Fatal("addGrants on the asset bucket added nothing")
	}
	if got := policy.Members("roles/storage.objectViewer"); !slices.Equal(got, []string{member("atelet")}) {
		t.Errorf("objectViewer members = %v, want only atelet", got)
	}
	if got := policy.Members("roles/storage.objectAdmin"); len(got) != 0 {
		t.Errorf("objectAdmin members on the asset bucket = %v, want none", got)
	}
	if addGrants(policy, cfg, "snaps-assets", bucketGrants(cfg)) {
		t.Error("a second addGrants added bindings the policy already has")
	}
}
