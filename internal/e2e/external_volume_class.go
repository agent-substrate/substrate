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

package e2e

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

const (
	externalVolumeClassAllowedRole = "allowed_atespace"
	maxPolicyGrantAttempts         = 5
)

type externalVolumeClassAccessPolicyClient interface {
	GetExternalVolumeClassAccessPolicy(ctx context.Context, in *ateapi.GetExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	CreateExternalVolumeClassAccessPolicy(ctx context.Context, in *ateapi.CreateExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
	UpdateExternalVolumeClassAccessPolicy(ctx context.Context, in *ateapi.UpdateExternalVolumeClassAccessPolicyRequest, opts ...grpc.CallOption) (*ateapi.AccessPolicy, error)
}

// EnsureExternalVolumeClassAtespaceAccess grants atespace permission to
// reference className via its ExternalVolumeClassAccessPolicy. It does not
// revoke the grant on cleanup, making it safe for suite-shared atespaces used
// by parallel tests.
func EnsureExternalVolumeClassAtespaceAccess(ctx context.Context, t *testing.T, clients *Clients, className, atespace string) {
	t.Helper()
	if _, err := allowAtespaceOnExternalVolumeClass(ctx, clients.SubstrateAPI, className, atespace); err != nil {
		t.Fatalf("granting atespace %q access to ExternalVolumeClass %q: %v", atespace, className, err)
	}
}

// AllowAtespaceOnExternalVolumeClass grants atespace permission to reference
// className and, if the grant was newly added, registers a t.Cleanup to revoke
// it when t ends. Use this for per-test atespaces.
func AllowAtespaceOnExternalVolumeClass(ctx context.Context, t *testing.T, clients *Clients, className, atespace string) {
	t.Helper()
	added, err := allowAtespaceOnExternalVolumeClass(ctx, clients.SubstrateAPI, className, atespace)
	if err != nil {
		t.Fatalf("granting atespace %q access to ExternalVolumeClass %q: %v", atespace, className, err)
	}
	if !added {
		return
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := revokeAtespaceOnExternalVolumeClass(cleanupCtx, clients.SubstrateAPI, className, atespace); err != nil {
			t.Logf("cleanup: revoking atespace %q access from ExternalVolumeClass %q: %v", atespace, className, err)
		}
	})
}

func allowAtespaceOnExternalVolumeClass(ctx context.Context, client externalVolumeClassAccessPolicyClient, className, atespace string) (bool, error) {
	member := "atespace:" + atespace
	ref := &ateapi.ObjectRef{Name: className}

	var lastErr error
	for range maxPolicyGrantAttempts {
		current, err := client.GetExternalVolumeClassAccessPolicy(ctx, &ateapi.GetExternalVolumeClassAccessPolicyRequest{
			ExternalVolumeClass: ref,
			Name:                resources.SingletonName,
		})
		if err != nil {
			if status.Code(err) != codes.NotFound {
				return false, err
			}
			_, err = client.CreateExternalVolumeClassAccessPolicy(ctx, &ateapi.CreateExternalVolumeClassAccessPolicyRequest{
				ExternalVolumeClass: ref,
				AccessPolicy: &ateapi.AccessPolicy{
					Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName},
					Bindings: []*ateapi.Binding{{
						Role:    externalVolumeClassAllowedRole,
						Members: []string{member},
					}},
				},
			})
			if err == nil {
				return true, nil
			}
			if status.Code(err) == codes.AlreadyExists {
				lastErr = err
				continue
			}
			return false, err
		}

		updated := proto.CloneOf(current)
		if !addPolicyMember(updated, externalVolumeClassAllowedRole, member) {
			return false, nil
		}
		_, err = client.UpdateExternalVolumeClassAccessPolicy(ctx, &ateapi.UpdateExternalVolumeClassAccessPolicyRequest{
			ExternalVolumeClass: ref,
			AccessPolicy:        updated,
		})
		if err == nil {
			return true, nil
		}
		if status.Code(err) == codes.Aborted || status.Code(err) == codes.NotFound {
			lastErr = err
			continue
		}
		return false, err
	}
	return false, fmt.Errorf("exceeded %d attempts updating access policy for external volume class %q: %w", maxPolicyGrantAttempts, className, lastErr)
}

func revokeAtespaceOnExternalVolumeClass(ctx context.Context, client externalVolumeClassAccessPolicyClient, className, atespace string) error {
	member := "atespace:" + atespace
	ref := &ateapi.ObjectRef{Name: className}

	var lastErr error
	for range maxPolicyGrantAttempts {
		current, err := client.GetExternalVolumeClassAccessPolicy(ctx, &ateapi.GetExternalVolumeClassAccessPolicyRequest{
			ExternalVolumeClass: ref,
			Name:                resources.SingletonName,
		})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return err
		}

		updated := proto.CloneOf(current)
		if !removePolicyMember(updated, externalVolumeClassAllowedRole, member) {
			return nil
		}
		_, err = client.UpdateExternalVolumeClassAccessPolicy(ctx, &ateapi.UpdateExternalVolumeClassAccessPolicyRequest{
			ExternalVolumeClass: ref,
			AccessPolicy:        updated,
		})
		if err == nil || status.Code(err) == codes.NotFound {
			return nil
		}
		if status.Code(err) == codes.Aborted {
			lastErr = err
			continue
		}
		return err
	}
	return fmt.Errorf("exceeded %d attempts revoking access policy member for external volume class %q: %w", maxPolicyGrantAttempts, className, lastErr)
}

func addPolicyMember(policy *ateapi.AccessPolicy, role, member string) bool {
	for _, b := range policy.GetBindings() {
		if b.GetRole() != role {
			continue
		}
		if slices.Contains(b.GetMembers(), member) {
			return false
		}
		b.Members = append(b.Members, member)
		return true
	}
	policy.Bindings = append(policy.Bindings, &ateapi.Binding{
		Role:    role,
		Members: []string{member},
	})
	return true
}

func removePolicyMember(policy *ateapi.AccessPolicy, role, member string) bool {
	removed := false
	out := make([]*ateapi.Binding, 0, len(policy.GetBindings()))
	for _, b := range policy.GetBindings() {
		if b.GetRole() != role {
			out = append(out, b)
			continue
		}
		members := make([]string, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			if m == member {
				removed = true
				continue
			}
			members = append(members, m)
		}
		if len(members) > 0 {
			b.Members = members
			out = append(out, b)
		}
	}
	policy.Bindings = out
	return removed
}
