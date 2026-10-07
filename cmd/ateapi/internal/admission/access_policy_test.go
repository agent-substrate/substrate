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

package admission

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestCreateAccessPolicyValidation(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	adm := New(persistence, nil, nil)

	if _, err := adm.CreateGlobalAccessPolicy(ctx, &ateapipb.AccessPolicy{
		Bindings: []*ateapipb.Binding{{Role: "editor", Members: []string{"user:alice@example.com"}}},
	}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "access_policy.bindings[0].role") {
		t.Errorf("CreateGlobalAccessPolicy(invalid role) = %v, want ErrInvalid on access_policy.bindings[0].role", err)
	}
	if _, err := adm.CreateAtespaceAccessPolicy(ctx, "ns", &ateapipb.AccessPolicy{
		Bindings: []*ateapipb.Binding{{Role: "invalid-role", Members: []string{"user:alice@example.com"}}},
	}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "access_policy.bindings[0].role") {
		t.Errorf("CreateAtespaceAccessPolicy(invalid role) = %v, want ErrInvalid on access_policy.bindings[0].role", err)
	}

	if _, err := adm.CreateAtespace(ctx, &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: "ns"},
	}); err != nil {
		t.Fatalf("CreateAtespace(ns): %v", err)
	}
	if _, err := adm.CreateGlobalAccessPolicy(ctx, &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{Role: "owner", Members: []string{"user:alice@example.com"}}},
	}); err != nil {
		t.Errorf("CreateGlobalAccessPolicy(valid) = %v, want nil", err)
	}
	if _, err := adm.CreateAtespaceAccessPolicy(ctx, "ns", &ateapipb.AccessPolicy{
		Metadata: &ateapipb.ResourceMetadata{Name: "default"},
		Bindings: []*ateapipb.Binding{{Role: "editor", Members: []string{"user:alice@example.com"}}},
	}); err != nil {
		t.Errorf("CreateAtespaceAccessPolicy(valid) = %v, want nil", err)
	}
}
