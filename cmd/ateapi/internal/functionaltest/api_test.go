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

package functionaltest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
)

// A request carrying a field the server does not know fails with
// INVALID_ARGUMENT.
func TestRejectUnknownFields(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	req := &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: "team-a"},
	}}
	unknown := protowire.AppendTag(nil, 9999, protowire.VarintType)
	req.ProtoReflect().SetUnknown(protowire.AppendVarint(unknown, 42))

	_, err := h.Control.CreateAtespace(t.Context(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateAtespace with an unknown field = %v, want InvalidArgument", err)
	}
	listed, err := h.Control.ListAtespaces(t.Context(), &ateapi.ListAtespacesRequest{})
	if err != nil {
		t.Fatalf("ListAtespaces: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListAtespacesResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("atespaces after a refused create (-want +got):\n%s", diff)
	}
}
