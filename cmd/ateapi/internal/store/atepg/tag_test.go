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

package atepg

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// createTestTag creates tagName over its own copy of an actor's external
// snapshot, already finalized.
func createTestTag(t *testing.T, s *Persistence, tagAtespace, tagName string) *ateapipb.Tag {
	t.Helper()
	tag, err := s.CreateTag(context.Background(), &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: tagAtespace, Name: tagName},
		Scope:    ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			Snapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/atespaces/" + tagAtespace + "/tags/" + tagName},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag(%s/%s) failed: %v", tagAtespace, tagName, err)
	}
	return tag
}
