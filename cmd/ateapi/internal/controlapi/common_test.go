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

package controlapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Helpers shared by the unit tests in this package.
const (
	testAtespace = "test-atespace"
	testActorID  = "id1"
)

var (
	ignoreUID        = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "uid")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapipb.ResourceMetadata{}, "create_time", "update_time")
)

func selectorLabelsOfSize(n int) map[string]string {
	labels := make(map[string]string, n)
	for i := 0; i < n; i++ {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	return labels
}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}

// firstAssignment returns the single Actor a Worker is hosting, or nil when it
// is hosting none. These tests place one Actor per Worker, so "the assignment"
// is still a meaningful thing to assert on even though a Worker holds a set;
// asserting through this keeps them readable and would fail loudly (by looking
// at the wrong entry) if a test ever placed two.
func firstAssignment(t *testing.T, st store.Interface, workerName string) *ateapipb.ActorAssignment {
	t.Helper()
	page, err := st.ListWorkerAssignments(context.Background(), workerName, store.ListOptions{})
	if err != nil {
		t.Fatalf("list assignments of worker %q: %v", workerName, err)
	}
	if len(page.Items) == 0 {
		return nil
	}
	return page.Items[0]
}

// seedAssignment places an actor on an already-created worker, which is how a
// test arranges a worker that is already hosting something.
func seedAssignment(t *testing.T, st store.Interface, workerName string, assignment *ateapipb.ActorAssignment) {
	t.Helper()
	if assignment == nil {
		return
	}
	ctx := context.Background()
	if err := st.BindActorToWorker(ctx, workerName, assignment, nil); err != nil {
		t.Fatalf("seed assignment on worker %q: %v", workerName, err)
	}
}
