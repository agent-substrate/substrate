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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// A listing fails as a whole on one undecodable row, so the error has to name
// the row.
func TestList_NamesAnUndecodableRow(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "t1")
	createTestTag(t, s, "team-a", "v1")
	if _, err := s.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: "a1", Atespace: "team-a"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := s.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "w1"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	}); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Not a valid encoding of any message.
	bad := []byte{0xff}
	for _, table := range []string{"atespaces", "actors", "actor_templates", "tags", "workers"} {
		if _, err := s.pool.Exec(ctx, "UPDATE "+table+" SET proto = $1", bad); err != nil {
			t.Fatalf("corrupting %s: %v", table, err)
		}
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO worker_assignments (actor_uid, worker_name, proto) VALUES ('uid-1', 'w1', $1)`, bad); err != nil {
		t.Fatalf("inserting assignment: %v", err)
	}

	opts := store.ListOptions{PageSize: 10}
	tests := []struct {
		name string
		list func() error
		want string
	}{
		{"atespaces", func() error { _, err := s.ListAtespaces(ctx, opts); return err }, "atespace team-a"},
		{"actors", func() error { _, err := s.ListActors(ctx, "team-a", opts); return err }, "actor team-a/a1"},
		{"actors, all atespaces", func() error { _, err := s.ListActors(ctx, "", opts); return err }, "actor team-a/a1"},
		{"actor templates", func() error { _, err := s.ListActorTemplates(ctx, "team-a", opts); return err }, "actor template team-a/t1"},
		{"actor templates, all atespaces", func() error { _, err := s.ListActorTemplates(ctx, "", opts); return err }, "actor template team-a/t1"},
		{"tags", func() error { _, err := s.ListTags(ctx, "team-a", opts); return err }, "tag team-a/v1"},
		{"tags, all atespaces", func() error { _, err := s.ListTags(ctx, "", opts); return err }, "tag team-a/v1"},
		{"workers", func() error { _, err := s.ListWorkers(ctx, opts); return err }, "worker w1"},
		{"worker assignments", func() error { _, err := s.ListWorkerAssignments(ctx, "w1", opts); return err }, "actor uid-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.list()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("list error = %v, want one naming %q", err, tc.want)
			}
		})
	}
}
