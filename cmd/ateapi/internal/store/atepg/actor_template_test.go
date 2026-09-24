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
	"errors"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func createTestActorTemplate(t *testing.T, s *Persistence, atespace, name string) {
	t.Helper()
	if _, err := s.CreateActorTemplate(context.Background(), &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
	}); err != nil {
		t.Fatalf("CreateActorTemplate(%q/%q) failed: %v", atespace, name, err)
	}
}

func TestUpdateActorTemplate_ConcurrentWriteReturnsConflict(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")
	createTestActorTemplate(t, s, "team-a", "template-a")
	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "template-a"}
	created, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}

	mutations := 0
	_, err = s.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(created), func(toUpdate *ateapipb.ActorTemplate) error {
		mutations++
		if _, err := s.UpdateActorTemplate(ctx, templateRef, store.PreconditionFrom(created), func(concurrent *ateapipb.ActorTemplate) error {
			concurrent.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
			return nil
		}); err != nil {
			return fmt.Errorf("concurrent actor template update: %w", err)
		}
		toUpdate.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{
			ErrorMessage: "LosingUpdate",
		}}
		return nil
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("UpdateActorTemplate error = %v, want ErrVersionConflict", err)
	}
	if mutations != 1 {
		t.Errorf("mutation ran %d times, want 1", mutations)
	}
	stored, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	if got := stored.GetWorkerSelector().GetMatchLabels()["tier"]; got != "paid" {
		t.Errorf("worker selector tier = %q, want paid", got)
	}
	if got := stored.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); got != "" {
		t.Errorf("status error message = %q, want empty: losing update was persisted", got)
	}
}

func TestGetActorTemplate_BackfillsDefaults(t *testing.T) {
	s := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, s, "team-a")

	if _, err := s.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
		Metadata:       &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "template-a"},
		SnapshotConfig: &ateapipb.SnapshotConfig{},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	templateRef := resources.ActorTemplateRef{Atespace: "team-a", Name: "template-a"}
	got, err := s.GetActorTemplate(ctx, templateRef)
	if err != nil {
		t.Fatalf("GetActorTemplate failed: %v", err)
	}
	want := &ateapipb.SnapshotConfig{
		OnPause:  ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		OnCommit: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		OnResume: &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT},
	}
	if diff := cmp.Diff(want, got.GetSnapshotConfig(), protocmp.Transform()); diff != "" {
		t.Errorf("GetActorTemplate did not backfill snapshot_config defaults (-want +got):\n%s", diff)
	}

	// ListActorTemplates decodes rows on its own path; it must backfill too.
	page, err := s.ListActorTemplates(ctx, "team-a", store.ListOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("ListActorTemplates returned %d items, want 1", len(page.Items))
	}
	if diff := cmp.Diff(want, page.Items[0].GetSnapshotConfig(), protocmp.Transform()); diff != "" {
		t.Errorf("ListActorTemplates did not backfill snapshot_config defaults (-want +got):\n%s", diff)
	}
}
