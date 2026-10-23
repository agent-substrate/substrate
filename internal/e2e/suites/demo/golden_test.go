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

package demo

import (
	"context"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGoldenSnapshotLifecycle(t *testing.T) {
	for _, fidelity := range []ateapi.SnapshotFidelity{
		ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
	} {
		t.Run(fidelity.String(), func(t *testing.T) {
			ctx := t.Context()
			clients := e2e.GetClients()
			ns := e2e.CreateNamespace(t)
			env, err := e2e.CheckEnv("BUCKET_NAME")
			if err != nil {
				t.Fatal(err)
			}
			// A distinct atespace catches placement in a system atespace and
			// keeps cleanup independent of the suite's shared demo atespace.
			atespace := "golden-" + ns.Name
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				_, _ = clients.SubstrateAPI.DeleteAtespace(cleanupCtx, &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: atespace}})
			})
			created := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns.Name, e2e.SubstrateTemplateOptions{
				Atespace: atespace, Name: "counter", PoolName: "counter", PoolReplicas: 1,
				Labels: map[string]string{"golden-e2e": ns.Name},
				SnapshotConfig: &ateapi.SnapshotConfig{
					StorageLocation:   "gs://" + env["BUCKET_NAME"] + "/" + atespace,
					PreferredFidelity: fidelity,
				},
			})

			templateRef := e2e.TemplateRef(created)

			tmpl, err := clients.SubstrateAPI.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: templateRef})
			if err != nil {
				t.Fatal(err)
			}
			tagRef := tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()
			if tagRef.GetAtespace() != atespace {
				t.Fatalf("golden tag atespace = %q, want %q", tagRef.GetAtespace(), atespace)
			}
			if got := tagRef.GetName(); got != tmpl.GetMetadata().GetUid() {
				t.Fatalf("golden tag name = %q, want template UID %q", got, tmpl.GetMetadata().GetUid())
			}
			tag, err := clients.SubstrateAPI.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef})
			if err != nil {
				t.Fatal(err)
			}
			if source := tag.GetSourceActor(); source.GetAtespace() != atespace || source.GetName() != tmpl.GetMetadata().GetUid() {
				t.Fatalf("golden source actor = %v, want %s/%s", source, atespace, tmpl.GetMetadata().GetUid())
			}
			if got := tag.GetStatus().GetSnapshot().GetFidelity(); got != fidelity {
				t.Fatalf("golden fidelity = %v, want %v", got, fidelity)
			}
			uri, err := resources.ParseSnapshotURI(tagSnapshotURI(tag))
			if err != nil {
				t.Fatal(err)
			}
			if !uri.OwnedBy(resources.TagSnapshotOwner(atespace, tag.GetMetadata().GetUid())) {
				t.Fatalf("golden snapshot %s is not owned by its tenant tag", uri)
			}
			actorRef := &ateapi.ObjectRef{Atespace: atespace, Name: "clone"}
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: actorRef})
			})
			actor, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: actorRef.Name}, ActorTemplate: templateRef,
			}})
			if err != nil {
				t.Fatal(err)
			}
			if got := durableSnapshotURI(actor.GetStatus()); got != uri.String() {
				t.Fatalf("clone snapshot = %q, want golden %q", got, uri)
			}
			if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{Actor: actorRef}); err != nil {
				t.Fatal(err)
			}
			response, err := callActor(t, resources.ActorRefFromObjectRef(actorRef))
			if err != nil {
				t.Fatal(err)
			}
			validateCounterResponse(t, response, "golden restore", 1, 1)
			// Delete the borrower before collecting the template's snapshot.
			if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: actorRef}); err != nil {
				t.Fatal(err)
			}
			if _, err := clients.SubstrateAPI.DeleteActorTemplate(ctx, &ateapi.DeleteActorTemplateRequest{ActorTemplate: templateRef}); err != nil {
				t.Fatal(err)
			}
			if _, err := clients.SubstrateAPI.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef}); status.Code(err) != codes.NotFound {
				t.Fatalf("golden tag after template deletion = %v, want NotFound", err)
			}
			if _, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: tagRef}); status.Code(err) != codes.NotFound {
				t.Fatalf("golden actor after template deletion = %v, want NotFound", err)
			}
			t.Logf("golden lifecycle passed: fidelity=%v, snapshot=%s", fidelity, uri)
		})
	}
}
