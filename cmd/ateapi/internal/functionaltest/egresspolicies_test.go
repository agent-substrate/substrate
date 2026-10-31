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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
)

// egressPolicySpec is a valid egress policy for an actor in atespace.
func egressPolicySpec(atespace string) *ateapi.EgressPolicy {
	return &ateapi.EgressPolicy{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: "default"},
		Rules:    []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{"api.example.com"}}}},
	}
}

// egressPolicyActor creates an actor for an egress policy to belong to.
func egressPolicyActor(h *harness.Harness, as string) *ateapi.ObjectRef {
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	return harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))
}

// Creates an egress policy for an existing actor and verifies that the whole
// resource has the expected shape.
func TestCreateActorEgressPolicy(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)

	created, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}
	want := &ateapi.EgressPolicy{
		Metadata: &ateapi.ResourceMetadata{Atespace: as, Name: "default"},
		Rules: []*ateapi.EgressRule{{Http: &ateapi.HttpRule{
			Hostnames: []string{"api.example.com"},
			Ports:     &ateapi.Ports{Numbers: []int32{80}},
		}}},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("created egress policy (-want +got):\n%s", diff)
	}
}

// Creating an egress policy for an existing actor name fails with
// ALREADY_EXISTS and leaves the existing one unchanged.
func TestCreateActorEgressPolicy_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	created, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}

	_, err = h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateActorEgressPolicy for an actor that has one = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("egress policy after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an egress policy with an invalid actor name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestCreateActorEgressPolicy_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{
		Actor:        &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"},
		EgressPolicy: egressPolicySpec(as),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActorEgressPolicy with an invalid actor name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("CreateActorEgressPolicy error %q does not name the invalid field actor.name", msg)
	}
}

// Creating an egress policy for an actor that doesn't exist fails with
// FAILED_PRECONDITION.
func TestCreateActorEgressPolicy_ActorNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{
		Actor:        &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"},
		EgressPolicy: egressPolicySpec(as),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActorEgressPolicy for a missing actor = %v, want FailedPrecondition", err)
	}
}

// Getting an existing egress policy returns exactly the resource
// CreateActorEgressPolicy returned.
func TestGetActorEgressPolicy(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	created, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}

	got, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorEgressPolicy (-created +got):\n%s", diff)
	}
}

// Getting an egress policy that doesn't exist fails with NOT_FOUND.
func TestGetActorEgressPolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)

	_, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActorEgressPolicy for an actor with none = %v, want NotFound", err)
	}
}

// Getting an egress policy with an invalid actor name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestGetActorEgressPolicy_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetActorEgressPolicy with an invalid actor name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("GetActorEgressPolicy error %q does not name the invalid field actor.name", msg)
	}
}

// After an actor with an egress policy is deleted, getting its egress policy
// fails with NOT_FOUND. An actor created again under the same name has no
// egress policy either: getting it fails with NOT_FOUND.
func TestGetActorEgressPolicy_ActorDeleted(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	if _, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)}); err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}
	template := h.GetActor(actor).GetActorTemplate()

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: actor}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	_, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActorEgressPolicy of a deleted actor = %v, want NotFound", err)
	}

	if _, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: actor.GetAtespace(), Name: actor.GetName()},
		ActorTemplate: template,
	}}); err != nil {
		t.Fatalf("CreateActor under the same name: %v", err)
	}
	_, err = h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActorEgressPolicy of an actor recreated under the same name = %v, want NotFound", err)
	}
}

// Updates the rules of an existing egress policy, sending it as read. The
// result, and a later Get, is the resource as read with that field changed
// and a higher version.
func TestUpdateActorEgressPolicy(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	before, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}
	rules := []*ateapi.EgressRule{{TlsPassthrough: &ateapi.TlsPassthroughRule{Hostnames: []string{"*"}, Ports: &ateapi.Ports{Numbers: []int32{443}}}}}

	toUpdate := proto.Clone(before).(*ateapi.EgressPolicy)
	toUpdate.Rules = rules
	updated, err := h.Control.UpdateActorEgressPolicy(t.Context(), &ateapi.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: toUpdate})
	if err != nil {
		t.Fatalf("UpdateActorEgressPolicy: %v", err)
	}
	want := proto.Clone(before).(*ateapi.EgressPolicy)
	want.Rules = rules
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated egress policy (-want +got):\n%s", diff)
	}
	if got, was := updated.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("updated egress policy has version %d, want higher than %d", got, was)
	}
	got, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActorEgressPolicy after update (-updated +got):\n%s", diff)
	}
}

// Updating an egress policy that doesn't exist fails with NOT_FOUND.
func TestUpdateActorEgressPolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	policy := egressPolicySpec(as)
	policy.Metadata.Uid, policy.Metadata.Version = foreignUID, 1

	_, err := h.Control.UpdateActorEgressPolicy(t.Context(), &ateapi.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: policy})
	if status.Code(err) != codes.NotFound {
		t.Errorf("UpdateActorEgressPolicy for an actor with none = %v, want NotFound", err)
	}
}

// Updating an egress policy with the wrong UID fails with ABORTED, and with
// the right UID but the wrong version fails with ABORTED; both leave it
// unchanged. Updating it without a UID or a version fails with
// INVALID_ARGUMENT.
func TestUpdateActorEgressPolicy_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	before, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}
	update := func(uid string, version int64) error {
		toUpdate := proto.Clone(before).(*ateapi.EgressPolicy)
		toUpdate.Metadata.Uid, toUpdate.Metadata.Version = uid, version
		toUpdate.Rules = []*ateapi.EgressRule{{Http: &ateapi.HttpRule{Hostnames: []string{"other.example.com"}}}}
		_, err := h.Control.UpdateActorEgressPolicy(t.Context(), &ateapi.UpdateActorEgressPolicyRequest{Actor: actor, EgressPolicy: toUpdate})
		return err
	}

	if err := update(foreignUID, before.GetMetadata().GetVersion()); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateActorEgressPolicy with the wrong uid = %v, want Aborted", err)
	}
	if err := update(before.GetMetadata().GetUid(), before.GetMetadata().GetVersion()+1); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateActorEgressPolicy with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(before, got, protocmp.Transform()); diff != "" {
		t.Errorf("egress policy after refused updates (-want +got):\n%s", diff)
	}
	if err := update("", 0); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateActorEgressPolicy without a uid or version = %v, want InvalidArgument", err)
	}
}

// Updating an egress policy with an invalid actor name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestUpdateActorEgressPolicy_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	policy := egressPolicySpec(as)
	policy.Metadata.Uid, policy.Metadata.Version = foreignUID, 1

	_, err := h.Control.UpdateActorEgressPolicy(t.Context(), &ateapi.UpdateActorEgressPolicyRequest{
		Actor:        &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"},
		EgressPolicy: policy,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateActorEgressPolicy with an invalid actor name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("UpdateActorEgressPolicy error %q does not name the invalid field actor.name", msg)
	}
}

// Deletes an existing egress policy. The response is the deleted resource,
// and a later Get fails with NOT_FOUND.
func TestDeleteActorEgressPolicy(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	created, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}

	deleted, err := h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("DeleteActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteActorEgressPolicy response (-created +deleted):\n%s", diff)
	}
	if _, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActorEgressPolicy after delete = %v, want NotFound", err)
	}
}

// Deleting an egress policy that doesn't exist fails with NOT_FOUND.
func TestDeleteActorEgressPolicy_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)

	_, err := h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: actor})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteActorEgressPolicy for an actor with none = %v, want NotFound", err)
	}
}

// Deleting an egress policy with the wrong UID fails with ABORTED, and with
// the right UID but the wrong version fails with ABORTED; both leave it
// unchanged. With the right UID and version the delete succeeds.
func TestDeleteActorEgressPolicy_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	actor := egressPolicyActor(h, as)
	created, err := h.Control.CreateActorEgressPolicy(t.Context(), &ateapi.CreateActorEgressPolicyRequest{Actor: actor, EgressPolicy: egressPolicySpec(as)})
	if err != nil {
		t.Fatalf("CreateActorEgressPolicy: %v", err)
	}
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

	_, err = h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: actor, Options: &ateapi.DeleteOptions{Uid: foreignUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActorEgressPolicy with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: actor, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActorEgressPolicy with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetActorEgressPolicy(t.Context(), &ateapi.GetActorEgressPolicyRequest{Name: "default", Actor: actor})
	if err != nil {
		t.Fatalf("GetActorEgressPolicy after refused deletes: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("egress policy after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: actor, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Errorf("DeleteActorEgressPolicy with the right uid and version: %v", err)
	}
}

// Deleting an egress policy with an invalid actor name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestDeleteActorEgressPolicy_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteActorEgressPolicy(t.Context(), &ateapi.DeleteActorEgressPolicyRequest{Name: "default", Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteActorEgressPolicy with an invalid actor name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("DeleteActorEgressPolicy error %q does not name the invalid field actor.name", msg)
	}
}
