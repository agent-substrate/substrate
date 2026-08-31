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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateCreateActorRequest(t *testing.T) {
	// This test verifies validation of user input for creation.  Since status
	// is scrubbed on input, we don't need to test the status field here, other
	// than that it is optional. TestValidateActorUpdate covers status
	// validation and updates.
	validReq := func(actor *ateapipb.Actor, mods ...func(actor *ateapipb.CreateActorRequest)) *ateapipb.CreateActorRequest {
		req := &ateapipb.CreateActorRequest{
			Actor: actor,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}
	withStatus := withActorStatus
	withMetadata := withActorMetadata
	withActorTemplate := withActorActorTemplate
	withSourceSnapshotTag := withActorSourceSnapshotTag
	withWorkerSelector := withActorWorkerSelector

	tests := []struct {
		name string
		req  *ateapipb.CreateActorRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(validActor()),
		nil,
	}, {
		"valid with status",
		validReq(validActor(withStatus())),
		nil, // ignored on input
	}, {
		"missing actor",
		&ateapipb.CreateActorRequest{Actor: nil},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata",
		validReq(validActor(func(a *ateapipb.Actor) { a.Metadata = nil })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata"), "")},
	}, {
		"missing actor.metadata.atespace",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "" }))),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "NS1" }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.metadata.name",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" }))),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "ID1" }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid actor.actor_template",
		validReq(validActor(withActorTemplate("as", "tmpl"))),
		nil,
	}, {
		"missing actor.actor_template.atespace",
		validReq(validActor(withActorTemplate("", "tmpl"))),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template", "atespace"), "")},
	}, {
		"invalid actor.actor_template.atespace",
		validReq(validActor(withActorTemplate("invalid value", "tmpl"))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.actor_template.name",
		validReq(validActor(withActorTemplate("as", ""))),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template", "name"), "")},
	}, {
		"invalid actor.actor_template.name",
		validReq(validActor(withActorTemplate("as", "invalid value"))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "actor_template", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid actor.source_snapshot_tag",
		validReq(validActor(withSourceSnapshotTag("as", "tag"))),
		nil,
	}, {
		"missing actor.source_snapshot_tag.atespace",
		validReq(validActor(withSourceSnapshotTag("", "tag"))),
		field.ErrorList{field.Required(field.NewPath("actor", "source_snapshot_tag", "atespace"), "")},
	}, {
		"invalid actor.source_snapshot_tag.atespace",
		validReq(validActor(withSourceSnapshotTag("invalid value", "tag"))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "source_snapshot_tag", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.source_snapshot_tag.name",
		validReq(validActor(withSourceSnapshotTag("as", ""))),
		field.ErrorList{field.Required(field.NewPath("actor", "source_snapshot_tag", "name"), "")},
	}, {
		"invalid actor.source_snapshot_tag.name",
		validReq(validActor(withSourceSnapshotTag("as", "invalid value"))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "source_snapshot_tag", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid worker_selector",
		validReq(validActor(withWorkerSelector(map[string]string{"tier": "1"}))),
		nil,
	}, {
		"worker_selector with nil match_labels",
		validReq(validActor(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{} })),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector"), nil, "one of").WithOrigin("union")},
	}, {
		"worker_selector with empty match_labels",
		validReq(validActor(withWorkerSelector(map[string]string{}))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector"), nil, "one of").WithOrigin("union")},
	}, {
		"worker_selector with exactly max match_labels",
		validReq(validActor(withWorkerSelector(selectorLabelsOfSize(10)))),
		nil,
	}, {
		"too many worker_selector.match_labels",
		validReq(validActor(withWorkerSelector(selectorLabelsOfSize(11)))),
		field.ErrorList{field.TooMany(field.NewPath("actor", "worker_selector", "match_labels"), 11, 10).WithOrigin("maxProperties")},
	}, {
		"invalid worker_selector label key",
		validReq(validActor(withWorkerSelector(map[string]string{"bad key!": "1"}))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels"), "bad key!", "").WithOrigin("format=k8s-label-key")},
	}, {
		"invalid worker_selector label value",
		validReq(validActor(withWorkerSelector(map[string]string{"tier": "not valid!"}))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "worker_selector", "match_labels").Key("tier"), "not valid!", "").WithOrigin("format=k8s-label-value")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateCreateActorRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateActorUpdate(t *testing.T) {
	// This test validates input and output fields, including status.  It also
	// tests updates to all fields.  This is where the majority of validation
	// test cases should live.
	validInput := validActor
	withStatus := withActorStatus
	validOutput := func(mods ...func(*ateapipb.Actor)) *ateapipb.Actor {
		allMods := []func(*ateapipb.Actor){withStatus()} // this needs to go first
		allMods = append(allMods, mods...)
		a := validActor(allMods...)
		return a
	}
	withMetadata := withActorMetadata
	withWorkerSelector := withActorWorkerSelector
	withActorTemplate := withActorActorTemplate
	withSourceSnapshotTag := withActorSourceSnapshotTag
	withWorkerAssignment := withActorWorkerAssignment

	tests := []struct {
		name   string
		oldVal *ateapipb.Actor
		newVal *ateapipb.Actor
		want   field.ErrorList
	}{{
		"valid",
		validInput(),
		validOutput(),
		nil,
	}, {
		"missing actor.metadata",
		validInput(),
		validOutput(func(a *ateapipb.Actor) { a.Metadata = nil }),
		field.ErrorList{field.Required(field.NewPath("metadata"), "")},
	}, {
		"missing actor.metadata.atespace",
		validInput(),
		validOutput(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "" })),
		field.ErrorList{
			field.Required(field.NewPath("metadata", "atespace"), ""),
			field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("immutable"),
		},
	}, {
		"invalid actor.metadata.atespace",
		validInput(),
		validOutput(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "invalid value" })),
		field.ErrorList{field.Invalid(field.NewPath("metadata", "atespace"), nil, "").WithOrigin("immutable")},
	}, {
		"missing actor.metadata.name",
		validInput(),
		validOutput(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" })),
		field.ErrorList{
			field.Required(field.NewPath("metadata", "name"), ""),
			field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("immutable"),
		},
	}, {
		"invalid actor.metadata.name",
		validInput(),
		validOutput(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "invalid value" })),
		field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), nil, "").WithOrigin("immutable")},
	}, {
		"change actor.actor_template",
		validInput(withActorTemplate("as1", "nm1")),
		validOutput(withActorTemplate("as2", "nm2")),
		field.ErrorList{field.Invalid(field.NewPath("actor_template"), nil, "").WithOrigin("immutable")},
	}, {
		"clear actor.actor_template",
		validInput(withActorTemplate("as", "nm")),
		validOutput(func(a *ateapipb.Actor) { a.ActorTemplate = nil }),
		field.ErrorList{field.Invalid(field.NewPath("actor_template"), nil, "").WithOrigin("immutable")},
	}, {
		"add actor.source_snapshot_tag",
		validInput(),
		validOutput(withSourceSnapshotTag("as", "nm")),
		field.ErrorList{field.Invalid(field.NewPath("source_snapshot_tag"), nil, "").WithOrigin("immutable")},
	}, {
		"clear actor.source_snapshot_tag",
		validInput(withSourceSnapshotTag("as", "nm")),
		validOutput(func(a *ateapipb.Actor) { a.SourceSnapshotTag = nil }),
		field.ErrorList{field.Invalid(field.NewPath("source_snapshot_tag"), nil, "").WithOrigin("immutable")},
	}, {
		"change actor.source_snapshot_tag",
		validInput(withSourceSnapshotTag("as1", "nm1")),
		validOutput(withSourceSnapshotTag("as2", "nm2")),
		field.ErrorList{field.Invalid(field.NewPath("source_snapshot_tag"), nil, "").WithOrigin("immutable")},
	}, {
		"set valid worker_selector",
		validInput(),
		validOutput(withWorkerSelector(map[string]string{"tier": "1"})),
		nil,
	}, {
		"clear worker_selector",
		validInput(withWorkerSelector(map[string]string{"tier": "1"})),
		validOutput(),
		nil,
	}, {
		"modify worker_selector",
		validInput(withWorkerSelector(map[string]string{"tier": "1"})),
		validOutput(withWorkerSelector(map[string]string{"tier": "2"})),
		nil,
	}, {
		"invalid worker_selector with nil match_labels",
		validInput(),
		validOutput(func(a *ateapipb.Actor) { a.WorkerSelector = &ateapipb.Selector{} }),
		field.ErrorList{field.Invalid(field.NewPath("worker_selector"), nil, "one of").WithOrigin("union")},
	}, {
		"invalid worker_selector label key",
		validInput(),
		validOutput(withWorkerSelector(map[string]string{"bad key": "2"})),
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels"), nil, "").WithOrigin("format=k8s-label-key")},
	}, {
		"invalid worker_selector label value",
		validInput(),
		validOutput(withWorkerSelector(map[string]string{"tier": "bad value"})),
		field.ErrorList{field.Invalid(field.NewPath("worker_selector", "match_labels").Key("tier"), nil, "").WithOrigin("format=k8s-label-value")},
	}, {
		"too many worker_selector.match_labels",
		validInput(),
		validOutput(withWorkerSelector(selectorLabelsOfSize(11))),
		field.ErrorList{field.TooMany(field.NewPath("worker_selector", "match_labels"), 11, 10).WithOrigin("maxProperties")},
	}, {
		"unspecified actor.status",
		validInput(withStatus()),
		validOutput(func(a *ateapipb.Actor) { a.Status = nil }),
		field.ErrorList{field.Required(field.NewPath("status"), "")},
	}, {
		"unspecified actor.status.state",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = 0 })),
		field.ErrorList{field.Required(field.NewPath("status", "state"), "")},
	}, {
		"change actor.status.state",
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = ateapipb.ActorState_ACTOR_STATE_PAUSED })),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = ateapipb.ActorState_ACTOR_STATE_CRASHED })),
		nil,
	}, {
		"negative actor.status.state",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = -1 })),
		field.ErrorList{field.Invalid(field.NewPath("status", "state"), nil, "").WithOrigin("minimum")},
	}, {
		"just out of bounds actor.status.state",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = 9 })),
		field.ErrorList{field.Invalid(field.NewPath("status", "state"), nil, "").WithOrigin("maximum")},
	}, {
		"invalid actor.status.state",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.State = 1234567890 })),
		field.ErrorList{field.Invalid(field.NewPath("status", "state"), nil, "").WithOrigin("maximum")},
	}, {
		"set valid actor.status.worker_assignment, IPv4",
		validInput(withStatus()),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) { wa.WorkerPodIp = "1.2.3.4" }))),
		nil,
	}, {
		"set valid actor.status.worker_assignment, IPv6",
		validInput(withStatus()),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) { wa.WorkerPodIp = "1234::5678" }))),
		nil,
	}, {
		"clear actor.status.worker_assignment",
		validInput(withStatus(withWorkerAssignment())),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.WorkerAssignment = nil })),
		nil,
	}, {
		"modify actor.status.worker_assignment",
		validInput(withStatus(withWorkerAssignment())),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) { wa.WorkerPod = "pod2" }))),
		field.ErrorList{field.Invalid(field.NewPath("status", "worker_assignment"), nil, "").WithOrigin("update")},
	}, {
		"empty actor.status.worker_assignment",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.WorkerAssignment = &ateapipb.WorkerAssignment{} })),
		field.ErrorList{
			field.Required(field.NewPath("status", "worker_assignment", "worker"), ""),
			field.Required(field.NewPath("status", "worker_assignment", "worker_namespace"), ""),
			field.Required(field.NewPath("status", "worker_assignment", "worker_pool"), ""),
			field.Required(field.NewPath("status", "worker_assignment", "worker_pod"), ""),
			field.Required(field.NewPath("status", "worker_assignment", "worker_pod_uid"), ""),
			field.Required(field.NewPath("status", "worker_assignment", "worker_pod_ip"), ""),
		},
	}, {
		"invalid actor.status.worker_assignment",
		validInput(),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) {
			wa.Worker = &ateapipb.ObjectRef{Atespace: "not-allowed", Name: "bad value"}
			wa.WorkerNamespace = "invalid namespace"
			wa.WorkerPool = "invalid pool"
			wa.WorkerPod = "invalid pod"
			wa.WorkerPodUid = "invalid UUID"
			wa.WorkerPodIp = "invalid IP"
		}))),
		field.ErrorList{
			field.Forbidden(field.NewPath("status", "worker_assignment", "worker", "atespace"), ""),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker", "name"), nil, "").WithOrigin("format=k8s-short-name"),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_namespace"), nil, "").WithOrigin("format=k8s-short-name"),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pool"), nil, "").WithOrigin("format=k8s-long-name"),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pod"), nil, "").WithOrigin("format=k8s-long-name"),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid"),
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pod_ip"), nil, "").WithOrigin("format=ip-strict"),
		},
	}, {
		// because we have manual IP format validation, let's be sure
		"invalid actor.status.worker_assignment_worker_pod_ip: leading 0s",
		validInput(),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) { wa.WorkerPodIp = "001.002.003.004" }))),
		field.ErrorList{
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pod_ip"), nil, "").WithOrigin("format=ip-strict"),
		},
	}, {
		// because we have manual IP format validation, let's be sure
		"invalid actor.status.worker_assignment_worker_pod_ip: non-canonical",
		validInput(),
		validOutput(withStatus(withWorkerAssignment(func(wa *ateapipb.WorkerAssignment) { wa.WorkerPodIp = "0012::0034" }))),
		field.ErrorList{
			field.Invalid(field.NewPath("status", "worker_assignment", "worker_pod_ip"), nil, "").WithOrigin("format=ip-strict"),
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateActorUpdate(context.Background(), nil, tt.newVal, tt.oldVal, true), tt.want)
		})
	}
}

func TestValidateGetActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.GetActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.GetActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateGetActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateListActorsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListActorsRequest
		want field.ErrorList
	}{{
		"valid, atespace scoped",
		&ateapipb.ListActorsRequest{Atespace: "ns1"},
		nil,
	}, {
		// Empty atespace means "all atespaces" (kubectl ate get actors -A).
		"valid, empty atespace means all atespaces",
		&ateapipb.ListActorsRequest{},
		nil,
	}, {
		"invalid atespace",
		&ateapipb.ListActorsRequest{Atespace: "NS1"},
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS1", "")},
	}, {
		"valid, positive page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListActorsRequest(tt.req), tt.want)
		})
	}
}

func TestValidateUpdateActorRequest(t *testing.T) {
	// This test verifies validation of user input for update.  Since status
	// is scrubbed on input, we don't need to test the status field here, other
	// than that it is optional. TestValidateActorUpdate covers status
	// validation and updates.
	validReq := func(actor *ateapipb.Actor, mods ...func(actor *ateapipb.UpdateActorRequest)) *ateapipb.UpdateActorRequest {
		req := &ateapipb.UpdateActorRequest{
			Actor: actor,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}
	validActor := func(mods ...func(*ateapipb.Actor)) *ateapipb.Actor {
		allMods := []func(*ateapipb.Actor){
			func(a *ateapipb.Actor) { // this needs to go first
				a.Metadata.Uid = "12345678-1234-1234-1234-123456789abc"
				a.Metadata.Version = 1
			},
		}
		allMods = append(allMods, mods...)
		a := validActor(allMods...)
		return a
	}
	withStatus := withActorStatus
	withMetadata := withActorMetadata

	tests := []struct {
		name string
		req  *ateapipb.UpdateActorRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(validActor()),
		nil,
	}, {
		"valid with status",
		validReq(validActor(withStatus())),
		nil, // ignored on input
	}, {
		"missing actor",
		&ateapipb.UpdateActorRequest{Actor: nil},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.metadata",
		validReq(validActor(func(a *ateapipb.Actor) { a.Metadata = nil })),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata"), "")},
	}, {
		"missing actor.metadata.atespace",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "" }))),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "atespace"), "")},
	}, {
		"invalid actor.metadata.atespace",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "NS1" }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.metadata.name",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" }))),
		field.ErrorList{field.Required(field.NewPath("actor", "metadata", "name"), "")},
	}, {
		"invalid actor.metadata.name",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "ID1" }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.metadata.uid precondition",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "" }))),
		nil,
	}, {
		"invalid actor.metadata.uid precondition",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Uid = "not-a-uuid" }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "uid"), "not-a-uuid", "").WithOrigin("format=k8s-uuid")},
	}, {
		"missing actor.metadata.version precondition",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = 0 }))),
		nil,
	}, {
		"negative actor.metadata.version precondition",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) { m.Version = -1 }))),
		field.ErrorList{field.Invalid(field.NewPath("actor", "metadata", "version"), int64(-1), "").WithOrigin("minimum")},
	}, {
		"missing actor.metadata.version and actor.metadata.uid",
		validReq(validActor(withMetadata(func(m *ateapipb.ResourceMetadata) {
			m.Uid = ""
			m.Version = 0
		}))),
		nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateUpdateActorRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestUpdateActor(t *testing.T) {
	const templateNS, templateName = "ns1", "tmpl1"

	tests := []struct {
		name     string
		stored   *ateapipb.Actor
		req      *ateapipb.Actor
		want     *ateapipb.Actor
		wantCode codes.Code
	}{
		{
			name:   "sets a worker_selector the stored actor does not have",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "overwrites an existing worker_selector",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate:  &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}},
		},
		{
			name:   "an omitted worker_selector is cleared",
			stored: &ateapipb.Actor{WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "free"}}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "SourceSnapshotTag immutable field is kept",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate:     &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
			want: &ateapipb.Actor{
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"},
				WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
			},
		},
		{
			name:   "changes to status in the request are ignored",
			stored: &ateapipb.Actor{},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			},
			want: &ateapipb.Actor{},
		},
		{
			name:   "an omitted immutable field is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate: &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName},
				// Omitted SourceSnapshotTag
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "an immutable field the request rewrites is rejected",
			stored: &ateapipb.Actor{SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag1"}},
			req: &ateapipb.Actor{
				ActorTemplate:     &ateapipb.ObjectRef{Atespace: "attacker-ns", Name: "attacker-tmpl"},
				SourceSnapshotTag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tag2"},
			},
			wantCode: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.stored.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID}
			tt.stored.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.stored.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}

			svc, created := rpcServiceWithActor(t, tt.stored)

			tt.req.Metadata = created.GetMetadata()
			updated, err := svc.UpdateActor(context.Background(), &ateapipb.UpdateActorRequest{Actor: tt.req})

			if tt.wantCode != codes.OK {
				if code := status.Code(err); code != tt.wantCode {
					t.Errorf("UpdateActor error = %v (code %v), want code %v", err, code, tt.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("UpdateActor failed: %v", err)
			}

			tt.want.Metadata = &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID, Version: 2}
			tt.want.ActorTemplate = &ateapipb.ObjectRef{Atespace: templateNS, Name: templateName}
			tt.want.Status = &ateapipb.ActorStatus{
				State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			}
			if diff := cmp.Diff(tt.want, updated, protocmp.Transform(), ignoreUID, ignoreTimestamps); diff != "" {
				t.Errorf("UpdateActor response mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestUpdateActor_DeleteRecreateRace checks that an update is not applied
// if an actor was deleted and recreated during the update operation.
func TestUpdateActor_DeleteRecreateRace(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	// Actor A: what the client reads, and what its uid precondition names.
	// Freshly created, so it sits at version 1.
	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status: &ateapipb.ActorStatus{
			State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPod: "pod-a"},
		},
	})

	// A concurrent client deletes A and recreates the same atespace/name as a
	// brand new actor B, in the window the handler used to leave open between
	// its own read and the store's WATCH.
	var recreated *ateapipb.Actor
	var err error
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_DELETING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark deleting: %v", err)
			}
			if _, err := persistence.DeleteActor(ctx, actorRef); err != nil {
				t.Fatalf("racing writer: DeleteActor: %v", err)
			}
			recreated, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
				Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
			})
			if err != nil {
				t.Fatalf("racing writer: recreate CreateActor: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil, nil)}

	// The client asserts "only update the actor with uid A".
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err = svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the actor holding uid %s was deleted mid-update",
			err, code, original.GetMetadata().GetUid())
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got, want := stored.GetMetadata().GetUid(), recreated.GetMetadata().GetUid(); got != want {
		t.Fatalf("stored uid = %s, want recreated actor's uid %s", got, want)
	}
	// The stored record must still be actor B as its creator left it. Any of A's
	// state showing up here is the clobber.
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("stored state = %v, want %v: recreated actor was overwritten with the deleted actor's state",
			got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}
	if got := stored.GetStatus().GetWorkerAssignment(); got != nil {
		t.Errorf("stored worker_assignment = %v, want nil: recreated actor inherited the deleted actor's worker", got)
	}
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: update meant for the deleted actor was applied", got)
	}
}

// TestUpdateActor_ConcurrentDisjointUpdates checks that a concurrent write is
// reported even when it touched a field the update does not. The version guards
// the whole actor, not a single field, so the server cannot know the two
// writes commute: it reports the conflict and leaves reconciling to the client.
func TestUpdateActor_ConcurrentDisjointUpdates(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	actorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorID}

	original := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testActorID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// A suspend workflow bumps state (a field that a later update operation will not touch)
	// inside the handler's read-modify-write window.
	racing := &conflictInjectingStore{
		Interface: persistence,
		inject: func() {
			if _, err := persistence.UpdateActor(ctx, actorRef, store.PreconditionFrom(original), func(toUpdate *ateapipb.Actor) error {
				toUpdate.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
				return nil
			}); err != nil {
				t.Fatalf("racing writer: mark suspending: %v", err)
			}
		},
	}
	svc := &RPCService{impl: newServiceImpl(racing, nil, nil)}

	// Update operation is changing the worker_selector field, not the actor's state (like the concurrent op)
	// This update must fail: the racing update bumped the version.
	original.WorkerSelector = &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	_, err := svc.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: original})
	if code := status.Code(err); code != codes.Aborted {
		t.Errorf("UpdateActor error = %v (code %v), want code Aborted: the guarded version moved under the update", err, code)
	}

	stored, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	// The concurrent writer's field survives; the rejected update wrote nothing.
	if got := stored.GetWorkerSelector(); got != nil {
		t.Errorf("stored worker_selector = %v, want nil: the rejected update was applied anyway", got)
	}
	if got := stored.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("stored state = %v, want %v: the concurrent writer's field must survive", got, ateapipb.ActorState_ACTOR_STATE_SUSPENDING)
	}
}

// validActor returns a minimal Actor which should pass input validation.
func validActor(mods ...func(*ateapipb.Actor)) *ateapipb.Actor {
	a := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "ns1", Name: "id1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "ns1", Name: "tmpl1"},
	}
	for _, m := range mods {
		m(a)
	}
	return a
}

// withActorMetadata returns a modifier func (see validActor) which sets
// the actor's resource metadata to a valid value.
func withActorMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) { mutate(a.Metadata) }
}

// withActorStatus returns a modifier func (see validActor) which sets the
// actor's status to a valid value.
func withActorStatus(mods ...func(*ateapipb.ActorStatus)) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) {
		a.Status = &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		}
		for _, m := range mods {
			m(a.Status)
		}
	}
}

// withActorWorkerSelector returns a modifier func (see validActor) which sets
// the actor's worker_selector to a valid value.
func withActorWorkerSelector(labels map[string]string) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) {
		a.WorkerSelector = &ateapipb.Selector{
			MatchLabels: labels,
		}
	}
}

// withActorActorTemplate returns a modifier func (see validActor) which sets
// the actor's actor_template to a valid value.
func withActorActorTemplate(atespace, name string) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) { a.ActorTemplate = &ateapipb.ObjectRef{Atespace: atespace, Name: name} }
}

// withActorSourceSnapshotTag returns a modifier func (see validActor) which sets
// the actor's source_snapshot_tag to a valid value.
func withActorSourceSnapshotTag(atespace, name string) func(*ateapipb.Actor) {
	return func(a *ateapipb.Actor) { a.SourceSnapshotTag = &ateapipb.ObjectRef{Atespace: atespace, Name: name} }
}

// withActorWorkerAssignment returns a modifier func (see validActor) which sets
// the actor's worker_assignment to a valid value.
func withActorWorkerAssignment(mods ...func(*ateapipb.WorkerAssignment)) func(*ateapipb.ActorStatus) {
	return func(s *ateapipb.ActorStatus) {
		s.WorkerAssignment = &ateapipb.WorkerAssignment{
			Worker:          &ateapipb.ObjectRef{Name: "worker"},
			WorkerNamespace: "ns",
			WorkerPool:      "pool",
			WorkerPod:       "pod",
			WorkerPodUid:    "12345678-1234-1234-1234-123456789abc",
			WorkerPodIp:     "1.2.3.4",
		}
		for _, m := range mods {
			m(s.WorkerAssignment)
		}
	}
}

// rpcServiceWithActor seeds one actor in a PostgreSQL-backed store and returns an
// RPCService over it.
func rpcServiceWithActor(t *testing.T, actor *ateapipb.Actor) (*RPCService, *ateapipb.Actor) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)

	created := storetest.MustCreateActor(t, context.Background(), persistence, actor)
	return &RPCService{impl: newServiceImpl(persistence, nil, nil)}, created
}

func TestValidateDeleteActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.DeleteActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateDeleteActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidatePauseActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.PauseActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.PauseActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validatePauseActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateResumeActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ResumeActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.ResumeActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateResumeActorRequest(tt.req), tt.want)
		})
	}
}

func TestValidateSuspendActorRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.SuspendActorRequest
		want field.ErrorList
	}{{
		"valid",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "id1"}},
		nil,
	}, {
		"missing actor",
		&ateapipb.SuspendActorRequest{},
		field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		"missing actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Name: "id1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		"invalid actor.atespace",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "NS1", Name: "id1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "")},
	}, {
		"missing actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateSuspendActorRequest(tt.req), tt.want)
		})
	}
}
