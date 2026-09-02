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

package validation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

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

func selectorLabelsOfSize(n int) map[string]string {
	labels := make(map[string]string, n)
	for i := 0; i < n; i++ {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	return labels
}

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
		"missing actor.actor_template",
		validReq(validActor(func(a *ateapipb.Actor) { a.ActorTemplate = nil })),
		field.ErrorList{field.Required(field.NewPath("actor", "actor_template"), "")},
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
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateActorRequest(context.Background(), tt.req), tt.want)
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
		"change actor.actor_template is allowed",
		validInput(withActorTemplate("as1", "nm1")),
		validOutput(withActorTemplate("as2", "nm2")),
		nil,
	}, {
		"clear actor.actor_template",
		validInput(withActorTemplate("as", "nm")),
		validOutput(func(a *ateapipb.Actor) { a.ActorTemplate = nil }),
		field.ErrorList{field.Required(field.NewPath("actor_template"), "")},
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
	}, {
		"valid actor.status.in_progress_snapshot_name",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.InProgressSnapshotName = "snap-1" })),
		nil,
	}, {
		"invalid actor.status.in_progress_snapshot_name",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.InProgressSnapshotName = "SNAP 1" })),
		field.ErrorList{field.Invalid(field.NewPath("status", "in_progress_snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid actor.status.latest_snapshot",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LatestSnapshot = &ateapipb.ObjectRef{Atespace: "as", Name: "snap-1"}
		})),
		nil,
	}, {
		"missing actor.status.latest_snapshot.atespace",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LatestSnapshot = &ateapipb.ObjectRef{Name: "snap-1"}
		})),
		field.ErrorList{field.Required(field.NewPath("status", "latest_snapshot", "atespace"), "")},
	}, {
		"valid actor.status.local_snapshot_info.snapshot_name",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "snap-1"}
		})),
		nil,
	}, {
		"invalid actor.status.local_snapshot_info.snapshot_name",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{SnapshotName: "SNAP 1"}
		})),
		field.ErrorList{field.Invalid(field.NewPath("status", "local_snapshot_info", "snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid actor.status.local_snapshot_info.node_vms entry",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{NodeVmsWithLocalSnapshots: []string{"node-1", "NOT A NODE"}}
		})),
		field.ErrorList{field.Invalid(field.NewPath("status", "local_snapshot_info", "node_vms_with_local_snapshots").Index(1), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		"too many actor.status.local_snapshot_info.node_vms entries",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			nodes := make([]string, 257)
			for i := range nodes {
				nodes[i] = fmt.Sprintf("node-%d", i)
			}
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{NodeVmsWithLocalSnapshots: nodes}
		})),
		field.ErrorList{field.TooMany(field.NewPath("status", "local_snapshot_info", "node_vms_with_local_snapshots"), 257, 256).WithOrigin("maxItems")},
	}, {
		"duplicate actor.status.local_snapshot_info.node_vms entry",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{NodeVmsWithLocalSnapshots: []string{"node-1", "node-1"}}
		})),
		field.ErrorList{field.Duplicate(field.NewPath("status", "local_snapshot_info", "node_vms_with_local_snapshots").Index(1), nil)},
	}, {
		"valid actor.status.local_snapshot_info.content_scope",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA}
		})),
		nil,
	}, {
		"negative actor.status.local_snapshot_info.content_scope",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{ContentScope: ateapipb.SnapshotContentScope(-1)}
		})),
		field.ErrorList{field.Invalid(field.NewPath("status", "local_snapshot_info", "content_scope"), nil, "").WithOrigin("minimum")},
	}, {
		"invalid actor.status.local_snapshot_info.content_scope",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.LocalSnapshotInfo = &ateapipb.LocalSnapshotInfo{ContentScope: ateapipb.SnapshotContentScope(3)}
		})),
		field.ErrorList{field.Invalid(field.NewPath("status", "local_snapshot_info", "content_scope"), nil, "").WithOrigin("maximum")},
	}, {
		"negative actor.status.in_progress_snapshot_source_actor_version",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.InProgressSnapshotSourceActorVersion = -1 })),
		field.ErrorList{field.Invalid(field.NewPath("status", "in_progress_snapshot_source_actor_version"), nil, "").WithOrigin("minimum")},
	}, {
		"too many actor_volumes",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			vols := make([]*ateapipb.ExternalVolume, 33)
			for i := range vols {
				vols[i] = &ateapipb.ExternalVolume{VolumeName: fmt.Sprintf("vol-%d", i), VolumeType: "substrate.io/mock"}
			}
			s.ActorVolumes = vols
		})),
		field.ErrorList{field.TooMany(field.NewPath("status", "actor_volumes"), 33, 32).WithOrigin("maxItems")},
	}, {
		// Set-once fields permit the nil->set transition, so a volume added
		// in an update validates like one added at creation.
		"adding a volume on update is allowed",
		validInput(withStatus()),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.ActorVolumes = []*ateapipb.ExternalVolume{{VolumeName: "vol-a", VolumeType: "substrate.io/mock"}}
		})),
		nil,
	}, {
		"duplicate actor_volumes volume_name",
		validInput(withStatus()),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.ActorVolumes = []*ateapipb.ExternalVolume{
				{VolumeName: "vol-a", VolumeType: "substrate.io/mock"},
				{VolumeName: "vol-a", VolumeType: "substrate.io/mock"},
			}
		})),
		field.ErrorList{field.Duplicate(field.NewPath("status", "actor_volumes").Index(1), nil)},
	}, {
		"provisioning transition on an existing volume is valid",
		validInput(withStatus(func(s *ateapipb.ActorStatus) {
			s.ActorVolumes = []*ateapipb.ExternalVolume{{VolumeName: "vol-a", VolumeType: "substrate.io/mock", Status: ateapipb.ExternalVolume_STATUS_PENDING}}
		})),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.ActorVolumes = []*ateapipb.ExternalVolume{{
				VolumeName:      "vol-a",
				VolumeType:      "substrate.io/mock",
				StorageVolumeId: "csi-426d29b7",
				Status:          ateapipb.ExternalVolume_STATUS_CREATED,
				VolumeContext:   map[string]string{"attachment": "iqn.2026-08.io.ate:vol-a"},
			}}
		})),
		nil,
	}, {
		"invalid actor.status.in_progress_local_snapshot_name",
		validInput(),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.InProgressLocalSnapshotName = "BAD NAME" })),
		field.ErrorList{field.Invalid(field.NewPath("status", "in_progress_local_snapshot_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"set actor.status.source_snapshot",
		validInput(withStatus()),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.SourceSnapshot = &ateapipb.ActorSourceSnapshotStatus{
				Snapshot:    &ateapipb.ObjectRef{Atespace: "as", Name: "snap-1"},
				SnapshotUid: "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63",
			}
		})),
		nil,
	}, {
		"clear actor.status.source_snapshot",
		validInput(withStatus(func(s *ateapipb.ActorStatus) {
			s.SourceSnapshot = &ateapipb.ActorSourceSnapshotStatus{
				Snapshot:    &ateapipb.ObjectRef{Atespace: "as", Name: "snap-1"},
				SnapshotUid: "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63",
			}
		})),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) { s.SourceSnapshot = nil })),
		field.ErrorList{field.Invalid(field.NewPath("status", "source_snapshot"), nil, "").WithOrigin("update")},
	}, {
		"change actor.status.source_snapshot",
		validInput(withStatus(func(s *ateapipb.ActorStatus) {
			s.SourceSnapshot = &ateapipb.ActorSourceSnapshotStatus{
				Snapshot:    &ateapipb.ObjectRef{Atespace: "as", Name: "snap-1"},
				SnapshotUid: "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63",
			}
		})),
		validOutput(withStatus(func(s *ateapipb.ActorStatus) {
			s.SourceSnapshot = &ateapipb.ActorSourceSnapshotStatus{
				Snapshot:    &ateapipb.ObjectRef{Atespace: "as", Name: "snap-2"},
				SnapshotUid: "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63",
			}
		})),
		field.ErrorList{field.Invalid(field.NewPath("status", "source_snapshot"), nil, "").WithOrigin("update")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateActorUpdate(context.Background(), nil, tt.newVal, tt.oldVal, true), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetActorRequest(context.Background(), tt.req), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"valid, positive page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "").WithOrigin("minimum")},
	}, {
		"valid page_token",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageToken: strings.Repeat("x", 256)},
		nil,
	}, {
		"too-large page_token",
		&ateapipb.ListActorsRequest{Atespace: "ns1", PageToken: strings.Repeat("x", 257)},
		field.ErrorList{field.TooLongCharacters(field.NewPath("page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListActorsRequest(context.Background(), tt.req), tt.want)
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
			assertValidateErr(t, ValidateUpdateActorRequest(context.Background(), tt.req), tt.want)
		})
	}
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
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteActorRequest(context.Background(), tt.req), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.PauseActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidatePauseActorRequest(context.Background(), tt.req), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateResumeActorRequest(context.Background(), tt.req), tt.want)
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
		field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), "NS1", "").WithOrigin("format=k8s-short-name")},
	}, {
		"missing actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1"}},
		field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		"invalid actor.name",
		&ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "ns1", Name: "ID1"}},
		field.ErrorList{field.Invalid(field.NewPath("actor", "name"), "ID1", "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateSuspendActorRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func validExternalVolume(mutate ...func(*ateapipb.ExternalVolume)) *ateapipb.ExternalVolume {
	v := &ateapipb.ExternalVolume{
		VolumeName:      "my-vol",
		StorageVolumeId: "valid-storage-id",
		VolumeType:      "mock",
		Status:          ateapipb.ExternalVolume_STATUS_CREATED,
	}
	for _, m := range mutate {
		m(v)
	}
	return v
}

func TestValidateExternalVolume(t *testing.T) {
	valid := validExternalVolume

	tests := []struct {
		name string
		obj  *ateapipb.ExternalVolume
		want field.ErrorList
	}{{
		name: "valid external volume",
		obj:  valid(),
	}, {
		name: "missing volume name",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_name"), "")},
	}, {
		name: "invalid volume name",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "NOT A VOLUME" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "valid external volume with empty storage volume id",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
	}, {
		name: "invalid storage volume id with null U+0000",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x00id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with unit separator U+001F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x1fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with DEL U+007F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\x7fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with C1 control U+0080",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\u0080id" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "invalid storage volume id with C1 control U+009F",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol\u009fid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "")},
	}, {
		name: "storage volume id too long",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = strings.Repeat("x", 257) }),
		want: field.ErrorList{field.TooLong(field.NewPath("storage_volume_id"), nil, 256).WithOrigin("maxLength")},
	}, {
		name: "valid csi volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "pd.csi.storage.gke.io" }),
	}, {
		name: "missing volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "" }),
		want: field.ErrorList{field.Required(field.NewPath("volume_type"), "")},
	}, {
		name: "invalid volume type with uppercase",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "MockPlugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "valid volume type with 253 characters",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = strings.Repeat("a", 253) }),
	}, {
		name: "invalid volume type exceeding 253 characters",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = strings.Repeat("a", 254) }),
		want: field.ErrorList{
			field.Invalid(field.NewPath("volume_type"), nil, ""),
			field.TooLong(field.NewPath("volume_type"), nil, 253).WithOrigin("maxLength"),
		},
	}, {
		name: "valid volume with substrate.io prefixed volume type",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/mock" }),
	}, {
		name: "invalid volume type with empty plugin after substrate.io prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "invalid volume type with invalid plugin name after substrate.io prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "substrate.io/Mock_Plugin" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "invalid volume type with non-substrate prefix",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "other.io/mock" }),
		want: field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "")},
	}, {
		name: "negative status",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.Status = ateapipb.ExternalVolume_Status(-1) }),
		want: field.ErrorList{field.Invalid(field.NewPath("status"), nil, "").WithOrigin("minimum")},
	}, {
		name: "status outside the enum",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.Status = ateapipb.ExternalVolume_Status(4) }),
		want: field.ErrorList{field.Invalid(field.NewPath("status"), nil, "").WithOrigin("maximum")},
	}, {
		name: "storage volume id at the bound",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = strings.Repeat("x", 256) }),
	}, {
		name: "too many volume_context entries",
		obj: valid(func(v *ateapipb.ExternalVolume) {
			ctxMap := make(map[string]string, 33)
			for i := 0; i < 33; i++ {
				ctxMap[fmt.Sprintf("key-%d", i)] = "v"
			}
			v.VolumeContext = ctxMap
		}),
		want: field.ErrorList{field.TooMany(field.NewPath("volume_context"), 33, 32).WithOrigin("maxProperties")},
	}, {
		name: "volume_context key too long",
		obj:  valid(func(v *ateapipb.ExternalVolume) { v.VolumeContext = map[string]string{strings.Repeat("k", 129): "v"} }),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context"), nil, 128).WithOrigin("maxLength")},
	}, {
		name: "volume_context value too long",
		obj: valid(func(v *ateapipb.ExternalVolume) {
			v.VolumeContext = map[string]string{"attachment": strings.Repeat("v", 257)}
		}),
		want: field.ErrorList{field.TooLong(field.NewPath("volume_context").Key("attachment"), nil, 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}

func TestValidateExternalVolume_Update(t *testing.T) {
	valid := validExternalVolume

	tests := []struct {
		name   string
		oldObj *ateapipb.ExternalVolume
		newObj *ateapipb.ExternalVolume
		want   field.ErrorList
	}{{
		name:   "unchanged volume is valid",
		oldObj: valid(),
		newObj: valid(),
	}, {
		name:   "volume_name changed is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "vol1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeName = "vol2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("volume_name"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id transition from empty to non-empty is valid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
	}, {
		name:   "storage_volume_id changed once set is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-2" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "storage_volume_id unset once set is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "vol-id-1" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.StorageVolumeId = "" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("storage_volume_id"), nil, "").WithOrigin("update")},
	}, {
		name:   "volume_type changed is invalid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "mock" }),
		newObj: valid(func(v *ateapipb.ExternalVolume) { v.VolumeType = "pd.csi.storage.gke.io" }),
		want:   field.ErrorList{field.Invalid(field.NewPath("volume_type"), nil, "").WithOrigin("update")},
	}, {
		name: "status and volume_context changed is valid",
		oldObj: valid(func(v *ateapipb.ExternalVolume) {
			v.Status = ateapipb.ExternalVolume_STATUS_PENDING
			v.VolumeContext = nil
		}),
		newObj: valid(func(v *ateapipb.ExternalVolume) {
			v.Status = ateapipb.ExternalVolume_STATUS_CREATED
			v.VolumeContext = map[string]string{"foo": "bar"}
		}),
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Update}
			assertValidateErr(t, Validate_ExternalVolume(context.Background(), op, nil, tt.newObj, tt.oldObj), tt.want)
		})
	}
}
