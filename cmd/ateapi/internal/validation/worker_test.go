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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	apiWorkerName      = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"
	apiOtherWorkerName = "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17"
)

// validWorker returns a Worker in the shape CreateWorker accepts: named, with
// its pod coordinates filled in and no status — status is output-only.
func validWorker(name string, mods ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    name,
		NodeName:        "node-1",
		Ip:              "10.1.2.3",
		SandboxClass:    "gvisor",
		Capacity:        &ateapipb.WorkerCapacity{CpuMilli: 2000, MemoryBytes: 4 << 30},
	}
	for _, m := range mods {
		m(w)
	}
	return w
}

// withWorkerMetadata returns a modifier func (see validWorker) which sets
// the worker's resource metadata to a valid value.
func withWorkerMetadata(mutate func(*ateapipb.ResourceMetadata)) func(*ateapipb.Worker) {
	return func(a *ateapipb.Worker) { mutate(a.Metadata) }
}

// withWorkerStatus returns a modifier func (see validWorker) which sets the
// actor's status to a valid value.
func withWorkerStatus(mods ...func(*ateapipb.WorkerStatus)) func(*ateapipb.Worker) {
	return func(a *ateapipb.Worker) {
		a.Status = &ateapipb.WorkerStatus{
			State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
		}
		for _, m := range mods {
			m(a.Status)
		}
	}
}

func newAPIAssignment(actorUID string) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		ActorTemplateRef: &ateapipb.ObjectRef{Atespace: "ate-system", Name: "tmpl"},
		Actor:            &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid:         actorUID,
	}
}

func workerRef(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: name}
}

func TestValidateListWorkersRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListWorkersRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		&ateapipb.ListWorkersRequest{},
		nil,
	}, {
		"valid, positive page_size",
		&ateapipb.ListWorkersRequest{PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListWorkersRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "").WithOrigin("minimum")},
	}, {
		"valid page_token",
		&ateapipb.ListWorkersRequest{PageToken: strings.Repeat("x", 256)},
		nil,
	}, {
		"too-large page_token",
		&ateapipb.ListWorkersRequest{PageToken: strings.Repeat("x", 257)},
		field.ErrorList{field.TooLongCharacters(field.NewPath("page_token"), "", 256).WithOrigin("maxLength")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListWorkersRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestValidateWorker pins the field paths validateWorker reports.
// TestCreateWorker_InvalidArgument drives the same rules through the RPC, but
// only observes the status code.
func TestValidateCreateWorkerRequest(t *testing.T) {
	// This test verifies validation of user input for creation. The RPC scrubs
	// status before validating, so status is absent from the valid shape; when
	// a request does carry one, it is validated like any other field.
	validReq := func(actor *ateapipb.Worker, mods ...func(actor *ateapipb.CreateWorkerRequest)) *ateapipb.CreateWorkerRequest {
		req := &ateapipb.CreateWorkerRequest{
			Worker: actor,
		}
		for _, m := range mods {
			m(req)
		}
		return req
	}
	withStatus := withWorkerStatus
	withMetadata := withWorkerMetadata

	tests := []struct {
		name string
		req  *ateapipb.CreateWorkerRequest
		want field.ErrorList
	}{{
		name: "valid unassigned worker",
		req:  validReq(validWorker(apiWorkerName)),
	}, {
		name: "valid with status",
		req:  validReq(validWorker(apiWorkerName, withStatus())),
	}, {
		name: "missing worker",
		req:  &ateapipb.CreateWorkerRequest{Worker: nil},
		want: field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		name: "missing metadata",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Metadata = nil })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "metadata"), "")},
	}, {
		name: "missing metadata.name",
		req:  validReq(validWorker(apiWorkerName, withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "" }))),
		want: field.ErrorList{field.Required(field.NewPath("worker", "metadata", "name"), "")},
	}, {
		name: "invalid metadata.name",
		req:  validReq(validWorker(apiWorkerName, withMetadata(func(m *ateapipb.ResourceMetadata) { m.Name = "not a name" }))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "metadata.atespace set on a global-scoped Worker",
		req:  validReq(validWorker(apiWorkerName, withMetadata(func(m *ateapipb.ResourceMetadata) { m.Atespace = "team-a" }))),
		want: field.ErrorList{field.Forbidden(field.NewPath("worker", "metadata", "atespace"), "")},
	}, {
		name: "missing worker_namespace",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerNamespace = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "worker_namespace"), "")},
	}, {
		name: "invalid worker_namespace",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerNamespace = "NS-1" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "worker_namespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing worker_pool",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPool = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "worker_pool"), "")},
	}, {
		name: "invalid worker_pool",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPool = "POOL_1" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "worker_pool"), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		name: "missing worker_pod",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPod = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "worker_pod"), "")},
	}, {
		name: "invalid worker_pod",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPod = "POD_1" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "worker_pod"), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		name: "missing worker_pod_uid",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPodUid = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "worker_pod_uid"), "")},
	}, {
		name: "invalid worker_pod_uid",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.WorkerPodUid = "INVALID-UUID" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "worker_pod_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "missing node_name",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.NodeName = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "node_name"), "")},
	}, {
		name: "invalid node_name",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.NodeName = "NODE_NAME" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "node_name"), nil, "").WithOrigin("format=k8s-long-name")},
	}, {
		name: "missing ip",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ip = "" })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "ip"), "")},
	}, {
		name: "invalid ip",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ip = "not-an-ip" })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "ip"), nil, "").WithOrigin("format=ip-strict")},
	}, {
		name: "sandbox_class too long",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.SandboxClass = strings.Repeat("x", 64) })),
		want: field.ErrorList{field.TooLong(field.NewPath("worker", "sandbox_class"), nil, 63).WithOrigin("maxLength")},
	}, {
		name: "valid labels",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			w.Labels = map[string]string{"tier": "batch", "pool.ate.io/zone": "us-west1-c"}
		})),
	}, {
		name: "too many labels",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			labels := make(map[string]string, 65)
			for i := 0; i < 65; i++ {
				labels[fmt.Sprintf("key-%d", i)] = "v"
			}
			w.Labels = labels
		})),
		want: field.ErrorList{field.TooMany(field.NewPath("worker", "labels"), 65, 64).WithOrigin("maxProperties")},
	}, {
		name: "invalid label key",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Labels = map[string]string{"bad key!": "batch"} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "labels"), "bad key!", "").WithOrigin("format=k8s-label-key")},
	}, {
		name: "invalid label value",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Labels = map[string]string{"tier": "not valid!"} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "labels").Key("tier"), "not valid!", "").WithOrigin("format=k8s-label-value")},
	}, {
		name: "absent capacity is allowed",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Capacity = nil })),
	}, {
		name: "valid capacity",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Capacity = &ateapipb.WorkerCapacity{CpuMilli: 2000, MemoryBytes: 4 << 30} })),
	}, {
		name: "negative capacity.cpu_milli",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Capacity = &ateapipb.WorkerCapacity{CpuMilli: -1, MemoryBytes: 4 << 30} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "capacity", "cpu_milli"), nil, "").WithOrigin("minimum")},
	}, {
		name: "negative capacity.memory_bytes",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Capacity = &ateapipb.WorkerCapacity{CpuMilli: 2000, MemoryBytes: -1} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "capacity", "memory_bytes"), nil, "").WithOrigin("minimum")},
	}, {
		name: "status needs a state",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.State = 0 }))),
		want: field.ErrorList{field.Required(field.NewPath("worker", "status", "state"), "")},
	}, {
		name: "status invalid state (too small)",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.State = -1 }))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "state"), nil, "").WithOrigin("minimum")},
	}, {
		name: "status invalid state (too large)",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.State = 99 }))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "state"), nil, "").WithOrigin("maximum")},
	}, {
		name: "valid assignment, when carried, passes",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment(apiOtherWorkerName)
		}))),
	}, {
		name: "assignment actor_uid must be a uuid",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment("not a uuid")
		}))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "assignment", "actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "assignment actor ref needs an atespace",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment(apiOtherWorkerName)
			s.Assignment.Actor.Atespace = ""
		}))),
		want: field.ErrorList{field.Required(field.NewPath("worker", "status", "assignment", "actor", "atespace"), "")},
	}, {
		name: "assignment template ref needs an atespace",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment(apiOtherWorkerName)
			s.Assignment.ActorTemplateRef = &ateapipb.ObjectRef{Name: "tmpl"}
		}))),
		want: field.ErrorList{field.Required(field.NewPath("worker", "status", "assignment", "actor_template_ref", "atespace"), "")},
	}, {
		name: "assignment needs a template ref",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment(apiOtherWorkerName)
			s.Assignment.ActorTemplateRef = nil
		}))),
		want: field.ErrorList{field.Required(field.NewPath("worker", "status", "assignment", "actor_template_ref"), "")},
	}, {
		name: "assignment template name must be a short name",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Assignment = newAPIAssignment(apiOtherWorkerName)
			s.Assignment.ActorTemplateRef.Name = "TMPL_1"
		}))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "assignment", "actor_template_ref", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCreateWorkerRequest(context.Background(), tc.req), tc.want)
		})
	}
}

func TestValidateDeleteWorkerRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.DeleteWorkerRequest
		want field.ErrorList
	}{{
		"valid, no options",
		&ateapipb.DeleteWorkerRequest{Worker: workerRef(apiWorkerName)},
		nil,
	}, {
		"valid, both guards",
		&ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{Uid: apiOtherWorkerName, Version: 3},
		},
		nil,
	}, {
		"missing worker",
		&ateapipb.DeleteWorkerRequest{},
		field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		"missing worker.name",
		&ateapipb.DeleteWorkerRequest{Worker: &ateapipb.ObjectRef{}},
		field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}, {
		"worker.atespace must be empty",
		&ateapipb.DeleteWorkerRequest{Worker: &ateapipb.ObjectRef{Atespace: "team-a", Name: apiWorkerName}},
		field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		"invalid options.uid",
		&ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{Uid: "not-a-uuid"},
		},
		field.ErrorList{field.Invalid(field.NewPath("options", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"negative options.version",
		&ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{Version: -1},
		},
		field.ErrorList{field.Invalid(field.NewPath("options", "version"), nil, "").WithOrigin("minimum")},
	}, {
		// Zero values waive the guards, so they are never validated for shape.
		"zero options are waived, not validated",
		&ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{},
		},
		nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteWorkerRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateUpdateWorkerRequest(t *testing.T) {
	// This test verifies validation of user input for update. The worker body
	// is deliberately not descended into here (updates are validated in two
	// steps); only the metadata that addresses the resource is checked.
	validReq := func(mods ...func(w *ateapipb.Worker)) *ateapipb.UpdateWorkerRequest {
		worker := validWorker(apiWorkerName)
		worker.Metadata.Uid = apiOtherWorkerName
		worker.Metadata.Version = 3
		for _, m := range mods {
			m(worker)
		}
		return &ateapipb.UpdateWorkerRequest{Worker: worker}
	}

	tests := []struct {
		name string
		req  *ateapipb.UpdateWorkerRequest
		want field.ErrorList
	}{{
		"valid",
		validReq(),
		nil,
	}, {
		// uid and version are preconditions the store requires; the request
		// validation deliberately leaves their presence to the store.
		"missing uid and version pass request validation",
		validReq(func(w *ateapipb.Worker) { w.Metadata.Uid = ""; w.Metadata.Version = 0 }),
		nil,
	}, {
		"missing worker",
		&ateapipb.UpdateWorkerRequest{},
		field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		"missing metadata",
		validReq(func(w *ateapipb.Worker) { w.Metadata = nil }),
		field.ErrorList{field.Required(field.NewPath("worker", "metadata"), "")},
	}, {
		"missing metadata.name",
		validReq(func(w *ateapipb.Worker) { w.Metadata.Name = "" }),
		field.ErrorList{field.Required(field.NewPath("worker", "metadata", "name"), "")},
	}, {
		"invalid metadata.name",
		validReq(func(w *ateapipb.Worker) { w.Metadata.Name = "Not A Name" }),
		field.ErrorList{field.Invalid(field.NewPath("worker", "metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		"invalid metadata.uid",
		validReq(func(w *ateapipb.Worker) { w.Metadata.Uid = "not-a-uuid" }),
		field.ErrorList{field.Invalid(field.NewPath("worker", "metadata", "uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		"metadata.atespace set on a global-scoped Worker",
		validReq(func(w *ateapipb.Worker) { w.Metadata.Atespace = "team-a" }),
		field.ErrorList{field.Forbidden(field.NewPath("worker", "metadata", "atespace"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateUpdateWorkerRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestValidateWorkerUpdate_RequireStatus pins the final-object check that the
// RPC path cannot reach: the server always sets status before storing, so only
// a direct call shows the guard catching a worker without one.
func TestValidateWorkerUpdate_RequireStatus(t *testing.T) {
	oldVal := validWorker(apiWorkerName)
	oldVal.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	newVal := proto.Clone(oldVal).(*ateapipb.Worker)
	newVal.Status = nil

	want := field.ErrorList{field.Required(field.NewPath("worker", "status"), "")}
	assertValidateErr(t, ValidateWorkerUpdate(context.Background(), field.NewPath("worker"), newVal, oldVal, true), want)

	// Without requireStatus the same worker passes: status is optional in the
	// schema, and clearing it is not otherwise constrained.
	assertValidateErr(t, ValidateWorkerUpdate(context.Background(), field.NewPath("worker"), newVal, oldVal, false), nil)
}

func TestValidateDeleteOptions(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.DeleteOptions)) *ateapipb.DeleteOptions {
		tb := &ateapipb.DeleteOptions{}
		for _, m := range mutate {
			m(tb)
		}
		return tb
	}

	tests := []struct {
		name string
		obj  *ateapipb.DeleteOptions
		want field.ErrorList
	}{{
		name: "valid",
		obj:  valid(), // all optional fields
	}, {
		name: "valid version",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Version = 1 }),
		want: nil,
	}, {
		name: "invalid version",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Version = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("version"), nil, "").WithOrigin("minimum")},
	}, {
		name: "valid uid",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Uid = "11111111-2222-3333-4444-555555555555" }),
		want: nil,
	}, {
		name: "invalid uid",
		obj:  valid(func(do *ateapipb.DeleteOptions) { do.Uid = "not a uid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := operation.Operation{Type: operation.Create}
			assertValidateErr(t, Validate_DeleteOptions(context.Background(), op, nil, tt.obj, nil), tt.want)
		})
	}
}
