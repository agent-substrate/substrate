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

package apivalidation

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Worker names are pod UIDs, which are opaque to everything above the syncer.
const (
	apiWorkerName      = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"
	apiOtherWorkerName = "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17"
)

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
		name: "valid dual-stack status.ips",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.Ips = []string{"fd00::1", "10.1.2.3"} }))),
	}, {
		name: "invalid status.ips",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.Ips = []string{"not-an-ip"} }))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "ips").Index(0), nil, "").WithOrigin("format=ip-strict")},
	}, {
		name: "two status.ips in one family",
		req:  validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) { s.Ips = []string{"10.1.2.3", "10.1.2.4"} }))),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "status", "ips").Index(1), nil, "")},
	}, {
		name: "too many status.ips",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.Ips = []string{"10.1.2.3", "fd00::1", "10.1.2.4"}
		}))),
		want: field.ErrorList{field.TooMany(field.NewPath("worker", "status", "ips"), 3, 2).WithOrigin("maxItems")},
	}, {
		name: "negative status epochs",
		req: validReq(validWorker(apiWorkerName, withStatus(func(s *ateapipb.WorkerStatus) {
			s.IssuedEpoch = -1
			s.RegisteredEpoch = -1
		}))),
		want: field.ErrorList{
			field.Invalid(field.NewPath("worker", "status", "issued_epoch"), nil, "").WithOrigin("minimum"),
			field.Invalid(field.NewPath("worker", "status", "registered_epoch"), nil, "").WithOrigin("minimum"),
		},
	}, {
		name: "valid worker_spiffe_id",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			w.WorkerSpiffeId = "spiffe://cluster.local/ns/ate-system/sa/ateworker"
		})),
	}, {
		name: "invalid worker_spiffe_id",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			w.WorkerSpiffeId = "https://cluster.local/ns/ate-system/sa/ateworker"
		})),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "worker_spiffe_id"), nil, "")},
	}, {
		name: "worker_spiffe_id too long",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			w.WorkerSpiffeId = "spiffe://cluster.local/" + strings.Repeat("a", 2048)
		})),
		want: field.ErrorList{field.TooLong(field.NewPath("worker", "worker_spiffe_id"), nil, 2048).WithOrigin("maxLength")},
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

func TestValidateAcquireWorkerEpochRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.AcquireWorkerEpochRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  &ateapipb.AcquireWorkerEpochRequest{Worker: workerRef(apiWorkerName)},
	}, {
		name: "missing worker",
		req:  &ateapipb.AcquireWorkerEpochRequest{},
		want: field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		name: "worker.atespace must be empty",
		req:  &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{Atespace: "team-a", Name: apiWorkerName}},
		want: field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		name: "missing worker.name",
		req:  &ateapipb.AcquireWorkerEpochRequest{Worker: &ateapipb.ObjectRef{}},
		want: field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateAcquireWorkerEpochRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateRegisterWorkerRequest(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.RegisterWorkerRequest)) *ateapipb.RegisterWorkerRequest {
		r := &ateapipb.RegisterWorkerRequest{
			Worker: workerRef(apiWorkerName),
			Epoch:  1,
			Ips:    []string{"10.1.2.3"},
			Capacity: &ateapipb.WorkerResources{
				Actors:        10,
				CpuMillicores: 2000, MemoryBytes: 4 << 30,
			},
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	tests := []struct {
		name string
		req  *ateapipb.RegisterWorkerRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  valid(),
	}, {
		name: "valid empty capacity",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity = &ateapipb.WorkerResources{} }),
	}, {
		name: "valid dual-stack ips",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = []string{"fd00::1", "10.1.2.3"} }),
	}, {
		name: "missing worker",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Worker = nil }),
		want: field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		name: "worker.atespace must be empty",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Worker.Atespace = "team-a" }),
		want: field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		name: "missing worker.name",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Worker.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}, {
		name: "invalid worker.name",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Worker.Name = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing epoch",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Epoch = 0 }),
		want: field.ErrorList{field.Required(field.NewPath("epoch"), "")},
	}, {
		name: "negative epoch",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Epoch = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("epoch"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing ips",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = nil }),
		want: field.ErrorList{field.Required(field.NewPath("ips"), "")},
	}, {
		name: "invalid ip",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = []string{"not-an-ip"} }),
		want: field.ErrorList{field.Invalid(field.NewPath("ips").Index(0), nil, "").WithOrigin("format=ip-strict")},
	}, {
		name: "two ips in one family",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = []string{"10.1.2.3", "10.1.2.4"} }),
		want: field.ErrorList{field.Invalid(field.NewPath("ips").Index(1), nil, "")},
	}, {
		name: "too many ips",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Ips = []string{"10.1.2.3", "fd00::1", "10.1.2.4"} }),
		want: field.ErrorList{field.TooMany(field.NewPath("ips"), 3, 2).WithOrigin("maxItems")},
	}, {
		name: "missing capacity",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity = nil }),
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}, {
		name: "negative capacity.actors",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity.Actors = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "actors"), nil, "").WithOrigin("minimum")},
	}, {
		name: "negative capacity.memory_bytes",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity.MemoryBytes = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "memory_bytes"), nil, "").WithOrigin("minimum")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRegisterWorkerRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateSetWorkerSandboxRuntimesRequest(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.SetWorkerSandboxRuntimesRequest)) *ateapipb.SetWorkerSandboxRuntimesRequest {
		r := &ateapipb.SetWorkerSandboxRuntimesRequest{
			Worker:         workerRef(apiWorkerName),
			Epoch:          1,
			SandboxClasses: []*ateapipb.WorkerSandboxClass{validSandboxClass()},
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	classesPath := field.NewPath("sandbox_classes")
	classPath := classesPath.Index(0)
	defaultRuntimePath := classPath.Child("default_runtime")
	defaultVersionPath := defaultRuntimePath.Child("version")
	restorablePath := classPath.Child("restorable_runtimes")
	tests := []struct {
		name string
		req  *ateapipb.SetWorkerSandboxRuntimesRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  valid(),
	}, {
		name: "valid with restorable runtimes",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = []*ateapipb.WorkerSandboxRuntime{{Version: &ateapipb.VersionedSandboxCompat{
				SchemaVersion: "v1",
				Attributes: []*ateapipb.AttributeEntry{
					{Key: "architecture", Value: "amd64"},
				},
			}}}
		}),
	}, {
		name: "missing worker",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Worker = nil }),
		want: field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		name: "worker.atespace must be empty",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Worker.Atespace = "team-a" }),
		want: field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		name: "missing epoch",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Epoch = 0 }),
		want: field.ErrorList{field.Required(field.NewPath("epoch"), "")},
	}, {
		name: "negative epoch",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.Epoch = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("epoch"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing sandbox_classes",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses = nil }),
		want: field.ErrorList{field.Required(classesPath, "")},
	}, {
		name: "empty sandbox_classes",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses = []*ateapipb.WorkerSandboxClass{} }),
		want: field.ErrorList{field.Required(classesPath, "")},
	}, {
		name: "too many sandbox_classes",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			second := validSandboxClass()
			second.SandboxClass = "microvm"
			r.SandboxClasses = append(r.SandboxClasses, second)
		}),
		want: field.ErrorList{field.TooMany(classesPath, 2, 1).WithOrigin("maxItems")},
	}, {
		name: "missing sandbox_classes.sandbox_class",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses[0].SandboxClass = "" }),
		want: field.ErrorList{field.Required(classPath.Child("sandbox_class"), "")},
	}, {
		name: "sandbox_classes.sandbox_class too long",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].SandboxClass = strings.Repeat("c", 64)
		}),
		want: field.ErrorList{
			field.TooLong(classPath.Child("sandbox_class"), "", 63).WithOrigin("maxLength"),
			field.NotSupported[string](classPath.Child("sandbox_class"), nil, nil),
		},
	}, {
		name: "unsupported sandbox_classes.sandbox_class",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses[0].SandboxClass = "kata" }),
		want: field.ErrorList{field.NotSupported[string](classPath.Child("sandbox_class"), nil, nil)},
	}, {
		name: "missing sandbox_classes.default_runtime",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses[0].DefaultRuntime = nil }),
		want: field.ErrorList{field.Required(defaultRuntimePath, "")},
	}, {
		name: "missing default_runtime.version",
		req:  valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) { r.SandboxClasses[0].DefaultRuntime.Version = nil }),
		want: field.ErrorList{field.Required(defaultVersionPath, "")},
	}, {
		name: "missing default_runtime.schema_version",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.SchemaVersion = ""
		}),
		want: field.ErrorList{field.Required(defaultVersionPath.Child("schema_version"), "")},
	}, {
		name: "default_runtime.schema_version too long",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.SchemaVersion = strings.Repeat("v", 65)
		}),
		want: field.ErrorList{field.TooLong(defaultVersionPath.Child("schema_version"), "", 64).WithOrigin("maxLength")},
	}, {
		name: "missing default_runtime.attributes",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = nil
		}),
		want: field.ErrorList{field.Required(defaultVersionPath.Child("attributes"), "")},
	}, {
		name: "duplicate attribute key",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
				{Key: "architecture", Value: "arm64"},
			}
		}),
		want: field.ErrorList{field.Duplicate(defaultVersionPath.Child("attributes").Index(1), nil)},
	}, {
		name: "missing attribute key",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Value: "amd64"}}
		}),
		want: field.ErrorList{field.Required(defaultVersionPath.Child("attributes").Index(0).Child("key"), "")},
	}, {
		name: "attribute key too long",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: strings.Repeat("k", 129), Value: "v"}}
		}),
		want: field.ErrorList{field.TooLong(defaultVersionPath.Child("attributes").Index(0).Child("key"), "", 128).WithOrigin("maxLength")},
	}, {
		name: "missing attribute value",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: "k"}}
		}),
		want: field.ErrorList{field.Required(defaultVersionPath.Child("attributes").Index(0).Child("value"), "")},
	}, {
		name: "attribute value too long",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: "k", Value: strings.Repeat("v", 257)}}
		}),
		want: field.ErrorList{field.TooLong(defaultVersionPath.Child("attributes").Index(0).Child("value"), "", 256).WithOrigin("maxLength")},
	}, {
		name: "attributes at the bound",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = attributeEntries(32)
		}),
	}, {
		name: "too many attributes",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].DefaultRuntime.Version.Attributes = attributeEntries(33)
		}),
		want: field.ErrorList{field.TooMany(defaultVersionPath.Child("attributes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "nil restorable_runtimes entry",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = []*ateapipb.WorkerSandboxRuntime{nil}
		}),
		want: field.ErrorList{field.Required(restorablePath.Index(0), "")},
	}, {
		name: "invalid restorable_runtimes entry",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = []*ateapipb.WorkerSandboxRuntime{{Version: &ateapipb.VersionedSandboxCompat{Attributes: attributeEntries(1)}}}
		}),
		want: field.ErrorList{field.Required(restorablePath.Index(0).Child("version", "schema_version"), "")},
	}, {
		name: "restorable_runtimes entry without a version",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = []*ateapipb.WorkerSandboxRuntime{{}}
		}),
		want: field.ErrorList{field.Required(restorablePath.Index(0).Child("version"), "")},
	}, {
		name: "restorable_runtimes at the bound",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = sandboxRuntimes(32)
		}),
	}, {
		name: "too many restorable_runtimes",
		req: valid(func(r *ateapipb.SetWorkerSandboxRuntimesRequest) {
			r.SandboxClasses[0].RestorableRuntimes = sandboxRuntimes(33)
		}),
		want: field.ErrorList{field.TooMany(restorablePath, 33, 32).WithOrigin("maxItems")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateSetWorkerSandboxRuntimesRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestValidateWorkerUpdate_SandboxClasses pins the status-side checks on the
// sandbox classes SetWorkerSandboxRuntimes records. They reach the store as
// status, which no request validator sees, so ValidateWorkerUpdate is the only
// check on them. The old worker already reports a class so that every
// mutation, clearing the list included, differs from it and is validated.
func TestValidateWorkerUpdate_SandboxClasses(t *testing.T) {
	classesPath := field.NewPath("worker", "status", "sandbox_classes")
	classPath := classesPath.Index(0)
	oldVal := validWorker(apiWorkerName, withWorkerStatus(func(s *ateapipb.WorkerStatus) {
		s.SandboxClasses = []*ateapipb.WorkerSandboxClass{validSandboxClass()}
	}))
	tests := []struct {
		name   string
		mutate func(s *ateapipb.WorkerStatus)
		want   field.ErrorList
	}{{
		name: "valid runtimes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.SandboxClasses[0].RestorableRuntimes = sandboxRuntimes(32)
		},
	}, {
		name:   "empty sandbox_classes",
		mutate: func(s *ateapipb.WorkerStatus) { s.SandboxClasses = []*ateapipb.WorkerSandboxClass{} },
	}, {
		name: "too many sandbox_classes",
		mutate: func(s *ateapipb.WorkerStatus) {
			second := validSandboxClass()
			second.SandboxClass = "microvm"
			s.SandboxClasses = append(s.SandboxClasses, second)
		},
		want: field.ErrorList{field.TooMany(classesPath, 2, 1).WithOrigin("maxItems")},
	}, {
		name:   "missing sandbox_classes.sandbox_class",
		mutate: func(s *ateapipb.WorkerStatus) { s.SandboxClasses[0].SandboxClass = "" },
		want:   field.ErrorList{field.Required(classPath.Child("sandbox_class"), "")},
	}, {
		name:   "unsupported sandbox_classes.sandbox_class",
		mutate: func(s *ateapipb.WorkerStatus) { s.SandboxClasses[0].SandboxClass = "kata" },
		want:   field.ErrorList{field.NotSupported[string](classPath.Child("sandbox_class"), nil, nil)},
	}, {
		name:   "missing sandbox_classes.default_runtime",
		mutate: func(s *ateapipb.WorkerStatus) { s.SandboxClasses[0].DefaultRuntime = nil },
		want:   field.ErrorList{field.Required(classPath.Child("default_runtime"), "")},
	}, {
		name:   "missing sandbox_classes.default_runtime.version",
		mutate: func(s *ateapipb.WorkerStatus) { s.SandboxClasses[0].DefaultRuntime.Version = nil },
		want:   field.ErrorList{field.Required(classPath.Child("default_runtime", "version"), "")},
	}, {
		name: "too many default_runtime attributes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.SandboxClasses[0].DefaultRuntime.Version.Attributes = attributeEntries(33)
		},
		want: field.ErrorList{field.TooMany(classPath.Child("default_runtime", "version", "attributes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "invalid restorable_runtimes entry",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.SandboxClasses[0].RestorableRuntimes = []*ateapipb.WorkerSandboxRuntime{{Version: &ateapipb.VersionedSandboxCompat{Attributes: attributeEntries(1)}}}
		},
		want: field.ErrorList{field.Required(classPath.Child("restorable_runtimes").Index(0).Child("version", "schema_version"), "")},
	}, {
		name: "too many restorable_runtimes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.SandboxClasses[0].RestorableRuntimes = sandboxRuntimes(33)
		},
		want: field.ErrorList{field.TooMany(classPath.Child("restorable_runtimes"), 33, 32).WithOrigin("maxItems")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newVal := proto.Clone(oldVal).(*ateapipb.Worker)
			tt.mutate(newVal.Status)
			assertValidateErr(t, ValidateWorkerUpdate(context.Background(), field.NewPath("worker"), newVal, oldVal, true), tt.want)
		})
	}
}

func TestValidateRequestActorSuspendRequest(t *testing.T) {
	valid := func(mutate ...func(*ateapipb.RequestActorSuspendRequest)) *ateapipb.RequestActorSuspendRequest {
		r := &ateapipb.RequestActorSuspendRequest{
			Worker:   workerRef(apiWorkerName),
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
			ActorUid: apiOtherWorkerName,
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	tests := []struct {
		name string
		req  *ateapipb.RequestActorSuspendRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  valid(),
	}, {
		name: "missing worker",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Worker = nil }),
		want: field.ErrorList{field.Required(field.NewPath("worker"), "")},
	}, {
		name: "worker.atespace must be empty",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Worker.Atespace = "team-a" }),
		want: field.ErrorList{field.Forbidden(field.NewPath("worker", "atespace"), "")},
	}, {
		name: "missing worker.name",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Worker.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("worker", "name"), "")},
	}, {
		name: "invalid worker.name",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Worker.Name = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Actor = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		name: "missing actor.atespace",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Actor.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		name: "invalid actor.atespace",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Actor.Atespace = "../escape" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor", "atespace"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor.name",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Actor.Name = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor", "name"), "")},
	}, {
		name: "invalid actor.name",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.Actor.Name = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid",
		req:  valid(func(r *ateapipb.RequestActorSuspendRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRequestActorSuspendRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateReportActorSnapshotStateRequest(t *testing.T) {
	report := func(gen int32) *ateapipb.SnapshotReport {
		return &ateapipb.SnapshotReport{
			Uuid:            fmt.Sprintf("snap-%d", gen),
			Generation:      gen,
			Fidelity:        ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			Object:          &ateapipb.ObjectSnapshot{SnapshotUri: fmt.Sprintf("gs://bucket/actors/a/snap-%d", gen)},
			LocalCommitTime: timestamppb.Now(),
		}
	}
	valid := func(mutate ...func(*ateapipb.ReportActorSnapshotStateRequest)) *ateapipb.ReportActorSnapshotStateRequest {
		durable := report(1)
		durable.DurableCompleteTime = timestamppb.Now()
		r := &ateapipb.ReportActorSnapshotStateRequest{
			Actor:    &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
			ActorUid: apiOtherWorkerName,
			State: &ateapipb.ActorSnapshotState{
				LatestLocal:   report(2),
				Uploading:     report(2),
				LatestDurable: durable,
				UploadDueTime: timestamppb.Now(),
			},
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	local := field.NewPath("state", "latest_local")
	tests := []struct {
		name string
		req  *ateapipb.ReportActorSnapshotStateRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  valid(),
	}, {
		name: "valid detach: no local snapshot",
		req: valid(func(r *ateapipb.ReportActorSnapshotStateRequest) {
			r.State.LatestLocal = nil
			r.State.Uploading = nil
			r.State.UploadDueTime = nil
		}),
	}, {
		name: "empty state",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State = &ateapipb.ActorSnapshotState{} }),
		want: field.ErrorList{field.Required(field.NewPath("state", "latest_durable"), "")},
	}, {
		name: "valid upload in flight with no local snapshot",
		req: valid(func(r *ateapipb.ReportActorSnapshotStateRequest) {
			r.State = &ateapipb.ActorSnapshotState{Uploading: r.State.GetUploading()}
		}),
	}, {
		name: "missing actor",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.Actor = nil }),
		want: field.ErrorList{field.Required(field.NewPath("actor"), "")},
	}, {
		name: "missing actor.atespace",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.Actor.Atespace = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor", "atespace"), "")},
	}, {
		name: "invalid actor.name",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.Actor.Name = "UPPER" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor", "name"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "missing actor_uid",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.ActorUid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("actor_uid"), "")},
	}, {
		name: "invalid actor_uid",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.ActorUid = "not-a-uuid" }),
		want: field.ErrorList{field.Invalid(field.NewPath("actor_uid"), nil, "").WithOrigin("format=k8s-uuid")},
	}, {
		name: "missing state",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State = nil }),
		want: field.ErrorList{field.Required(field.NewPath("state"), "")},
	}, {
		name: "missing uuid",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Uuid = "" }),
		want: field.ErrorList{field.Required(local.Child("uuid"), "")},
	}, {
		name: "invalid uuid",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Uuid = "../escape" }),
		want: field.ErrorList{field.Invalid(local.Child("uuid"), nil, "").WithOrigin("format=k8s-short-name")},
	}, {
		name: "generation below 1",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Generation = -1 }),
		want: field.ErrorList{field.Invalid(local.Child("generation"), nil, "").WithOrigin("minimum")},
	}, {
		name: "missing fidelity",
		req: valid(func(r *ateapipb.ReportActorSnapshotStateRequest) {
			r.State.LatestLocal.Fidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
		}),
		want: field.ErrorList{field.Required(local.Child("fidelity"), "")},
	}, {
		name: "unknown fidelity",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Fidelity = 4 }),
		want: field.ErrorList{field.Invalid(local.Child("fidelity"), nil, "").WithOrigin("maximum")},
	}, {
		name: "missing object",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Object = nil }),
		want: field.ErrorList{field.Required(local.Child("object"), "")},
	}, {
		name: "missing object.snapshot_uri",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestLocal.Object.SnapshotUri = "" }),
		want: field.ErrorList{field.Required(local.Child("object", "snapshot_uri"), "")},
	}, {
		name: "valid without local_commit_time",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestDurable.LocalCommitTime = nil }),
	}, {
		name: "uploading is validated",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.Uploading.Uuid = "" }),
		want: field.ErrorList{field.Required(field.NewPath("state", "uploading", "uuid"), "")},
	}, {
		name: "latest_durable is validated",
		req:  valid(func(r *ateapipb.ReportActorSnapshotStateRequest) { r.State.LatestDurable.Object = nil }),
		want: field.ErrorList{field.Required(field.NewPath("state", "latest_durable", "object"), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateReportActorSnapshotStateRequest(context.Background(), tt.req), tt.want)
		})
	}
}

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

func workerRef(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: name}
}

// validSandboxClass returns a WorkerSandboxClass in the shape RegisterWorker
// accepts: a sandbox class and a default runtime, with nothing restorable.
func validSandboxClass() *ateapipb.WorkerSandboxClass {
	return &ateapipb.WorkerSandboxClass{
		SandboxClass:   "gvisor",
		DefaultRuntime: &ateapipb.WorkerSandboxRuntime{Version: validSandboxCompat()},
	}
}

// validSandboxCompat returns a v1 compatibility identity with one attribute.
func validSandboxCompat() *ateapipb.VersionedSandboxCompat {
	return &ateapipb.VersionedSandboxCompat{
		SchemaVersion: "v1",
		Attributes:    []*ateapipb.AttributeEntry{{Key: "architecture", Value: "amd64"}},
	}
}

// attributeEntries returns n valid attributes with distinct keys.
func attributeEntries(n int) []*ateapipb.AttributeEntry {
	attrs := make([]*ateapipb.AttributeEntry, n)
	for i := range attrs {
		attrs[i] = &ateapipb.AttributeEntry{Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	return attrs
}

// sandboxRuntimes returns n valid runtimes.
func sandboxRuntimes(n int) []*ateapipb.WorkerSandboxRuntime {
	runtimes := make([]*ateapipb.WorkerSandboxRuntime, n)
	for i := range runtimes {
		runtimes[i] = &ateapipb.WorkerSandboxRuntime{Version: validSandboxCompat()}
	}
	return runtimes
}
