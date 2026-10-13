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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
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
		name: "valid dual-stack ips",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ips = []string{"fd00::1", "10.1.2.3"} })),
	}, {
		name: "missing ips",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ips = nil })),
		want: field.ErrorList{field.Required(field.NewPath("worker", "ips"), "")},
	}, {
		name: "invalid ip",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ips = []string{"not-an-ip"} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "ips").Index(0), nil, "").WithOrigin("format=ip-strict")},
	}, {
		name: "two ips in one family",
		req:  validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) { w.Ips = []string{"10.1.2.3", "10.1.2.4"} })),
		want: field.ErrorList{field.Invalid(field.NewPath("worker", "ips").Index(1), nil, "")},
	}, {
		name: "too many ips",
		req: validReq(validWorker(apiWorkerName, func(w *ateapipb.Worker) {
			w.Ips = []string{"10.1.2.3", "fd00::1", "10.1.2.4"}
		})),
		want: field.ErrorList{field.TooMany(field.NewPath("worker", "ips"), 3, 2).WithOrigin("maxItems")},
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

func TestValidateRegisterWorkerRequest(t *testing.T) {
	validRuntime := validSandboxRuntime
	valid := func(mutate ...func(*ateapipb.RegisterWorkerRequest)) *ateapipb.RegisterWorkerRequest {
		r := &ateapipb.RegisterWorkerRequest{
			Worker: workerRef(apiWorkerName),
			Capacity: &ateapipb.WorkerResources{
				Actors: 10,
				Resources: &ateapipb.Resources{
					Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "4Gi"}},
				},
			},
			DefaultRuntime: validRuntime(),
		}
		for _, m := range mutate {
			m(r)
		}
		return r
	}
	defaultRuntimePath := field.NewPath("default_runtime")
	restorablePath := field.NewPath("restorable_runtimes")
	tests := []struct {
		name string
		req  *ateapipb.RegisterWorkerRequest
		want field.ErrorList
	}{{
		name: "valid",
		req:  valid(),
	}, {
		name: "valid with restorable runtimes",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.RestorableRuntimes = []*ateapipb.SandboxRuntime{{
				SandboxClass: "gvisor",
				Version: &ateapipb.VersionedSandboxCompat{
					SchemaVersion: "v1",
					Attributes: []*ateapipb.AttributeEntry{
						{Key: "architecture", Value: "amd64"},
					},
				},
			}}
		}),
	}, {
		name: "valid empty capacity",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity = &ateapipb.WorkerResources{} }),
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
		name: "missing capacity",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity = nil }),
		want: field.ErrorList{field.Required(field.NewPath("capacity"), "")},
	}, {
		name: "negative capacity.actors",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.Capacity.Actors = -1 }),
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "actors"), nil, "").WithOrigin("minimum")},
	}, {
		name: "invalid limit quantity",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.Capacity.Resources.Limits = []*ateapipb.Limits{{Name: "cpu", Quantity: "lots"}}
		}),
		want: field.ErrorList{field.Invalid(field.NewPath("capacity", "resources", "limits").Index(0).Child("quantity"), nil, "")},
	}, {
		name: "missing default_runtime",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime = nil }),
		want: field.ErrorList{field.Required(defaultRuntimePath, "")},
	}, {
		name: "missing default_runtime.sandbox_class",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime.SandboxClass = "" }),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("sandbox_class"), "")},
	}, {
		name: "default_runtime.sandbox_class too long",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime.SandboxClass = strings.Repeat("c", 64) }),
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("sandbox_class"), "", 63).WithOrigin("maxLength")},
	}, {
		name: "missing default_runtime.version",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime.Version = nil }),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version"), "")},
	}, {
		name: "missing default_runtime.version.schema_version",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime.Version.SchemaVersion = "" }),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "schema_version"), "")},
	}, {
		name: "default_runtime.version.schema_version too long",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.SchemaVersion = strings.Repeat("v", 65)
		}),
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "schema_version"), "", 64).WithOrigin("maxLength")},
	}, {
		name: "missing default_runtime.version.attributes",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.DefaultRuntime.Version.Attributes = nil }),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes"), "")},
	}, {
		name: "duplicate attribute key",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{
				{Key: "architecture", Value: "amd64"},
				{Key: "architecture", Value: "arm64"},
			}
		}),
		want: field.ErrorList{field.Duplicate(defaultRuntimePath.Child("version", "attributes").Index(1), nil)},
	}, {
		name: "missing attribute key",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Value: "amd64"}}
		}),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes").Index(0).Child("key"), "")},
	}, {
		name: "attribute key too long",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: strings.Repeat("k", 129), Value: "v"}}
		}),
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "attributes").Index(0).Child("key"), "", 128).WithOrigin("maxLength")},
	}, {
		name: "missing attribute value",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: "k"}}
		}),
		want: field.ErrorList{field.Required(defaultRuntimePath.Child("version", "attributes").Index(0).Child("value"), "")},
	}, {
		name: "attribute value too long",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = []*ateapipb.AttributeEntry{{Key: "k", Value: strings.Repeat("v", 257)}}
		}),
		want: field.ErrorList{field.TooLong(defaultRuntimePath.Child("version", "attributes").Index(0).Child("value"), "", 256).WithOrigin("maxLength")},
	}, {
		name: "attributes at the bound",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = attributeEntries(32)
		}),
	}, {
		name: "too many attributes",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.DefaultRuntime.Version.Attributes = attributeEntries(33)
		}),
		want: field.ErrorList{field.TooMany(defaultRuntimePath.Child("version", "attributes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "nil restorable_runtimes entry",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.RestorableRuntimes = []*ateapipb.SandboxRuntime{nil} }),
		want: field.ErrorList{field.Required(restorablePath.Index(0), "")},
	}, {
		name: "invalid restorable_runtimes entry",
		req: valid(func(r *ateapipb.RegisterWorkerRequest) {
			r.RestorableRuntimes = []*ateapipb.SandboxRuntime{{SandboxClass: "gvisor"}}
		}),
		want: field.ErrorList{field.Required(restorablePath.Index(0).Child("version"), "")},
	}, {
		name: "restorable_runtimes at the bound",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.RestorableRuntimes = sandboxRuntimes(32) }),
	}, {
		name: "too many restorable_runtimes",
		req:  valid(func(r *ateapipb.RegisterWorkerRequest) { r.RestorableRuntimes = sandboxRuntimes(33) }),
		want: field.ErrorList{field.TooMany(restorablePath, 33, 32).WithOrigin("maxItems")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateRegisterWorkerRequest(context.Background(), tt.req), tt.want)
		})
	}
}

// TestValidateWorkerUpdate_SandboxRuntimes pins the status-side checks on the
// runtimes RegisterWorker records. They reach the store as status, which no
// request validator sees, so ValidateWorkerUpdate is the only check on them.
func TestValidateWorkerUpdate_SandboxRuntimes(t *testing.T) {
	statusPath := field.NewPath("worker", "status")
	oldVal := validWorker(apiWorkerName, withWorkerStatus())
	tests := []struct {
		name   string
		mutate func(s *ateapipb.WorkerStatus)
		want   field.ErrorList
	}{{
		name: "valid runtimes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.DefaultRuntime = validSandboxRuntime()
			s.RestorableRuntimes = sandboxRuntimes(32)
		},
	}, {
		name: "missing default_runtime.sandbox_class",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.DefaultRuntime = validSandboxRuntime()
			s.DefaultRuntime.SandboxClass = ""
		},
		want: field.ErrorList{field.Required(statusPath.Child("default_runtime", "sandbox_class"), "")},
	}, {
		name: "missing default_runtime.version",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.DefaultRuntime = &ateapipb.SandboxRuntime{SandboxClass: "gvisor"}
		},
		want: field.ErrorList{field.Required(statusPath.Child("default_runtime", "version"), "")},
	}, {
		name: "too many default_runtime attributes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.DefaultRuntime = validSandboxRuntime()
			s.DefaultRuntime.Version.Attributes = attributeEntries(33)
		},
		want: field.ErrorList{field.TooMany(statusPath.Child("default_runtime", "version", "attributes"), 33, 32).WithOrigin("maxItems")},
	}, {
		name: "invalid restorable_runtimes entry",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.RestorableRuntimes = []*ateapipb.SandboxRuntime{{SandboxClass: "gvisor"}}
		},
		want: field.ErrorList{field.Required(statusPath.Child("restorable_runtimes").Index(0).Child("version"), "")},
	}, {
		name: "too many restorable_runtimes",
		mutate: func(s *ateapipb.WorkerStatus) {
			s.RestorableRuntimes = sandboxRuntimes(33)
		},
		want: field.ErrorList{field.TooMany(statusPath.Child("restorable_runtimes"), 33, 32).WithOrigin("maxItems")},
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
		Ips:             []string{"10.1.2.3"},
		SandboxClass:    "gvisor",
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

// validSandboxRuntime returns a SandboxRuntime in the shape RegisterWorker
// accepts: a class and a v1 identity with one attribute.
func validSandboxRuntime() *ateapipb.SandboxRuntime {
	return &ateapipb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version: &ateapipb.VersionedSandboxCompat{
			SchemaVersion: "v1",
			Attributes:    []*ateapipb.AttributeEntry{{Key: "architecture", Value: "amd64"}},
		},
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
func sandboxRuntimes(n int) []*ateapipb.SandboxRuntime {
	rts := make([]*ateapipb.SandboxRuntime, n)
	for i := range rts {
		rts[i] = validSandboxRuntime()
	}
	return rts
}
