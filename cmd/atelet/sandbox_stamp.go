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

package main

import (
	"cmp"
	"fmt"
	"slices"

	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/protobuf/encoding/protojson"
)

// recordedRuntime is an ActorSandboxRuntime in a sandbox record or snapshot
// manifest, in protojson form. Fields it does not know are dropped when read,
// as the skew policy requires of manifest fields.
type recordedRuntime struct {
	*ateletpb.ActorSandboxRuntime
}

func (r recordedRuntime) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(r.ActorSandboxRuntime)
}

func (r *recordedRuntime) UnmarshalJSON(data []byte) error {
	r.ActorSandboxRuntime = &ateletpb.ActorSandboxRuntime{}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, r.ActorSandboxRuntime)
}

// hostRuntime returns rt, a runtime a Run or Restore names, with only the
// host attributes atelet writes. The worker's attributes describe the worker
// binary, so a checkpoint takes the worker's attributes from the worker
// instead.
func hostRuntime(rt *ateletpb.ActorSandboxRuntime) *recordedRuntime {
	out := &ateletpb.ActorSandboxRuntime{
		SandboxClass: rt.GetSandboxClass(),
		Version:      &ateletpb.VersionedSandboxCompat{SchemaVersion: rt.GetVersion().GetSchemaVersion()},
	}
	for _, a := range rt.GetVersion().GetAttributes() {
		if hardware.IsHostKey(a.GetKey()) {
			out.Version.Attributes = append(out.Version.Attributes, &ateletpb.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()})
		}
	}
	return &recordedRuntime{out}
}

// checkRegisteredAttributes rejects an attribute of a worker's runtime that
// is neither a host key without rules, as atelet writes them, nor a key the
// worker owns.
func checkRegisteredAttributes(compat *ateletpb.VersionedSandboxCompat) error {
	for _, a := range compat.GetAttributes() {
		switch {
		case hardware.IsWorkerKey(a.GetKey()):
		case hardware.IsHostKey(a.GetKey()) && len(a.GetCompatibleValues()) == 0:
		case hardware.IsHostKey(a.GetKey()):
			return fmt.Errorf("attribute %q is a host key and takes no compatible_values", a.GetKey())
		default:
			return fmt.Errorf("attribute %q is not a host key, and a worker's keys start with %q", a.GetKey(), hardware.WorkerKeyPrefix)
		}
	}
	return nil
}

// checkpointStamp returns the stamp of a snapshot: host, the host attributes
// of the runtime the actor ran under, plus attrs, the worker's attributes for
// the checkpoint, sorted by key. It rejects an attribute of attrs that is not
// a key the worker owns.
func checkpointStamp(host *recordedRuntime, attrs []*ateworkerpb.AttributeEntry) (*ateletpb.ActorSandboxRuntime, error) {
	for _, a := range attrs {
		if !hardware.IsWorkerKey(a.GetKey()) {
			return nil, fmt.Errorf("worker attribute %q does not start with %q", a.GetKey(), hardware.WorkerKeyPrefix)
		}
	}
	stamp := &ateletpb.ActorSandboxRuntime{
		SandboxClass: host.GetSandboxClass(),
		Version:      &ateletpb.VersionedSandboxCompat{SchemaVersion: host.GetVersion().GetSchemaVersion()},
	}
	for _, a := range host.GetVersion().GetAttributes() {
		stamp.Version.Attributes = append(stamp.Version.Attributes, &ateletpb.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()})
	}
	for _, a := range attrs {
		entry := &ateletpb.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()}
		for _, c := range a.GetCompatibleValues() {
			entry.CompatibleValues = append(entry.CompatibleValues, &ateletpb.CompatibleValue{Value: c.GetValue(), Fidelity: ateletpb.CompatibleFidelity(c.GetFidelity())})
		}
		stamp.Version.Attributes = append(stamp.Version.Attributes, entry)
	}
	slices.SortFunc(stamp.Version.Attributes, func(a, b *ateletpb.AttributeEntry) int { return cmp.Compare(a.GetKey(), b.GetKey()) })
	return stamp, nil
}
