// Copyright 2026 The Agent Substrate Authors
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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
	"go.ate.dev/substrate/internal/resources"
)

// cmp options, used with protocmp.Transform, that skip the resource metadata
// ate-api assigns.
var (
	// ignoreVersion and ignoreCreateUpdateTime skip what ate-api changes on every
	// write, for comparing a resource before and after an operation.
	ignoreVersion          = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "version")
	ignoreCreateUpdateTime = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "create_time", "update_time")
	// ignoreDeleteTime skips the time ate-api stamps on a deleted resource.
	ignoreDeleteTime = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "delete_time")
	// ignoreCreatedMetadata skips what ate-api assigns when it creates a
	// resource, for comparing a new resource against a literal.
	ignoreCreatedMetadata = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time")
)

// foreignUID is a valid UID no resource has, for preconditions that must miss.
const foreignUID = "0f0e0d0c-0b0a-4908-8706-050403020100"

// callerVersion and callerTime are output-only values a caller sends, which
// ate-api must not store.
var (
	callerVersion int64 = 7
	callerTime          = timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
)

// setOutputOnlyMetadata sets the metadata fields ate-api assigns on create to
// values a caller could send.
func setOutputOnlyMetadata(m *ateapi.ResourceMetadata) {
	m.Uid = foreignUID
	m.Version = callerVersion
	m.CreateTime = callerTime
	m.UpdateTime = callerTime
	m.DeleteTime = callerTime
}

// assertAssignedMetadata checks that ate-api replaced every field
// setOutputOnlyMetadata sets.
func assertAssignedMetadata(t *testing.T, m *ateapi.ResourceMetadata) {
	t.Helper()
	if m.GetUid() == foreignUID || m.GetVersion() == callerVersion || proto.Equal(m.GetCreateTime(), callerTime) || proto.Equal(m.GetUpdateTime(), callerTime) || m.GetDeleteTime() != nil {
		t.Errorf("created metadata %v keeps output-only fields the caller sent", m)
	}
}

// mutated returns a copy of m with mutate applied.
func mutated[M proto.Message](m M, mutate func(M)) M {
	c := proto.CloneOf(m)
	mutate(c)
	return c
}

// templateResources is the size of the actors of harness templates.
func templateResources() *ateapi.Resources {
	return resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)
}

// latestSnapshot is the actor's snapshot with the highest generation.
func latestSnapshot(t *testing.T, actor *ateapi.Actor) *ateapi.Snapshot {
	t.Helper()
	var latest *ateapi.Snapshot
	for _, s := range actor.GetStatus().GetSnapshots() {
		if s.GetGeneration() >= latest.GetGeneration() {
			latest = s
		}
	}
	if latest == nil {
		t.Fatalf("actor %s has no snapshot", actor.GetMetadata().GetName())
	}
	return latest
}

// snapshotObjects are the objects the fake atelet uploads for the snapshot at
// uri.
func snapshotObjects(uri string) []string {
	prefix := strings.TrimPrefix(uri, "gs://")
	return []string{prefix + "/manifest.json", prefix + "/memory.zst"}
}

// assertRPCs checks that a fake received exactly ops since rec started, in
// order. With no ops, it checks that the fake received none.
func assertRPCs[Op ~string](t *testing.T, rec *harness.RPCRecording[Op], ops ...Op) {
	t.Helper()
	var got []Op
	for _, rpc := range rec.RPCs() {
		got = append(got, rpc.Op)
	}
	if diff := cmp.Diff(ops, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("RPCs (-want +got):\n%s", diff)
	}
}

// onlyRPC checks that a fake received exactly one RPC since rec started, with
// a request of type R, and returns that request.
func onlyRPC[R any, Op ~string](t *testing.T, rec *harness.RPCRecording[Op]) R {
	t.Helper()
	rpcs := rec.RPCs()
	if len(rpcs) == 1 {
		if req, ok := rpcs[0].Request.(R); ok {
			return req
		}
	}
	var zero R
	t.Fatalf("received %v, want exactly one %T", rpcs, zero)
	return zero
}
