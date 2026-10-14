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
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/protoshapetest"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestProtoShapes pins the field set of every AteomHerder request and
// response, of the parts of them the herder reads, and of the WorkerService
// request it sends. The herder is faithful only while it does with each field
// what the real atelet and ateom do: a new request field may change what a
// call costs, and a new response field may be one ate-api-server stores, which
// the herder's empty responses would leave out. When this fails, decide what
// the herder should do with the change, then update the expected fields in
// the table below.
func TestProtoShapes(t *testing.T) {
	for _, tc := range []struct {
		msg    proto.Message
		fields []string
	}{
		{&ateletpb.RunRequest{}, []string{
			"1 target_ateom_uid string",
			"2 atespace string",
			"3 actor_name string",
			"4 actor_uid string",
			"5 actor_template_atespace string",
			"6 actor_template_name string",
			"7 spec atelet.WorkloadSpec",
			"8 sandbox_assets atelet.SandboxAssets",
			"9 egress_gateway optional atelet.EgressGateway",
			"10 cpu_milli int64",
			"11 memory_bytes int64",
		}},
		{&ateletpb.RunResponse{}, nil},
		{&ateletpb.CheckpointRequest{}, []string{
			"1 target_ateom_uid string",
			"2 atespace string",
			"3 actor_name string",
			"4 actor_uid string",
			"5 actor_template_atespace string",
			"6 actor_template_name string",
			"7 spec atelet.WorkloadSpec",
			"8 type atelet.CheckpointType",
			"9 local_config atelet.LocalCheckpointConfiguration oneof config",
			"10 external_config atelet.ExternalCheckpointConfiguration oneof config",
			"11 scope atelet.SnapshotScope",
		}},
		{&ateletpb.CheckpointResponse{}, nil},
		{&ateletpb.RestoreRequest{}, []string{
			"1 target_ateom_uid string",
			"2 atespace string",
			"3 actor_name string",
			"4 actor_uid string",
			"5 actor_template_atespace string",
			"6 actor_template_name string",
			"7 spec atelet.WorkloadSpec",
			"8 type atelet.CheckpointType",
			"9 local_config atelet.LocalCheckpointConfiguration oneof config",
			"10 external_config atelet.ExternalRestoreConfiguration oneof config",
			"11 scope atelet.SnapshotScope",
			"13 egress_gateway optional atelet.EgressGateway",
			"14 cpu_milli int64",
			"15 memory_bytes int64",
			"16 sandbox_assets atelet.SandboxAssets",
		}},
		{&ateletpb.RestoreResponse{}, nil},
		{&ateletpb.UploadPausedCheckpointRequest{}, []string{
			"1 atespace string",
			"2 actor_name string",
			"3 actor_uid string",
			"4 actor_template_atespace string",
			"5 actor_template_name string",
			"6 local_snapshot_name string",
			"7 destination_snapshot_uri string",
			"8 desired_scope atelet.SnapshotScope",
		}},
		{&ateletpb.UploadPausedCheckpointResponse{}, nil},
		{&ateletpb.TerminateRequest{}, []string{
			"1 target_ateom_uid string",
			"2 atespace string",
			"3 actor_name string",
			"4 actor_uid string",
			"5 actor_template_atespace string",
			"6 actor_template_name string",
			"7 spec atelet.WorkloadSpec",
		}},
		{&ateletpb.TerminateResponse{}, nil},
		// Read by the herder: an egress gateway decides whether a Run or
		// Restore mints, and an external checkpoint's URI is where the
		// placeholder goes.
		{&ateletpb.EgressGateway{}, []string{"1 address string"}},
		{&ateletpb.ExternalCheckpointConfiguration{}, []string{"1 snapshot_uri string"}},
		// Sent by the herder.
		{&ateapipb.MintAteomActorCertificateRequest{}, []string{
			"1 actor ateapi.ObjectRef",
			"2 actor_uid string",
			"3 certificate_signing_request bytes",
		}},
	} {
		name := tc.msg.ProtoReflect().Descriptor().FullName()
		if diff := cmp.Diff(tc.fields, protoshapetest.Fields(tc.msg)); diff != "" {
			t.Errorf("%s fields changed (-expected +now):\n%s\nDecide what fake-atelet's herder should do with the change, then update the expected fields in TestProtoShapes.", name, diff)
		}
	}
}

// TestEnumValues pins the values of the request enums that decide what the
// herder writes to object storage. The herder writes a placeholder only for an
// external checkpoint, and the same placeholder for every scope; a new
// checkpoint type or scope may need one too, or a different one. When this
// fails, decide what the herder should do with the new value, then update the
// expected values in the table below.
func TestEnumValues(t *testing.T) {
	for _, tc := range []struct {
		enum   protoreflect.Enum
		values []string
	}{
		{ateletpb.CheckpointType(0), []string{
			"0 CHECKPOINT_TYPE_UNSPECIFIED",
			"1 CHECKPOINT_TYPE_LOCAL",
			"2 CHECKPOINT_TYPE_EXTERNAL",
		}},
		{ateletpb.SnapshotScope(0), []string{
			"0 SNAPSHOT_SCOPE_UNSPECIFIED",
			"1 SNAPSHOT_SCOPE_FULL",
			"2 SNAPSHOT_SCOPE_DATA",
		}},
	} {
		name := tc.enum.Descriptor().FullName()
		if diff := cmp.Diff(tc.values, enumValues(tc.enum)); diff != "" {
			t.Errorf("%s values changed (-expected +now):\n%s\nDecide what fake-atelet's herder should do with the new value, then update the expected values in TestEnumValues.", name, diff)
		}
	}
}

// enumValues returns one line per value of e's enum, in declaration order: its
// number and name, e.g. "2 CHECKPOINT_TYPE_EXTERNAL".
func enumValues(e protoreflect.Enum) []string {
	var out []string
	values := e.Descriptor().Values()
	for i := range values.Len() {
		v := values.Get(i)
		out = append(out, fmt.Sprintf("%d %s", v.Number(), v.Name()))
	}
	return out
}
