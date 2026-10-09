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

package protoshapetest

import (
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
)

func TestFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  proto.Message
		want []string
	}{
		{"empty", &ateletpb.RunResponse{}, nil},
		{"scalar and message", &ateapipb.MintAteomActorCertificateRequest{}, []string{
			"1 actor ateapi.ObjectRef",
			"2 actor_uid string",
			"3 certificate_signing_request bytes",
		}},
		{"repeated", &ateapipb.Resources{}, []string{"1 limits repeated ateapi.Limits"}},
		{"map", &ateletpb.SandboxAssets{}, []string{
			"1 sandbox_class string",
			"2 assets map<string, atelet.ArchAssets>",
			"3 pause_image string",
		}},
		{"enum and oneof", &ateletpb.CheckpointRequest{}, []string{
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, Fields(tc.msg)); diff != "" {
				t.Errorf("Fields mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFieldsOptional(t *testing.T) {
	const want = "9 egress_gateway optional atelet.EgressGateway"
	if got := Fields(&ateletpb.RunRequest{}); !slices.Contains(got, want) {
		t.Errorf("Fields(RunRequest) = %q, want it to contain %q", got, want)
	}
}
