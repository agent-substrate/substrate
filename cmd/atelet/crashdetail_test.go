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

package main

import (
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"
)

// TestCrashReasonMirrorsTheAPI holds the two enums together.
//
// The worker-facing proto does not import the public API, so crashDetailToAPI
// converts a reason by its number. That is only sound while the two name the
// same value for every number, which this asserts in both directions.
func TestCrashReasonMirrorsTheAPI(t *testing.T) {
	worker := ateletpb.CrashReason_name
	api := ateapi.CrashReason_name

	for number, name := range worker {
		apiName, ok := api[number]
		if !ok {
			t.Errorf("ateletpb.CrashReason has %d (%s), substrate.ateapi.v1.CrashReason does not", number, name)
			continue
		}
		if apiName != name {
			t.Errorf("reason %d is %s in ateletpb and %s in the API", number, name, apiName)
		}
	}
	for number, name := range api {
		if _, ok := worker[number]; !ok {
			t.Errorf("substrate.ateapi.v1.CrashReason has %d (%s), ateletpb.CrashReason does not", number, name)
		}
	}
}

func TestCrashDetailToAPI(t *testing.T) {
	tests := []struct {
		name string
		in   *ateletpb.CrashDetail
		want *ateapi.CrashDetail
	}{
		{
			name: "a worker that does not classify the crash reports nothing",
			in:   nil,
			want: nil,
		},
		{
			name: "every field crosses the hop",
			in: &ateletpb.CrashDetail{
				Reason:         ateletpb.CrashReason_CRASH_REASON_CONTAINER_EXITED,
				ContainerName:  "agent",
				ExitCode:       139,
				Signal:         11,
				LastStderrLine: "segmentation fault",
			},
			want: &ateapi.CrashDetail{
				Reason:         ateapi.CrashReason_CRASH_REASON_CONTAINER_EXITED,
				ContainerName:  "agent",
				ExitCode:       139,
				Signal:         11,
				LastStderrLine: "segmentation fault",
			},
		},
		{
			name: "an unknown exit code stays -1 rather than reading as success",
			in: &ateletpb.CrashDetail{
				Reason:   ateletpb.CrashReason_CRASH_REASON_CONTAINER_EXITED,
				ExitCode: -1,
			},
			want: &ateapi.CrashDetail{
				Reason:   ateapi.CrashReason_CRASH_REASON_CONTAINER_EXITED,
				ExitCode: -1,
			},
		},
		{
			// A newer worker on an older atelet. Recording the number raw
			// would put a value on the actor that nothing here can name.
			name: "a reason this build does not know reads as unspecified",
			in:   &ateletpb.CrashDetail{Reason: ateletpb.CrashReason(9999), ContainerName: "agent"},
			want: &ateapi.CrashDetail{
				Reason:        ateapi.CrashReason_CRASH_REASON_UNSPECIFIED,
				ContainerName: "agent",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := crashDetailToAPI(tt.in)
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("crashDetailToAPI() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
