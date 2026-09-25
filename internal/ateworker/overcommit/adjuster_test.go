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

package overcommit

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

func TestToAteletResources(t *testing.T) {
	if got := toAteletResources(nil); got != nil {
		t.Errorf("toAteletResources(nil) = %v, want nil", got)
	}
	in := &ateapi.Resources{CpuMillicores: 250, MemoryBytes: 1 << 30}
	want := &ateletpb.Resources{CpuMillicores: 250, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(want, toAteletResources(in), protocmp.Transform()); diff != "" {
		t.Errorf("toAteletResources() mismatch (-want +got):\n%s", diff)
	}
}
