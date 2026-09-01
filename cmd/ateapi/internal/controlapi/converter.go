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

package controlapi

import (
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func actorSnapshotContentScopeToAtelet(in ateapipb.SnapshotContentScope) ateletpb.SnapshotScope {
	if in == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
	}
	return ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL
}

// effectiveContentScope normalizes a template snapshot scope for comparisons:
// UNSPECIFIED means FULL, on both the converted CRD and the stored resource.
func effectiveContentScope(in ateapipb.SnapshotContentScope) ateapipb.SnapshotContentScope {
	if in == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	return in
}

// sandboxClassString renders the proto enum in the CRD's lower-case string
// form, which the scheduler and the metric labels share.
func sandboxClassString(in ateapipb.SandboxClass) string {
	switch in {
	case ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR:
		return string(atev1alpha1.SandboxClassGvisor)
	case ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM:
		return string(atev1alpha1.SandboxClassMicroVM)
	default:
		return ""
	}
}
