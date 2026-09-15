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

// Package defaults applies each resource's field defaults in place. It is
// its own package, separate from validation, so that both the RPC handlers
// and the store layer (which needs to backfill defaults on read, without
// depending on the RPC handlers) can call it.
package defaults

import (
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ApplyActorTemplateDefaults applies the ActorTemplate defaults in place.
func ApplyActorTemplateDefaults(t *ateapipb.ActorTemplate) {
	if t == nil {
		return
	}
	applySnapshotsConfigDefaults(t.SnapshotsConfig)
	for _, c := range t.Containers {
		applyContainerDefaults(c)
	}
}

// applySnapshotsConfigDefaults fills the snapshot scopes and the resume policy.
func applySnapshotsConfigDefaults(sc *ateapipb.SnapshotsConfig) {
	if sc == nil {
		return
	}
	if sc.OnPause == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		sc.OnPause = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	if sc.OnCommit == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		sc.OnCommit = ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	if sc.OnResume == nil {
		sc.OnResume = &ateapipb.OnResumeConfig{}
	}
	if sc.OnResume.FromData == ateapipb.ResumeSource_RESUME_SOURCE_UNSPECIFIED {
		sc.OnResume.FromData = ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT
	}
}

// applyContainerDefaults fills the readiness probe's deadline and path. An
// absent probe means no readiness gate, so nothing is created for it.
func applyContainerDefaults(c *ateapipb.Container) {
	const (
		defaultReadyzTimeoutSeconds int32 = 30
		defaultReadyzPath                 = "/readyz"
	)
	if c == nil || c.Readyz == nil {
		return
	}
	if c.Readyz.TimeoutSeconds == 0 {
		c.Readyz.TimeoutSeconds = defaultReadyzTimeoutSeconds
	}
	if hg := c.Readyz.HttpGet; hg != nil && hg.Path == "" {
		hg.Path = defaultReadyzPath
	}
}

// ApplyActorDefaults applies the Actor defaults in place. An Actor has none:
// its optional fields (worker_selector, source_tag) mean something by being
// absent.
func ApplyActorDefaults(*ateapipb.Actor) {}

// ApplyAtespaceDefaults applies the Atespace defaults in place. An Atespace
// has none; it is metadata only.
func ApplyAtespaceDefaults(*ateapipb.Atespace) {}

// ApplyEgressPolicyDefaults applies the EgressPolicy defaults in place. A
// policy has none: an empty rule list means deny all, and an unset header
// prefix already is the empty string.
func ApplyEgressPolicyDefaults(*ateapipb.EgressPolicy) {}

// ApplyTagDefaults applies the Tag defaults in place. A Tag has none: scope
// and source_actor are required.
func ApplyTagDefaults(*ateapipb.Tag) {}

// ApplyWorkerDefaults applies the Worker defaults in place. A Worker has
// none: sandbox_class and labels mirror the WorkerPool as they are.
func ApplyWorkerDefaults(*ateapipb.Worker) {}
