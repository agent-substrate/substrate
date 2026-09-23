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
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// Apply set default values to Substrate resource proto messages.
func Apply(m proto.Message) {
	switch v := m.(type) {
	case *ateapipb.Actor:
		applyActorDefaults(v)
	case *ateapipb.ActorTemplate:
		applyActorTemplateDefaults(v)
	case *ateapipb.Atespace:
		applyAtespaceDefaults(v)
	case *ateapipb.EgressPolicy:
		applyEgressPolicyDefaults(v)
	case *ateapipb.Tag:
		applyTagDefaults(v)
	case *ateapipb.Worker:
		applyWorkerDefaults(v)
	default:
		panic(fmt.Sprintf("unknown resource message %T", m))
	}
}

func applyActorTemplateDefaults(t *ateapipb.ActorTemplate) {
	if t == nil {
		return
	}
	applySnapshotsConfigDefaults(t.SnapshotsConfig)
	for _, c := range t.Containers {
		applyContainerDefaults(c)
	}
}

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

func applyContainerDefaults(c *ateapipb.Container) {
	const (
		defaultWakeupProbeTimeoutSeconds int32 = 30
		defaultWakeupProbePath                 = "/"
	)
	if c == nil || c.WakeupProbe == nil {
		return
	}
	if c.WakeupProbe.TimeoutSeconds == 0 {
		c.WakeupProbe.TimeoutSeconds = defaultWakeupProbeTimeoutSeconds
	}
	if hg := c.WakeupProbe.HttpGet; hg != nil && hg.Path == "" {
		hg.Path = defaultWakeupProbePath
	}
}

func applyActorDefaults(*ateapipb.Actor) {}

func applyAtespaceDefaults(*ateapipb.Atespace) {}

func applyEgressPolicyDefaults(*ateapipb.EgressPolicy) {}

func applyTagDefaults(*ateapipb.Tag) {}

func applyWorkerDefaults(*ateapipb.Worker) {}
