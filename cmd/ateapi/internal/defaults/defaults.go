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
	case *ateapipb.AccessPolicy:
		applyAccessPolicyDefaults(v)
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
	applySnapshotConfigDefaults(t.SnapshotConfig)
	for _, c := range t.Containers {
		applyContainerDefaults(c)
	}
}

func applySnapshotConfigDefaults(sc *ateapipb.SnapshotConfig) {
	if sc == nil {
		return
	}
	if sc.PreferredFidelity == ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
		sc.PreferredFidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	}
	if sc.GoldenSnapshotConfig == nil {
		sc.GoldenSnapshotConfig = &ateapipb.GoldenSnapshotConfig{}
	}
	if sc.GoldenSnapshotConfig.Mode == ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_UNSPECIFIED {
		sc.GoldenSnapshotConfig.Mode = ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED
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

func applyAccessPolicyDefaults(*ateapipb.AccessPolicy) {}

func applyActorDefaults(*ateapipb.Actor) {}

func applyAtespaceDefaults(*ateapipb.Atespace) {}

// applyEgressPolicyDefaults fills the port of an http or https rule that
// names none: the protocol's well-known port. A tls_passthrough rule has no
// default; validation requires its ports.
func applyEgressPolicyDefaults(p *ateapipb.EgressPolicy) {
	if p == nil {
		return
	}
	for _, r := range p.Rules {
		switch {
		case r.GetHttp() != nil && r.Http.Ports == nil:
			r.Http.Ports = &ateapipb.Ports{Numbers: []int32{80}}
		case r.GetHttps() != nil && r.Https.Ports == nil:
			r.Https.Ports = &ateapipb.Ports{Numbers: []int32{443}}
		}
	}
}

func applyTagDefaults(*ateapipb.Tag) {}

func applyWorkerDefaults(*ateapipb.Worker) {}
