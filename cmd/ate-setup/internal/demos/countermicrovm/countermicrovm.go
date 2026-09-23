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

// Package countermicrovm installs the micro-VM variant of the counter demo.
//
// It needs the cluster-wide `microvm` SandboxConfig that
// hack/install-microvm-deps.sh --install creates. It has no external-volume
// option: the CSI path is covered by demo-counter.
package countermicrovm

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-counter-microvm"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-counter-microvm",
		Short:              "The counter demo on micro-VM workers (needs hack/install-microvm-deps.sh --install)",
		WorkerPoolManifest: "demos/counter/counter-microvm.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "counter-microvm"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/counter/counter-microvm-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "counter-microvm"},
		}},
		GoldenTimeout: demos.MicroVMGoldenTimeout,
	})
}
