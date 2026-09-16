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

// Package egressmicrovmmitm installs the micro-VM MITM variant of the egress
// demo. It needs both what demo-egress-microvm needs (the cluster-wide
// `microvm` SandboxConfig from hack/install-microvm-deps.sh --install) and
// what demo-egress-mitm needs (an sdsmint install, for the trust bundle).
package egressmicrovmmitm

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-egress-microvm-mitm"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-egress-microvm-mitm",
		Short:              "Egress MITM inspection on micro-VM workers (needs install-microvm-deps.sh and --experimental-use-sdsmint)",
		WorkerPoolManifest: "demos/egress/egress-microvm-mitm.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "egress-microvm-mitm"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/egress/egress-microvm-mitm-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "egress-microvm-mitm"},
		}},
		GoldenTimeout: demos.MicroVMGoldenTimeout,
	})
}
