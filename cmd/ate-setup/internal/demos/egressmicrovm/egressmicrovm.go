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

// Package egressmicrovm installs the micro-VM variant of the egress demo.
//
// It needs the cluster-wide `microvm` SandboxConfig that
// hack/install-microvm-deps.sh --install creates. It is a demo of its own
// rather than a flag on demo-egress so that it appears in help and in the
// `delete all` sweep, and so that both can be installed side by side: the
// networking e2e suite runs against whichever the sandbox class under test
// selects.
package egressmicrovm

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-egress-microvm"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-egress-microvm",
		Short:              "Egress policy enforcement on micro-VM workers (needs hack/install-microvm-deps.sh --install)",
		WorkerPoolManifest: "demos/egress/egress-microvm.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "egress-microvm"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/egress/egress-microvm-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "egress-microvm"},
		}},
		GoldenTimeout: demos.MicroVMGoldenTimeout,
	})
}
