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

// Package egressmitm installs the MITM variant of the egress demo.
//
// Its actors project the egress gateway trust bundle, which only resolves on
// an sdsmint install (deploy atenet --experimental-use-sdsmint), so it cannot
// be part of what a passthrough install deploys. A golden snapshot only exists
// once an actor starts, and an actor whose trust bundle does not resolve never
// does, so a timeout waiting for the golden is the symptom of a missing
// sdsmint install.
package egressmitm

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-egress-mitm"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-egress-mitm",
		Short:              "Egress MITM inspection through atenet (needs an --experimental-use-sdsmint install)",
		WorkerPoolManifest: "demos/egress/egress-mitm.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "egress-mitm"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/egress/egress-mitm-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "egress-mitm"},
		}},
	})
}
