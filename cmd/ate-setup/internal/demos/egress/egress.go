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

// Package egress installs the egress demo, which exercises egress policy
// enforcement through atenet.
package egress

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-egress"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-egress",
		Short:              "Egress policy enforcement through atenet",
		WorkerPoolManifest: "demos/egress/egress.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "egress"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/egress/egress-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "egress"},
		}},
	})
}
