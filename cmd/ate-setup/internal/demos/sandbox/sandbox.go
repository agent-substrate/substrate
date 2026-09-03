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

// Package sandbox installs the sandbox demo: an ActorTemplate driven on demand
// by the sandbox client rather than by a long-lived workload.
package sandbox

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// namespace is the pool's k8s namespace; it doubles as the atespace holding
// the demo's ActorTemplate.
const namespace = "ate-demo-sandbox"

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-sandbox",
		Short:              "An on-demand sandbox actor driven by the sandbox client",
		WorkerPoolManifest: "demos/sandbox/sandbox.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "sandbox-workerpool"}},
		Templates: []demos.SubstrateTemplate{{
			Manifest: "demos/sandbox/sandbox-template.yaml.tmpl",
			Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "sandbox-template"},
		}},
	})
}
