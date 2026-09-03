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

// Package multitemplate installs the multi-template demo, where two
// ActorTemplates in two different atespaces share a single WorkerPool.
package multitemplate

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

func init() {
	demos.Register(&demos.Substrate{
		DemoName:           "demo-multi-template",
		Short:              "Two ActorTemplates sharing one WorkerPool",
		WorkerPoolManifest: "demos/multi-template/multi-template.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: "ate-demo-multi-template-pool", Name: "shared-pool"}},
		Templates: []demos.SubstrateTemplate{
			{
				Manifest: "demos/multi-template/counter-template.yaml.tmpl",
				Ref:      resources.ActorTemplateRef{Atespace: "ate-demo-multi-template-counter", Name: "counter"},
			},
			{
				Manifest: "demos/multi-template/fspersist-template.yaml.tmpl",
				Ref:      resources.ActorTemplateRef{Atespace: "ate-demo-multi-template-fspersist", Name: "fspersist"},
			},
		},
	})
}
