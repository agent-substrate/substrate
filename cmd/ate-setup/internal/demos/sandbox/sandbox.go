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
)

const namespace = "ate-demo-sandbox"

func init() {
	demos.Register(&demos.Simple{
		DemoName:       "demo-sandbox",
		Short:          "An on-demand sandbox actor driven by the sandbox client",
		Template:       "demos/sandbox/sandbox.yaml.tmpl",
		ActorTemplates: []steps.TemplateRef{{Namespace: namespace, Name: "sandbox-template"}},
		// There is no workload to come up, and the template is exercised on
		// demand, so the install does not block on readiness.
		SkipReadinessWait: true,
	})
}
