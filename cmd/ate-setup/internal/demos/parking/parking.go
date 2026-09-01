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

// Package parking installs the parking demo, which parks and unparks actors on
// a small WorkerPool.
package parking

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

const namespace = "ate-demo-parking"

func init() {
	demos.Register(&demos.Simple{
		DemoName:       "demo-parking",
		Short:          "Actor parking and unparking on a small WorkerPool",
		Template:       "demos/parking/parking.yaml.tmpl",
		Deployments:    []steps.TemplateRef{{Atespace: namespace, Name: "parking"}},
		ActorTemplates: []steps.TemplateRef{{Atespace: namespace, Name: "parking"}},
	})
}
