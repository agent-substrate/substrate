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

// Package sandboxclass names the sandbox runtime families: the values a
// WorkerPool and a SandboxConfig can select. It imports
// nothing from Kubernetes, so any package can name a class without pulling in
// the ate.dev/v1 API types.
package sandboxclass

import "slices"

const (
	// GVisor is the gVisor/runsc runtime (cmd/ateworker-gvisor). Default.
	GVisor = "gvisor"
	// MicroVM is the micro-VM runtime (cmd/ateworker-microvm). It needs /dev/kvm
	// and vhost devices.
	MicroVM = "microvm"
)

// All returns every sandbox class. Keep it in sync with the
// +kubebuilder:validation:Enum markers in api/infra/k8s/ate/v1.
func All() []string {
	return []string{GVisor, MicroVM}
}

// Valid reports whether class is one of All. The match is case-sensitive.
func Valid(class string) bool {
	return slices.Contains(All(), class)
}
