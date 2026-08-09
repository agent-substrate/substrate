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

// Package templateversion holds ActorTemplateVersion helpers shared between
// the ateapi server and kubectl-ate, which must default a spec identically:
// the server before validation and storage, the CLI before comparing a
// manifest against the stored (defaulted) spec on re-apply.
package templateversion

import (
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	defaultReadyzTimeout = 30
	defaultReadyzPath    = "/readyz"
)

// DefaultSpec materializes server-applied defaults into the spec, mirroring
// the CRD's API-server defaulting: a readyz timeout of 0 means 30s and an
// empty readyz path means "/readyz", so the effective values are visible on
// the stored object.
func DefaultSpec(spec *ateapipb.ActorTemplateVersionSpec) {
	for _, c := range spec.GetContainers() {
		readyz := c.GetReadyz()
		if readyz == nil {
			continue
		}
		if readyz.GetTimeoutSeconds() == 0 {
			readyz.TimeoutSeconds = defaultReadyzTimeout
		}
		if httpGet := readyz.GetHttpGet(); httpGet != nil && httpGet.GetPath() == "" {
			httpGet.Path = defaultReadyzPath
		}
	}
}
