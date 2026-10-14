// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"os"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/ko"
)

// SubstrateVersion returns the build version, the same one ko stamps into the
// binaries.
//
// With --image-repo nothing is built, so `git describe` would report the
// checkout rather than the images being installed. The image tag is the
// version in that case. VERSION still wins over both.
func (e *Env) SubstrateVersion() string {
	if e.Cfg.Images.IsPrebuilt() {
		if v := os.Getenv("VERSION"); v != "" {
			return v
		}
		// The tag may carry its digest (v1@sha256:...); the version
		// is the tag alone.
		v, _, _ := strings.Cut(e.Cfg.Images.Tag, "@")
		return v
	}
	// ko.BuildVersion consults VERSION itself before `git describe`.
	return ko.BuildVersion(e.Cfg.Root)
}
