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

package demos

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/render"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// bucketNamePlaceholder is substituted into every demo template with the
// snapshot bucket for this environment.
const bucketNamePlaceholder = "BUCKET_NAME"

// ExternalVolumePlaceholders are the optional external-volume hooks in the
// counter and autoscaled-workerpool templates. Every path except
// `deploy demo counter --with-external-volume` drops them, which removes the
// lines entirely.
var ExternalVolumePlaceholders = []string{
	"VALIDATE_EXISTING_FILE_PATH_ARG",
	"EXTERNAL_VOLUME_MOUNTS",
	"EXTERNAL_VOLUMES",
}

// Render expands a demo template with the configured bucket name and the
// build version (pool templates pin worker pods to version-labeled nodes).
func Render(e *steps.Env, relPath string, extraValues map[string]string, drop []string) ([]byte, error) {
	version, _, err := e.SubstrateVersion()
	if err != nil {
		return nil, err
	}
	values := map[string]string{
		bucketNamePlaceholder: e.Cfg.BucketName,
		"SUBSTRATE_VERSION":   version,
	}
	for k, v := range extraValues {
		values[k] = v
	}
	return render.Template(e.Cfg.Path(relPath), values, drop)
}
