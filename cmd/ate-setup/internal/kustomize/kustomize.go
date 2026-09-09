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

// Package kustomize renders the overlays under manifests/ate-install, standing
// in for `kubectl kustomize <dir> --load-restrictor LoadRestrictionsNone`.
package kustomize

import (
	"fmt"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// Build renders an overlay directory to a multi-document YAML manifest.
//
// Load restrictions are disabled to match the shell scripts: the kind overlays
// reference base manifests that sit outside their own directory, which the
// default root-relative restriction rejects.
func Build(dir string) ([]byte, error) {
	opts := krusty.MakeDefaultOptions()
	opts.LoadRestrictions = types.LoadRestrictionsNone

	k := krusty.MakeKustomizer(opts)
	resMap, err := k.Run(filesys.MakeFsOnDisk(), dir)
	if err != nil {
		return nil, fmt.Errorf("while building the kustomization at %s: %w", dir, err)
	}

	out, err := resMap.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("while serializing the kustomization at %s: %w", dir, err)
	}
	return out, nil
}
