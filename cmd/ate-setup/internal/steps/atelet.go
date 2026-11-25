// Copyright 2026 The Agent Substrate Authors
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

package steps

import (
	"bytes"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// addAteletTolerations appends tolerations to those the atelet DaemonSet in
// manifest already carries. A manifest without the DaemonSet is returned
// unchanged.
func addAteletTolerations(manifest []byte, tolerations []corev1.Toleration) ([]byte, error) {
	if len(tolerations) == 0 {
		return manifest, nil
	}
	nodes, err := (&kio.ByteReader{
		Reader:                bytes.NewReader(manifest),
		OmitReaderAnnotations: true,
	}).Read()
	if err != nil {
		return nil, fmt.Errorf("while parsing manifest: %w", err)
	}
	found := false
	for _, node := range nodes {
		if node.GetKind() != "DaemonSet" || node.GetName() != "atelet" {
			continue
		}
		found = true
		list, err := node.Pipe(yaml.LookupCreate(yaml.SequenceNode, "spec", "template", "spec", "tolerations"))
		if err != nil {
			return nil, err
		}
		for _, t := range tolerations {
			raw, err := json.Marshal(t)
			if err != nil {
				return nil, err
			}
			item, err := yaml.ConvertJSONToYamlNode(string(raw))
			if err != nil {
				return nil, err
			}
			if err := list.PipeE(yaml.Append(item.YNode())); err != nil {
				return nil, err
			}
		}
	}
	if !found {
		return manifest, nil
	}
	var out bytes.Buffer
	if err := (kio.ByteWriter{Writer: &out}).Write(nodes); err != nil {
		return nil, fmt.Errorf("while writing manifest: %w", err)
	}
	return out.Bytes(), nil
}
