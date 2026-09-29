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

package steps

import (
	"bytes"
	"fmt"
	"strings"

	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"
)

// substrateImagePrefix marks a container as running a substrate binary. Every
// such binary accepts --preview.
const substrateImagePrefix = "ko://github.com/agent-substrate/substrate/cmd/"

const previewFlag = "--preview"

// containerListPaths are where a workload keeps its containers.
var containerListPaths = [][]string{
	{"spec", "containers"},
	{"spec", "initContainers"},
	{"spec", "template", "spec", "containers"},
	{"spec", "template", "spec", "initContainers"},
	{"spec", "jobTemplate", "spec", "template", "spec", "containers"},
	{"spec", "jobTemplate", "spec", "template", "spec", "initContainers"},
}

// previewArg is the --preview argument every substrate component gets.
func previewArg(enabled []string) string {
	if len(enabled) > 0 {
		return previewFlag + "=" + strings.Join(enabled, ",")
	}
	return previewFlag + "="
}

// injectPreviewArg sets --preview on every substrate container in manifest,
// replacing any --preview it already carries. It must run before image
// resolution, which rewrites the ko:// references it keys on.
func injectPreviewArg(manifest []byte, enabled []string) ([]byte, error) {
	if !bytes.Contains(manifest, []byte(substrateImagePrefix)) {
		return manifest, nil
	}
	nodes, err := (&kio.ByteReader{
		Reader:                bytes.NewReader(manifest),
		OmitReaderAnnotations: true,
	}).Read()
	if err != nil {
		return nil, fmt.Errorf("while parsing manifest: %w", err)
	}
	arg := previewArg(enabled)
	for _, node := range nodes {
		for _, path := range containerListPaths {
			containers, err := node.Pipe(yaml.Lookup(path...))
			if err != nil {
				return nil, err
			}
			if containers == nil {
				continue
			}
			elements, err := containers.Elements()
			if err != nil {
				return nil, err
			}
			for _, container := range elements {
				if err := setPreviewArg(container, arg); err != nil {
					return nil, err
				}
			}
		}
	}
	var out bytes.Buffer
	if err := (kio.ByteWriter{Writer: &out}).Write(nodes); err != nil {
		return nil, fmt.Errorf("while writing manifest: %w", err)
	}
	return out.Bytes(), nil
}

func setPreviewArg(container *yaml.RNode, arg string) error {
	image, err := container.GetString("image")
	if err != nil || !strings.HasPrefix(image, substrateImagePrefix) {
		return nil
	}
	args, err := container.Pipe(yaml.LookupCreate(yaml.SequenceNode, "args"))
	if err != nil {
		return err
	}
	kept := args.YNode().Content[:0]
	for _, item := range args.YNode().Content {
		if strings.HasPrefix(item.Value, previewFlag+"=") {
			continue
		}
		kept = append(kept, item)
	}
	args.YNode().Content = append(kept, yaml.NewStringRNode(arg).YNode())
	return nil
}
