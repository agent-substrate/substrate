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

// Package examples holds a hand-written YAML example of each resource. The doc
// site shows each beside the resource's fields.
//
// grpc/ holds one file per gRPC resource as ate-api-server returns it, with
// server-set fields such as uid and status. Each is its message's protojson
// form as YAML, named after the message, e.g. Actor.yaml. Tests check that
// every resource has an example and that each example parses as its message,
// so a renamed or removed field fails them.
//
// k8s/ holds one file per Kubernetes resource as users apply it with kubectl,
// named after its kind, e.g. WorkerPool.yaml. Tests check each against its
// CRD's schema, so a renamed field or an invalid value fails them. They don't
// run CEL rules or check metadata.
package examples

import (
	"embed"
	"path"
	"strings"
)

//go:embed grpc/*.yaml k8s/*.yaml
var files embed.FS

// GRPCResources returns each gRPC resource example's text keyed by message
// name. For example, grpc/Actor.yaml's text under "Actor".
func GRPCResources() (map[string]string, error) {
	return byName("grpc")
}

// K8sResources returns each Kubernetes resource example's text keyed by
// kind. For example, k8s/WorkerPool.yaml's text under "WorkerPool".
func K8sResources() (map[string]string, error) {
	return byName("k8s")
}

func byName(dir string) (map[string]string, error) {
	entries, err := files.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	examples := make(map[string]string, len(entries))
	for _, e := range entries {
		text, err := files.ReadFile(path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		examples[strings.TrimSuffix(e.Name(), ".yaml")] = string(text)
	}
	return examples, nil
}
