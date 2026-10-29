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

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/atelet"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

const ateletManifest = "../../manifests/ate-install/atelet.yaml"

// TestAteletManifestDoesNotExposeHerderOnHostPort pins that the AteomHerder
// listener is reachable only on atelet's Pod IP. ate-api-server dials the Pod
// IP, so a hostPort would only add every node IP as a way in.
func TestAteletManifestDoesNotExposeHerderOnHostPort(t *testing.T) {
	raw, err := os.ReadFile(ateletManifest)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, doc := range bytes.Split(raw, []byte("\n---")) {
		var ds appsv1.DaemonSet
		if err := yaml.Unmarshal(doc, &ds); err != nil || ds.Kind != "DaemonSet" || !strings.HasPrefix(ds.Name, "atelet") {
			continue
		}
		for _, c := range ds.Spec.Template.Spec.Containers {
			for _, p := range c.Ports {
				if p.Name != "grpc" && p.ContainerPort != atelet.DefaultPort {
					continue
				}
				found = true
				if p.HostPort != 0 {
					t.Errorf("container %q port %q sets hostPort %d; AteomHerder must not listen on the node IP", c.Name, p.Name, p.HostPort)
				}
			}
		}
		// ate-api-server dials atelet's Pod IP; with hostNetwork that would be
		// the node IP, and removing the hostPort would protect nothing.
		if ds.Spec.Template.Spec.HostNetwork {
			t.Errorf("atelet DaemonSet uses hostNetwork, which exposes AteomHerder on the node IP")
		}
	}
	if !found {
		t.Fatal("found no atelet DaemonSet with a grpc port; the manifest changed shape and this test is checking nothing")
	}
}
