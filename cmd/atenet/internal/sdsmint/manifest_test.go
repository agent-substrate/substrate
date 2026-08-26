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

package sdsmint

import (
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

const sdsmintContainerName = "sdsmint"

// TestManifestKeepsTheCAOffTheDataPlane verifies that the MITM CA is only mounted
// on the `sdsmint` container in the `atenet-egress` Deployment.
func TestManifestKeepsTheCAOffTheDataPlane(t *testing.T) {
	pod := egressPodSpec(t)

	// Reached through sdsmint's own flag rather than by volume name, so
	// renaming the volume cannot quietly turn this into a test of nothing.
	caVolume := mountSupplying(t, pod, sdsmintContainerName, sdsmintCAPoolPath(t, pod)).Name

	for _, c := range allContainers(pod) {
		if c.Name == sdsmintContainerName {
			continue
		}
		for _, m := range c.VolumeMounts {
			if m.Name == caVolume {
				t.Errorf("container %q mounts the MITM CA volume %q at %s; only %s may see the signing key, and a mount that spreads gives that up without breaking anything else a test could notice",
					c.Name, caVolume, m.MountPath, sdsmintContainerName)
			}
		}
	}
}

// sdsmintCAPoolPath returns the value of the `--ca-pool-path` flag of
// the `sdsmint` container in the `atenet-egress` Deployment.
func sdsmintCAPoolPath(t *testing.T, pod *corev1.PodSpec) string {
	t.Helper()
	args := container(t, pod, sdsmintContainerName).Args
	cmd := NewSdsmintCmd()
	if len(args) == 0 || args[0] != cmd.Name() {
		t.Fatalf("the %s container's args are %v; they should invoke the %q subcommand", sdsmintContainerName, args, cmd.Name())
	}
	if err := cmd.ParseFlags(args[1:]); err != nil {
		t.Fatalf("the %s container's flags are not ones the binary accepts: %v", sdsmintContainerName, err)
	}
	path, err := cmd.Flags().GetString("ca-pool-path")
	if err != nil {
		t.Fatalf("reading --ca-pool-path from the manifest args: %v", err)
	}
	if path == "" {
		t.Fatal("the manifest passes no --ca-pool-path; sdsmint requires it")
	}
	return path
}

func egressPodSpec(t *testing.T) *corev1.PodSpec {
	t.Helper()
	const egressManifestPath = "../../../../manifests/ate-install/atenet-egress-with-sdsmint.yaml"
	raw, err := os.ReadFile(egressManifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", egressManifestPath, err)
	}
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var head struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := yaml.Unmarshal([]byte(doc), &head); err != nil {
			t.Fatalf("parsing a document of %s: %v", egressManifestPath, err)
		}
		if head.Kind != "Deployment" || head.Metadata.Name != "atenet-egress" {
			continue
		}
		var deployment appsv1.Deployment
		if err := yaml.Unmarshal([]byte(doc), &deployment); err != nil {
			t.Fatalf("decoding the atenet-egress Deployment from %s: %v", egressManifestPath, err)
		}
		return &deployment.Spec.Template.Spec
	}
	t.Fatalf("%s has no Deployment named atenet-egress", egressManifestPath)
	return nil
}

// allContainers is every container in the pod.
func allContainers(pod *corev1.PodSpec) []corev1.Container {
	return append(append([]corev1.Container{}, pod.InitContainers...), pod.Containers...)
}

func container(t *testing.T, pod *corev1.PodSpec, name string) corev1.Container {
	t.Helper()
	for _, c := range allContainers(pod) {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the egress pod has no container named %q", name)
	return corev1.Container{}
}

// mountSupplying returns the mount a container reads file through.
func mountSupplying(t *testing.T, pod *corev1.PodSpec, containerName, file string) corev1.VolumeMount {
	t.Helper()
	for _, m := range container(t, pod, containerName).VolumeMounts {
		if strings.HasPrefix(file, strings.TrimRight(m.MountPath, "/")+"/") {
			return m
		}
	}
	t.Fatalf("container %q mounts nothing that would supply %s", containerName, file)
	return corev1.VolumeMount{}
}
