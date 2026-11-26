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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// kubeAPIClients maps each ate-system workload that calls the Kubernetes API
// to the one container in it that does. Only that container may get a
// ServiceAccount token.
var kubeAPIClients = map[string]string{
	"Deployment/ate-api-server":          "ate-api-server",
	"Deployment/ate-controller":          "ate-controller",
	"DaemonSet/atelet":                   "atelet",
	"Deployment/atenet-router":           "atenet-router",
	"Deployment/k8s-credential-provider": "k8s-credential-provider",
	"Deployment/fake-workersync":         "fake-workersync",
}

// tokenDir is where client-go's in-cluster config reads the token, CA and
// namespace.
const tokenDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// automountsToken reports whether the kubelet mounts a ServiceAccount token
// into pod, following the pod field and then its ServiceAccount's.
func (in systemInstall) automountsToken(pod systemPod) bool {
	if pod.spec.AutomountServiceAccountToken != nil {
		return *pod.spec.AutomountServiceAccountToken
	}
	name := pod.spec.ServiceAccountName
	if name == "" {
		name = "default"
	}
	if sa, ok := in.serviceAccounts[name]; ok && sa.AutomountServiceAccountToken != nil {
		return *sa.AutomountServiceAccountToken
	}
	return true
}

// tokenVolumes returns pod's projected volumes that carry a ServiceAccount
// token, by name.
func tokenVolumes(pod systemPod) map[string]*corev1.ProjectedVolumeSource {
	out := map[string]*corev1.ProjectedVolumeSource{}
	for _, volume := range pod.spec.Volumes {
		if volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil {
				out[volume.Name] = volume.Projected
			}
		}
	}
	return out
}

// tokenMount returns the container's mount of one of the pod's token volumes,
// if it has one.
func tokenMount(pod systemPod, container corev1.Container) (corev1.VolumeMount, bool) {
	volumes := tokenVolumes(pod)
	for _, mount := range container.VolumeMounts {
		if volumes[mount.Name] != nil {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
}

// containerGetsToken reports whether the container can read a ServiceAccount
// token, through automount or a token volume it mounts.
func (in systemInstall) containerGetsToken(pod systemPod, container corev1.Container) bool {
	_, mounted := tokenMount(pod, container)
	return in.automountsToken(pod) || mounted
}

// projectedPaths returns the names of the files a projected volume puts in its
// mount.
func projectedPaths(projected *corev1.ProjectedVolumeSource) []string {
	var out []string
	for _, source := range projected.Sources {
		switch {
		case source.ServiceAccountToken != nil:
			out = append(out, source.ServiceAccountToken.Path)
		case source.ConfigMap != nil:
			for _, item := range source.ConfigMap.Items {
				out = append(out, item.Path)
			}
		case source.DownwardAPI != nil:
			for _, item := range source.DownwardAPI.Items {
				out = append(out, item.Path)
			}
		}
	}
	return out
}

// clientContainerName returns the name of the pod's container that calls the
// Kubernetes API, or "" if none does. The fake data plane's atelet keeps the
// atelet name but never calls the API.
func clientContainerName(pod systemPod) string {
	if pod.labels["ate.dev/fake-atelet"] == "true" {
		return ""
	}
	return kubeAPIClients[pod.workload]
}

// checkClientToken checks that the client container gets a token with what
// automount would give. Automount is complete by construction, so only a
// hand-mounted token needs its path and files checked.
func checkClientToken(t *testing.T, install systemInstall, pod systemPod, client corev1.Container) {
	t.Helper()
	if install.automountsToken(pod) {
		return
	}
	mount, ok := tokenMount(pod, client)
	if !ok {
		t.Errorf("%s: container %s calls the Kubernetes API but gets no ServiceAccount token; mount one at %s", pod.workload, client.Name, tokenDir)
		return
	}
	if mount.MountPath != tokenDir {
		t.Errorf("%s: container %s mounts its ServiceAccount token at %s, want %s", pod.workload, client.Name, mount.MountPath, tokenDir)
	}
	files := projectedPaths(tokenVolumes(pod)[mount.Name])
	for _, want := range []string{corev1.ServiceAccountTokenKey, corev1.ServiceAccountRootCAKey, corev1.ServiceAccountNamespaceKey} {
		if !slices.Contains(files, want) {
			t.Errorf("%s: token volume %s has no %s file, which client-go needs", pod.workload, mount.Name, want)
		}
	}
}

func TestSystemPodsMountTokenOnlyForKubeAPIClients(t *testing.T) {
	clientsFound := map[string]bool{}
	for _, cfg := range installConfigs() {
		t.Run(installName(cfg), func(t *testing.T) {
			install := loadSystemInstall(t, cfg)
			for _, pod := range install.pods {
				clientName := clientContainerName(pod)
				for _, container := range slices.Concat(pod.spec.InitContainers, pod.spec.Containers) {
					if container.Name == clientName {
						clientsFound[pod.workload] = true
						checkClientToken(t, install, pod, container)
						continue
					}
					if install.containerGetsToken(pod, container) {
						t.Errorf("%s: container %s gets a ServiceAccount token but does not call the Kubernetes API; set automountServiceAccountToken: false and mount the token only into the client container", pod.workload, container.Name)
					}
				}
			}
		})
	}
	for workload, clientName := range kubeAPIClients {
		if !clientsFound[workload] {
			t.Errorf("kubeAPIClients lists container %s of %s, which no install config deploys in ate-system; fix or remove it", clientName, workload)
		}
	}
}
