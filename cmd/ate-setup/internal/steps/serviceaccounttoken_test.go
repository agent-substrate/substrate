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

import "testing"

// kubeAPIClients are the ate-system workloads that call the Kubernetes API, and
// so may get a ServiceAccount token. No other workload may.
var kubeAPIClients = map[string]bool{
	"Deployment/ate-api-server":            true,
	"Deployment/ate-controller":            true,
	"DaemonSet/atelet":                     true,
	"Deployment/atenet-router":             true,
	"Deployment/podcertificate-controller": true,
	"Deployment/k8s-credential-provider":   true,
	"Deployment/fake-workersync":           true,
}

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

func TestSystemPodsMountTokenOnlyForKubeAPIClients(t *testing.T) {
	for _, cfg := range installConfigs() {
		t.Run(installName(cfg), func(t *testing.T) {
			install := loadSystemInstall(t, cfg)
			for _, pod := range install.pods {
				if install.automountsToken(pod) && !kubeAPIClients[pod.workload] {
					t.Errorf("%s automounts a ServiceAccount token but does not call the Kubernetes API; set automountServiceAccountToken: false", pod.workload)
				}
			}
		})
	}
}
