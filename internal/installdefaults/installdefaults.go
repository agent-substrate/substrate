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

// Package installdefaults holds the default namespace and Service names
// that match the canonical install layout in manifests/ate-install/.
// Binaries use these as flag defaults; deployments that diverge from
// the canonical layout pass actual values via the corresponding flags.
package installdefaults

import "os"

const (
	// SystemNamespace is the namespace where substrate's control-plane
	// components and the atelet DaemonSet run.
	SystemNamespace = "ate-system"
	// APIServiceName is the Service name of ate-api-server.
	APIServiceName = "api"
	// RouterServiceName is the Service name of atenet-router.
	RouterServiceName = "atenet-router"
	// DNSServiceName is the Service name of substrate's CoreDNS.
	DNSServiceName = "dns"

	// PodNamespaceEnv is the conventional env var name for the namespace
	// a pod is running in, exposed via Kubernetes' downward API.
	PodNamespaceEnv = "POD_NAMESPACE"
)

// NamespaceFromPodEnv returns the namespace from the PodNamespaceEnv env
// var when set (typically populated via Kubernetes' downward API), and
// falls back to SystemNamespace for non-k8s invocations (tests, local dev).
func NamespaceFromPodEnv() string {
	if ns := os.Getenv(PodNamespaceEnv); ns != "" {
		return ns
	}
	return SystemNamespace
}
