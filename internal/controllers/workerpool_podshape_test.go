// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// TestBuildDeploymentPodShape asserts the micro-VM runtime adds the
// /dev/kvm device, nodeSelector, and toleration; gvisor (default) does not.
func TestBuildDeploymentPodShape(t *testing.T) {
	tests := []struct {
		name       string
		runtime    atev1alpha1.Runtime
		wantKVM    bool
		wantPlaced bool
	}{
		{name: "gvisor default", runtime: "", wantKVM: false, wantPlaced: false},
		{name: "gvisor explicit", runtime: atev1alpha1.RuntimeGvisor, wantKVM: false, wantPlaced: false},
		{name: "microvm", runtime: atev1alpha1.RuntimeMicroVM, wantKVM: true, wantPlaced: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wp := &atev1alpha1.WorkerPool{}
			wp.Name = "wp"
			wp.Namespace = "ns"
			wp.Spec.Replicas = 1
			wp.Spec.AteomImage = "example.com/ateom:latest"
			wp.Spec.Runtime = tc.runtime

			dep := buildDeploymentApplyConfig(wp)
			ps := dep.Spec.Template.Spec

			// /dev/kvm volume + mount.
			hasKVMVol := false
			for _, v := range ps.Volumes {
				if v.Name != nil && *v.Name == "dev-kvm" {
					hasKVMVol = true
					if v.HostPath == nil || v.HostPath.Path == nil || *v.HostPath.Path != "/dev/kvm" {
						t.Errorf("dev-kvm volume hostPath = %+v, want /dev/kvm", v.HostPath)
					}
					if v.HostPath.Type == nil || *v.HostPath.Type != corev1.HostPathCharDev {
						t.Errorf("dev-kvm volume type = %v, want CharDevice", v.HostPath.Type)
					}
				}
			}
			hasKVMMount := false
			for _, c := range ps.Containers {
				for _, m := range c.VolumeMounts {
					if m.MountPath != nil && *m.MountPath == "/dev/kvm" {
						hasKVMMount = true
					}
				}
			}
			if hasKVMVol != tc.wantKVM || hasKVMMount != tc.wantKVM {
				t.Errorf("kvm present vol=%v mount=%v, want %v", hasKVMVol, hasKVMMount, tc.wantKVM)
			}

			// nodeSelector + toleration on ate.dev/runtime.
			_, hasSelector := ps.NodeSelector["ate.dev/runtime"]
			hasToleration := false
			for _, tol := range ps.Tolerations {
				if tol.Key != nil && *tol.Key == "ate.dev/runtime" {
					hasToleration = true
				}
			}
			if hasSelector != tc.wantPlaced || hasToleration != tc.wantPlaced {
				t.Errorf("placement selector=%v toleration=%v, want %v", hasSelector, hasToleration, tc.wantPlaced)
			}

			// The base run-ateom hostPath mount must always be present.
			foundBase := false
			for _, v := range ps.Volumes {
				if v.Name != nil && *v.Name == "run-ateom" {
					foundBase = true
				}
			}
			if !foundBase {
				t.Error("run-ateom volume missing")
			}
		})
	}
}
