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

package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agent-substrate/substrate/internal/deviceplugin"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestMicroVMHypervisorResources(t *testing.T) {
	one := resource.MustParse("1")
	for _, tc := range []struct {
		name             string
		requests, limits corev1.ResourceList
		arch             string
		want             string
	}{
		{name: "default", want: deviceplugin.ResourceKVM},
		{name: "explicit kvm", limits: corev1.ResourceList{deviceplugin.ResourceKVM: one}, want: deviceplugin.ResourceKVM},
		{name: "mshv", limits: corev1.ResourceList{deviceplugin.ResourceMSHV: one}, want: deviceplugin.ResourceMSHV},
		{name: "request only", requests: corev1.ResourceList{deviceplugin.ResourceMSHV: one}, want: deviceplugin.ResourceMSHV},
		{name: "both", limits: corev1.ResourceList{deviceplugin.ResourceMSHV: one, deviceplugin.ResourceKVM: one}},
		{name: "split conflict", requests: corev1.ResourceList{deviceplugin.ResourceMSHV: one}, limits: corev1.ResourceList{deviceplugin.ResourceKVM: one}},
		{name: "zero", limits: corev1.ResourceList{deviceplugin.ResourceMSHV: resource.MustParse("0")}},
		{name: "two", limits: corev1.ResourceList{deviceplugin.ResourceMSHV: resource.MustParse("2")}},
		{name: "fraction", requests: corev1.ResourceList{deviceplugin.ResourceMSHV: resource.MustParse("500m")}},
		{name: "arm64", arch: "arm64", limits: corev1.ResourceList{deviceplugin.ResourceMSHV: one}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := &atev1alpha1.WorkerPoolPodTemplate{Resources: &corev1.ResourceRequirements{Requests: tc.requests, Limits: tc.limits}, NodeSelector: map[string]string{"pool": "test"}}
			if tc.arch != "" {
				tmpl.NodeSelector[corev1.LabelArchStable] = tc.arch
			}
			if tmpl.Resources.Limits == nil {
				tmpl.Resources.Limits = corev1.ResourceList{}
			}
			tmpl.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("2Gi")
			wp := testWorkerPoolApplyConfig(tmpl)
			wp.Spec.SandboxClass = atev1alpha1.SandboxClassMicroVM
			err := validateMicroVMResources(wp)
			if (err != nil) != (tc.want == "") {
				t.Fatalf("validation = %v, want resource %q", err, tc.want)
			}
			if err != nil {
				return
			}
			ps := buildDeploymentApplyConfig(wp, ateomOTelSettings{}, installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, installdefaults.RouterServiceAccount).Spec.Template.Spec
			c := ps.Containers[0]
			for _, name := range []string{deviceplugin.ResourceKVM, deviceplugin.ResourceMSHV} {
				qty, ok := deviceLimit(c, name)
				if ok != (name == tc.want) || (ok && qty != "1") {
					t.Fatalf("resource %s: %s/%v, selected %s", name, qty, ok, tc.want)
				}
			}
			if qty, _ := deviceLimit(c, "memory"); qty != "2Gi" {
				t.Fatalf("memory lost: %s", qty)
			}
			if ps.NodeSelector["pool"] != "test" {
				t.Fatal("operator node selector lost")
			}
			if tc.want == deviceplugin.ResourceMSHV && ps.NodeSelector[corev1.LabelArchStable] != "amd64" {
				t.Fatal("MSHV must select amd64")
			}
			if c.SecurityContext.Privileged == nil || *c.SecurityContext.Privileged {
				t.Fatal("worker must remain nonprivileged")
			}
			for _, v := range ps.Volumes {
				if v.HostPath != nil && v.HostPath.Path != nil && (*v.HostPath.Path == "/dev/mshv" || *v.HostPath.Path == "/dev/kvm") {
					t.Fatal("hypervisor must not use hostPath")
				}
			}
		})
	}
}
