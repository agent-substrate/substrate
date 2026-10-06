// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agent-substrate/substrate/internal/deviceplugin"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// validateMicroVMResources ensures a worker receives exactly one supported
// hypervisor. Requests-only selections are normalized into limits by pod shaping.
func validateMicroVMResources(wp *atev1alpha1.WorkerPool) error {
	if wp.Spec.SandboxClass != atev1alpha1.SandboxClassMicroVM || wp.Spec.Template == nil || wp.Spec.Template.Resources == nil {
		return nil
	}
	selected := ""
	for _, resources := range []corev1.ResourceList{wp.Spec.Template.Resources.Requests, wp.Spec.Template.Resources.Limits} {
		for _, name := range []string{deviceplugin.ResourceKVM, deviceplugin.ResourceMSHV} {
			quantity, ok := resources[corev1.ResourceName(name)]
			if !ok {
				continue
			}
			if quantity.Cmp(resource.MustParse("1")) != 0 {
				return fmt.Errorf("microvm resource %s must equal 1", name)
			}
			if selected != "" && selected != name {
				return fmt.Errorf("microvm workers must select only one of %s and %s", deviceplugin.ResourceKVM, deviceplugin.ResourceMSHV)
			}
			selected = name
		}
	}
	if selected == deviceplugin.ResourceMSHV {
		if arch := wp.Spec.Template.NodeSelector[corev1.LabelArchStable]; arch != "" && arch != "amd64" {
			return fmt.Errorf("MSHV workers require amd64, got node selector %q", arch)
		}
	}
	return nil
}
