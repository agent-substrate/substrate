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

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"github.com/agent-substrate/substrate/internal/ateompath"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestBuildDeploymentApplyConfig(t *testing.T) {
	requiredNodeAffinity := &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "workload",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"substrate"},
				}},
			}},
		},
	}
	preferredNodeAffinity := &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight: 50,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "disk",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"ssd"},
				}},
			},
		}},
	}
	tolerationSeconds := int64(300)
	toleration := corev1.Toleration{
		Key:               "dedicated",
		Operator:          corev1.TolerationOpEqual,
		Value:             "workerpool",
		Effect:            corev1.TaintEffectNoSchedule,
		TolerationSeconds: &tolerationSeconds,
	}

	tests := []struct {
		name string
		wp   *atev1alpha1.WorkerPool
		want *appsv1ac.DeploymentApplyConfiguration
	}{
		{
			name: "default workerpool",
			wp:   testWorkerPoolApplyConfig(nil),
			want: expectedDeploymentApplyConfig(nil),
		},
		{
			name: "with node selector",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeSelector: map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithNodeSelector(map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				})
			}),
		},
		{
			name: "with tolerations",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				Tolerations: []corev1.Toleration{toleration},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{
					*corev1ac.Toleration().
						WithKey("dedicated").
						WithOperator(corev1.TolerationOpEqual).
						WithValue("workerpool").
						WithEffect(corev1.TaintEffectNoSchedule).
						WithTolerationSeconds(300),
				}
			}),
		},
		{
			name: "with node affinity",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeAffinity: requiredNodeAffinity,
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(
					corev1ac.NodeAffinity().WithRequiredDuringSchedulingIgnoredDuringExecution(
						corev1ac.NodeSelector().WithNodeSelectorTerms(
							corev1ac.NodeSelectorTerm().WithMatchExpressions(
								corev1ac.NodeSelectorRequirement().
									WithKey("workload").
									WithOperator(corev1.NodeSelectorOpIn).
									WithValues("substrate"),
							),
						),
					),
				))
			}),
		},
		{
			name: "with priority class name",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				PriorityClassName: "interactive-workerpool",
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithPriorityClassName("interactive-workerpool")
			}),
		},
		{
			name: "with resources",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.Containers[0].WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					}).
					WithLimits(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					}))
			}),
		},
		{
			name: "with combined scheduling fields",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeSelector: map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				},
				Tolerations:       []corev1.Toleration{toleration},
				PriorityClassName: "interactive-workerpool",
				NodeAffinity:      preferredNodeAffinity,
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithNodeSelector(map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				})
				podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{
					*corev1ac.Toleration().
						WithKey("dedicated").
						WithOperator(corev1.TolerationOpEqual).
						WithValue("workerpool").
						WithEffect(corev1.TaintEffectNoSchedule).
						WithTolerationSeconds(300),
				}
				podSpecAC.WithPriorityClassName("interactive-workerpool")
				podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(
					corev1ac.NodeAffinity().WithPreferredDuringSchedulingIgnoredDuringExecution(
						corev1ac.PreferredSchedulingTerm().
							WithWeight(50).
							WithPreference(corev1ac.NodeSelectorTerm().WithMatchExpressions(
								corev1ac.NodeSelectorRequirement().
									WithKey("disk").
									WithOperator(corev1.NodeSelectorOpIn).
									WithValues("ssd"),
							)),
					),
				))
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDeploymentApplyConfig(tt.wp)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("buildDeploymentApplyConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestMicroVMPodShape asserts the micro-VM sandbox class adds the /dev/kvm
// device (volume + container mount) and node placement (nodeSelector +
// toleration on ate.dev/sandboxClass); other classes get none of it.
func TestMicroVMPodShape(t *testing.T) {
	tests := []struct {
		name        string
		class       atev1alpha1.SandboxClass
		wantMicroVM bool
	}{
		{"gvisor default", "", false},
		{"gvisor explicit", atev1alpha1.SandboxClassGvisor, false},
		{"microvm", atev1alpha1.SandboxClassMicroVM, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp := testWorkerPoolApplyConfig(nil)
			wp.Spec.SandboxClass = tt.class
			ps := buildDeploymentApplyConfig(wp).Spec.Template.Spec

			hasVol := false
			for _, v := range ps.Volumes {
				if v.Name != nil && *v.Name == "dev-kvm" {
					hasVol = true
					if v.HostPath == nil || v.HostPath.Path == nil || *v.HostPath.Path != "/dev/kvm" ||
						v.HostPath.Type == nil || *v.HostPath.Type != corev1.HostPathCharDev {
						t.Errorf("dev-kvm volume = %+v, want /dev/kvm CharDevice", v.HostPath)
					}
				}
			}
			hasMount := false
			for _, c := range ps.Containers {
				for _, m := range c.VolumeMounts {
					if m.MountPath != nil && *m.MountPath == "/dev/kvm" {
						hasMount = true
					}
				}
			}
			_, hasSelector := ps.NodeSelector["ate.dev/sandboxClass"]
			hasTol := false
			for _, tol := range ps.Tolerations {
				if tol.Key != nil && *tol.Key == "ate.dev/sandboxClass" {
					hasTol = true
				}
			}
			if hasVol != tt.wantMicroVM || hasMount != tt.wantMicroVM || hasSelector != tt.wantMicroVM || hasTol != tt.wantMicroVM {
				t.Errorf("microvm shape: vol=%v mount=%v selector=%v toleration=%v, want all %v",
					hasVol, hasMount, hasSelector, hasTol, tt.wantMicroVM)
			}
		})
	}
}

func testWorkerPoolApplyConfig(tmpl *atev1alpha1.WorkerPoolPodTemplate) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "default", UID: "uid"},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:   2,
			AteomImage: "ateom:v1",
			Template:   tmpl,
		},
	}
}

func expectedDeploymentApplyConfig(mutatePodSpec func(*corev1ac.PodSpecApplyConfiguration)) *appsv1ac.DeploymentApplyConfiguration {
	wp := testWorkerPoolApplyConfig(nil)

	podSpecAC := corev1ac.PodSpec().
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsUser(0).
			WithRunAsGroup(0)).
		WithVolumes(corev1ac.Volume().
			WithName("run-ateom").
			WithHostPath(corev1ac.HostPathVolumeSource().
				WithPath(ateompath.BasePath).
				WithType(corev1.HostPathDirectoryOrCreate))).
		WithContainers(corev1ac.Container().
			WithName("ateom").
			WithImage(wp.Spec.AteomImage).
			WithArgs("--pod-uid=$(POD_UID)").
			WithSecurityContext(corev1ac.SecurityContext().
				WithPrivileged(true).
				WithRunAsUser(0).
				WithRunAsGroup(0)).
			WithEnv(corev1ac.EnvVar().
				WithName("POD_UID").
				WithValueFrom(corev1ac.EnvVarSource().
					WithFieldRef(corev1ac.ObjectFieldSelector().
						WithFieldPath("metadata.uid")))).
			WithVolumeMounts(corev1ac.VolumeMount().
				WithName("run-ateom").
				WithMountPath(ateompath.BasePath)).
			WithResources(corev1ac.ResourceRequirements()))

	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithAffinity(corev1ac.Affinity())
	if mutatePodSpec != nil {
		mutatePodSpec(podSpecAC)
	}

	return appsv1ac.Deployment(wp.Name, wp.Namespace).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(atev1alpha1.GroupVersion.String()).
			WithKind("WorkerPool").
			WithName(wp.Name).
			WithUID(wp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(wp.Spec.Replicas).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{"ate.dev/worker-pool": wp.Name})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(map[string]string{"ate.dev/worker-pool": wp.Name}).
				WithSpec(podSpecAC)))
}
