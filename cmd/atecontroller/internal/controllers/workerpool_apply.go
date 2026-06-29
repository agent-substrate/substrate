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
	corev1 "k8s.io/api/core/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"github.com/agent-substrate/substrate/internal/ateompath"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// buildDeploymentApplyConfig constructs the SSA apply configuration for the
// Deployment managed by a WorkerPool. Only fields owned by this controller
// are declared here.
func buildDeploymentApplyConfig(wp *atev1alpha1.WorkerPool) *appsv1ac.DeploymentApplyConfiguration {
	containerAC := corev1ac.Container().
		WithName("ateom").
		WithImage(wp.Spec.AteomImage).
		WithArgs(
			"--pod-uid=$(POD_UID)",
		).
		WithSecurityContext(corev1ac.SecurityContext().
			WithPrivileged(true).
			WithRunAsUser(0).
			WithRunAsGroup(0)).
		WithEnv(
			corev1ac.EnvVar().
				WithName("POD_UID").
				WithValueFrom(corev1ac.EnvVarSource().
					WithFieldRef(corev1ac.ObjectFieldSelector().
						WithFieldPath("metadata.uid"))),
		).
		WithVolumeMounts(corev1ac.VolumeMount().
			WithName("run-ateom").
			WithMountPath(ateompath.BasePath))

	podSpecAC := corev1ac.PodSpec().
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsUser(0).
			WithRunAsGroup(0)).
		WithVolumes(corev1ac.Volume().
			WithName("run-ateom").
			WithHostPath(corev1ac.HostPathVolumeSource().
				WithPath(ateompath.BasePath).
				WithType(corev1.HostPathDirectoryOrCreate)))

	applyWorkerPoolPodTemplate(podSpecAC, containerAC, wp.Spec.Template)
	maybeApplyMicroVMPodShape(podSpecAC, containerAC, wp.Spec.SandboxClass)
	podSpecAC.WithContainers(containerAC)

	return appsv1ac.Deployment(deploymentName(wp.Name), wp.Namespace).
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
				WithLabels(map[string]string{
					"ate.dev/worker-pool": wp.Name,
				}).
				WithSpec(podSpecAC)))
}

// maybeApplyMicroVMPodShape adds the /dev/kvm device and node placement a
// micro-VM (kata + cloud-hypervisor) worker pool needs, on top of any
// pod-template settings. No-op unless sandboxClass is the micro-VM class.
//
// TODO: this hardcodes one sandbox class's pod requirements in the controller.
// Consider making it generic so a sandbox class can declare its own pod shape
// (e.g. required devices/mounts + node placement on the SandboxConfig spec)
// instead of branching on SandboxClass here, so new classes don't need a
// controller change.
func maybeApplyMicroVMPodShape(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	sandboxClass atev1alpha1.SandboxClass,
) {
	if sandboxClass != atev1alpha1.SandboxClassMicroVM {
		return
	}

	// The micro-VM runtime needs /dev/kvm. The container is already privileged
	// (so it can also reach vhost devices), but we mount /dev/kvm explicitly.
	containerAC.WithVolumeMounts(corev1ac.VolumeMount().
		WithName("dev-kvm").
		WithMountPath("/dev/kvm"))
	podSpecAC.WithVolumes(corev1ac.Volume().
		WithName("dev-kvm").
		WithHostPath(corev1ac.HostPathVolumeSource().
			WithPath("/dev/kvm").
			WithType(corev1.HostPathCharDev)))

	// Pin placement to KVM-capable, nested-virt nodes via nodeSelector +
	// toleration on ate.dev/sandboxClass=microvm. This is our own convention
	// (GKE attaches no label/taint to nested-virt pools): applied to kind nodes
	// by hack/create-kind-cluster.sh and via --node-labels at GKE pool creation.
	// Additive on top of the WorkerPool's configurable scheduling fields
	// (spec.template nodeSelector/tolerations/affinity, added in #247) — merge,
	// don't overwrite.
	if podSpecAC.NodeSelector == nil {
		podSpecAC.NodeSelector = map[string]string{}
	}
	podSpecAC.NodeSelector["ate.dev/sandboxClass"] = string(atev1alpha1.SandboxClassMicroVM)
	podSpecAC.WithTolerations(corev1ac.Toleration().
		WithKey("ate.dev/sandboxClass").
		WithOperator(corev1.TolerationOpEqual).
		WithValue(string(atev1alpha1.SandboxClassMicroVM)).
		WithEffect(corev1.TaintEffectNoSchedule))
}

func applyWorkerPoolPodTemplate(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	tmpl *atev1alpha1.WorkerPoolPodTemplate,
) {
	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithAffinity(corev1ac.Affinity())
	resourcesAC := corev1ac.ResourceRequirements()
	containerAC.WithResources(resourcesAC)

	if tmpl == nil {
		return
	}

	if tmpl.NodeSelector != nil {
		podSpecAC.WithNodeSelector(tmpl.NodeSelector)
	}
	podSpecAC.Tolerations = tolerationApplyValues(tolerationsToApply(tmpl.Tolerations))
	podSpecAC.WithPriorityClassName(tmpl.PriorityClassName)

	if tmpl.NodeAffinity != nil {
		podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(nodeAffinityToApply(tmpl.NodeAffinity)))
	}

	if tmpl.Resources != nil {
		if tmpl.Resources.Requests != nil {
			resourcesAC.WithRequests(tmpl.Resources.Requests)
		}
		if tmpl.Resources.Limits != nil {
			resourcesAC.WithLimits(tmpl.Resources.Limits)
		}
	}
}

func tolerationApplyValues(tolerations []*corev1ac.TolerationApplyConfiguration) []corev1ac.TolerationApplyConfiguration {
	out := make([]corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for _, toleration := range tolerations {
		out = append(out, *toleration)
	}
	return out
}

func tolerationsToApply(tolerations []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	out := make([]*corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for i := range tolerations {
		t := &tolerations[i]
		ac := corev1ac.Toleration()
		if t.Key != "" {
			ac.WithKey(t.Key)
		}
		if t.Operator != "" {
			ac.WithOperator(t.Operator)
		}
		if t.Value != "" {
			ac.WithValue(t.Value)
		}
		if t.Effect != "" {
			ac.WithEffect(t.Effect)
		}
		if t.TolerationSeconds != nil {
			ac.WithTolerationSeconds(*t.TolerationSeconds)
		}
		out = append(out, ac)
	}
	return out
}

func nodeAffinityToApply(na *corev1.NodeAffinity) *corev1ac.NodeAffinityApplyConfiguration {
	ac := corev1ac.NodeAffinity()
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		ac.WithRequiredDuringSchedulingIgnoredDuringExecution(nodeSelectorToApply(na.RequiredDuringSchedulingIgnoredDuringExecution))
	}
	for i := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		term := &na.PreferredDuringSchedulingIgnoredDuringExecution[i]
		ac.WithPreferredDuringSchedulingIgnoredDuringExecution(preferredSchedulingTermToApply(term))
	}
	return ac
}

func nodeSelectorToApply(ns *corev1.NodeSelector) *corev1ac.NodeSelectorApplyConfiguration {
	ac := corev1ac.NodeSelector()
	for i := range ns.NodeSelectorTerms {
		ac.WithNodeSelectorTerms(nodeSelectorTermToApply(&ns.NodeSelectorTerms[i]))
	}
	return ac
}

func preferredSchedulingTermToApply(term *corev1.PreferredSchedulingTerm) *corev1ac.PreferredSchedulingTermApplyConfiguration {
	return corev1ac.PreferredSchedulingTerm().
		WithWeight(term.Weight).
		WithPreference(nodeSelectorTermToApply(&term.Preference))
}

func nodeSelectorTermToApply(term *corev1.NodeSelectorTerm) *corev1ac.NodeSelectorTermApplyConfiguration {
	ac := corev1ac.NodeSelectorTerm()
	for i := range term.MatchExpressions {
		ac.WithMatchExpressions(nodeSelectorRequirementToApply(&term.MatchExpressions[i]))
	}
	for i := range term.MatchFields {
		ac.WithMatchFields(nodeSelectorRequirementToApply(&term.MatchFields[i]))
	}
	return ac
}

func nodeSelectorRequirementToApply(req *corev1.NodeSelectorRequirement) *corev1ac.NodeSelectorRequirementApplyConfiguration {
	ac := corev1ac.NodeSelectorRequirement().WithKey(req.Key).WithOperator(req.Operator)
	if len(req.Values) > 0 {
		ac.WithValues(req.Values...)
	}
	return ac
}
