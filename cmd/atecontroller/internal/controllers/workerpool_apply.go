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
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func applyWorkerPoolPodTemplate(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	tmpl *atev1alpha1.WorkerPoolPodTemplate,
) {
	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithAffinity(corev1ac.Affinity())

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
