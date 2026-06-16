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
)

func TestTolerationsToApply(t *testing.T) {
	tolerations := []corev1.Toleration{{
		Key:               "gpu",
		Operator:          corev1.TolerationOpEqual,
		Value:             "true",
		Effect:            corev1.TaintEffectNoSchedule,
		TolerationSeconds: ptrInt64(300),
	}}

	got := tolerationsToApply(tolerations)
	if len(got) != 1 {
		t.Fatalf("expected 1 toleration, got %d", len(got))
	}
	if got[0].Key == nil || *got[0].Key != "gpu" {
		t.Fatalf("unexpected key: %v", got[0].Key)
	}
	if got[0].TolerationSeconds == nil || *got[0].TolerationSeconds != 300 {
		t.Fatalf("unexpected tolerationSeconds: %v", got[0].TolerationSeconds)
	}
}

func TestNodeAffinityToApply(t *testing.T) {
	na := &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "zone",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"us-west1-a"},
				}},
			}},
		},
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

	got := nodeAffinityToApply(na)
	if got.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatal("expected required node selector")
	}
	if len(got.PreferredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("expected 1 preferred term, got %d", len(got.PreferredDuringSchedulingIgnoredDuringExecution))
	}
	if got.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight == nil || *got.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight != 50 {
		t.Fatalf("unexpected weight: %v", got.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight)
	}
}

func ptrInt64(v int64) *int64 {
	return &v
}
