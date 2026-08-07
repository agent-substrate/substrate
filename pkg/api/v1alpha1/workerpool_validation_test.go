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

package v1alpha1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerPoolValidation(t *testing.T) {
	ctx := context.Background()

	basePool := &WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: WorkerPoolSpec{
			Replicas:   1,
			AteomImage: "ateom:latest",
		},
	}

	tests := []struct {
		name    string
		mutate  func(*WorkerPool)
		wantErr bool
		errMsg  string
	}{{
		name:    "base worker pool",
		mutate:  func(wp *WorkerPool) {},
		wantErr: false,
	}, {
		name: "replicas below minimum",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Replicas = -1
		},
		wantErr: true,
		errMsg:  "spec.replicas: Invalid value: -1: spec.replicas in body should be greater than or equal to 0",
	}, {
		name: "missing ateomImage",
		mutate: func(wp *WorkerPool) {
			wp.Spec.AteomImage = ""
		},
		wantErr: true,
		errMsg:  "spec.ateomImage: Invalid value: \"\": spec.ateomImage in body should be at least 1 chars long",
	}, {
		name: "valid template",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{
				NodeSelector: map[string]string{"workload": "substrate"},
				Tolerations: []corev1.Toleration{{
					Key:      "gpu",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				}},
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
			}
		},
		wantErr: false,
	}, {
		name: "too many tolerations",
		mutate: func(wp *WorkerPool) {
			tolerations := make([]corev1.Toleration, 17)
			for i := range tolerations {
				tolerations[i] = corev1.Toleration{
					Key:      "key",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				}
			}
			wp.Spec.Template = &WorkerPoolPodTemplate{Tolerations: tolerations}
		},
		wantErr: true,
		errMsg:  "spec.template.tolerations: Too many",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp := basePool.DeepCopy()
			tt.mutate(wp)

			err := k8sClient.Create(ctx, wp)
			if err != nil && !tt.wantErr {
				t.Errorf("unexpected failure: %v", err)
			}
			if err == nil && tt.wantErr {
				t.Errorf("unexpected success, expected %q", tt.errMsg)
			}
			if err != nil && tt.wantErr && tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("wrong error:\n  wanted: %q\n     got: %q", tt.errMsg, err.Error())
			}

			if err == nil {
				_ = k8sClient.Delete(ctx, wp)
			}
		})
	}
}
