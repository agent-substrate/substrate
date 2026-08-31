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
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const gpuResourceName = corev1.ResourceName("nvidia.com/gpu")

func gpuTemplate(limits, requests corev1.ResourceList) *WorkerPoolPodTemplate {
	return &WorkerPoolPodTemplate{
		Resources: &corev1.ResourceRequirements{Limits: limits, Requests: requests},
	}
}

func TestWorkerPoolValidation(t *testing.T) {
	ctx := context.Background()

	basePool := &WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: WorkerPoolSpec{
			Replicas:    1,
			WorkerImage: "ateom:latest",
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
		name: "unset workerImage is allowed",
		mutate: func(wp *WorkerPool) {
			wp.Spec.WorkerImage = ""
		},
		wantErr: false,
	}, {
		name: "valid template",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{
				Labels: map[string]WorkerPoolLabelValue{
					"project":                    "agent-substrate",
					"policy.example.com/profile": "sandbox_host",
				},
				Annotations: map[string]string{
					"policy.example.com/exemption": "sandbox-host",
				},
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
		name: "invalid worker label key",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Labels: map[string]WorkerPoolLabelValue{"bad key": "value"}}
		},
		wantErr: true,
		errMsg:  "label keys must be valid Kubernetes qualified names",
	}, {
		name: "invalid worker label value",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Labels: map[string]WorkerPoolLabelValue{"project": "bad value"}}
		},
		wantErr: true,
		errMsg:  "spec.template.labels.project in body should match",
	}, {
		name: "reserved ate.dev label",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Labels: map[string]WorkerPoolLabelValue{"ate.dev/custom": "value"}}
		},
		wantErr: true,
		errMsg:  "ate.dev and its subdomains are reserved",
	}, {
		name: "reserved ate.dev subdomain label",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Labels: map[string]WorkerPoolLabelValue{"policy.ate.dev/exemption": "value"}}
		},
		wantErr: true,
		errMsg:  "ate.dev and its subdomains are reserved",
	}, {
		name: "reserved ate.dev annotation",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Annotations: map[string]string{"ate.dev/custom": "value"}}
		},
		wantErr: true,
		errMsg:  "ate.dev and its subdomains are reserved",
	}, {
		name: "reserved ate.dev subdomain annotation",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Annotations: map[string]string{"policy.ate.dev/exemption": "value"}}
		},
		wantErr: true,
		errMsg:  "ate.dev and its subdomains are reserved",
	}, {
		name: "invalid worker annotation key",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = &WorkerPoolPodTemplate{Annotations: map[string]string{"bad key": "value"}}
		},
		wantErr: true,
		errMsg:  "annotation keys must be valid Kubernetes qualified names",
	}, {
		name: "too many worker labels",
		mutate: func(wp *WorkerPool) {
			labels := make(map[string]WorkerPoolLabelValue, 65)
			for i := range 65 {
				labels[fmt.Sprintf("label-%d", i)] = "value"
			}
			wp.Spec.Template = &WorkerPoolPodTemplate{Labels: labels}
		},
		wantErr: true,
		errMsg:  "spec.template.labels: Too many",
	}, {
		name: "too many worker annotations",
		mutate: func(wp *WorkerPool) {
			annotations := make(map[string]string, 65)
			for i := range 65 {
				annotations[fmt.Sprintf("annotation-%d", i)] = "value"
			}
			wp.Spec.Template = &WorkerPoolPodTemplate{Annotations: annotations}
		},
		wantErr: true,
		errMsg:  "spec.template.annotations: Too many",
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
	}, {
		name: "gpu on a gvisor pool",
		mutate: func(wp *WorkerPool) {
			wp.Spec.SandboxClass = SandboxClassGvisor
			wp.Spec.Template = gpuTemplate(corev1.ResourceList{gpuResourceName: resource.MustParse("1")}, nil)
		},
		wantErr: false,
	}, {
		name: "gpu limit on a micro-VM pool",
		mutate: func(wp *WorkerPool) {
			wp.Spec.SandboxClass = SandboxClassMicroVM
			wp.Spec.Template = gpuTemplate(corev1.ResourceList{gpuResourceName: resource.MustParse("1")}, nil)
		},
		wantErr: true,
		errMsg:  "nvidia.com/gpu is only supported when sandboxClass is 'gvisor'",
	}, {
		// The pod shape keys off limits OR requests, so the rule must reject both.
		name: "gpu request on a micro-VM pool",
		mutate: func(wp *WorkerPool) {
			wp.Spec.SandboxClass = SandboxClassMicroVM
			wp.Spec.Template = gpuTemplate(nil, corev1.ResourceList{gpuResourceName: resource.MustParse("1")})
		},
		wantErr: true,
		errMsg:  "nvidia.com/gpu is only supported when sandboxClass is 'gvisor'",
	}, {
		// Kubernetes refuses a pod that requests an extended resource without a
		// matching limit, so a pool like this would shape a worker the Deployment
		// then rejects. Catch it on the WorkerPool the user wrote.
		name: "gpu in requests only",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = gpuTemplate(nil, corev1.ResourceList{gpuResourceName: resource.MustParse("1")})
		},
		wantErr: true,
		errMsg:  "nvidia.com/gpu must be set in limits",
	}, {
		name: "gpu in both limits and requests",
		mutate: func(wp *WorkerPool) {
			wp.Spec.Template = gpuTemplate(
				corev1.ResourceList{gpuResourceName: resource.MustParse("1")},
				corev1.ResourceList{gpuResourceName: resource.MustParse("1")})
		},
		wantErr: false,
	}, {
		name: "micro-VM pool without a gpu",
		mutate: func(wp *WorkerPool) {
			wp.Spec.SandboxClass = SandboxClassMicroVM
			wp.Spec.Template = gpuTemplate(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}, nil)
		},
		wantErr: false,
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

func TestWorkerPoolReservedMetadataUpdate(t *testing.T) {
	ctx := context.Background()
	wp := &WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-reserved-metadata-update",
			Namespace: "default",
		},
		Spec: WorkerPoolSpec{
			Replicas:    1,
			WorkerImage: "example.com/ateom:latest",
		},
	}
	if err := k8sClient.Create(ctx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, wp) })

	wp.Spec.Template = &WorkerPoolPodTemplate{
		Annotations: map[string]string{"security.ate.dev/exemption": "value"},
	}
	err := k8sClient.Update(ctx, wp)
	if err == nil {
		t.Fatal("update unexpectedly accepted a reserved annotation")
	}
	if want := "ate.dev and its subdomains are reserved"; !strings.Contains(err.Error(), want) {
		t.Errorf("wrong error:\n  wanted: %q\n     got: %q", want, err.Error())
	}
}
