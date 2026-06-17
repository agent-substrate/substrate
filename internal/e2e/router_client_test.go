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

package e2e

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestResolveHTTPTargetPort(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "envoy",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			}},
		},
	}
	svc := func(p corev1.ServicePort) *corev1.Service {
		return &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "atenet-router"},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{p}},
		}
	}

	// Pod whose named "http" port resolves to an invalid zero container port.
	zeroPortPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "envoy",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 0}},
			}},
		},
	}

	tests := []struct {
		name    string
		svc     *corev1.Service
		pod     *corev1.Pod
		want    int32
		wantErr bool
	}{
		{"int target port", svc(corev1.ServicePort{Port: 80, TargetPort: intstr.FromInt(8080)}), pod, 8080, false},
		{"named target port", svc(corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("http")}), pod, 8080, false},
		{"named target not found", svc(corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("nope")}), pod, 0, true},
		{"named target resolves to zero", svc(corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("http")}), zeroPortPod, 0, true},
		{"unset target port", svc(corev1.ServicePort{Port: 80}), pod, 0, true},
		{"no port 80", svc(corev1.ServicePort{Port: 443, TargetPort: intstr.FromInt(8443)}), pod, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveHTTPTargetPort(tc.svc, tc.pod)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestIsPodReady(t *testing.T) {
	pod := func(phase corev1.PodPhase, ready corev1.ConditionStatus, deleting bool) *corev1.Pod {
		p := &corev1.Pod{Status: corev1.PodStatus{
			Phase:      phase,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}},
		}}
		if deleting {
			now := metav1.Now()
			p.DeletionTimestamp = &now
		}
		return p
	}

	tests := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{"running and ready", pod(corev1.PodRunning, corev1.ConditionTrue, false), true},
		{"running not ready", pod(corev1.PodRunning, corev1.ConditionFalse, false), false},
		{"pending", pod(corev1.PodPending, corev1.ConditionTrue, false), false},
		{"terminating", pod(corev1.PodRunning, corev1.ConditionTrue, true), false},
		{"no ready condition", &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPodReady(tc.pod); got != tc.want {
				t.Errorf("isPodReady = %v, want %v", got, tc.want)
			}
		})
	}
}
