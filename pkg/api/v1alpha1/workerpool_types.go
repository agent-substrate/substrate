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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:XValidation:rule="!has(self.minReady) || !has(self.maxReplicas) || self.minReady <= self.maxReplicas",message="minReady must not exceed maxReplicas"
type WorkerPoolSpec struct {
	// Replicas is the number of worker pods to run. When autoscaling is enabled
	// it is owned by the autoscaler (written via the scale subresource); the
	// fields below are the declarative inputs that drive it.
	// +required
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// AteomImage is the ateom container image to deploy as workers.
	// +kubebuilder:validation:MinLength=1
	// +required
	AteomImage string `json:"ateomImage"`

	// MinReady is the minimum number of worker pods the autoscaler keeps the
	// pool at — the reservation floor it must never scale below. When unset the
	// pool may be scaled to zero. The floor is enforced by the autoscaler; the
	// WorkerPool controller never clamps Replicas itself, so that the scale
	// subresource keeps a single writer.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinReady *int32 `json:"minReady,omitempty"`

	// TargetBuffer is the desired number of idle (warm) workers the autoscaler
	// keeps available to absorb resume bursts. When the idle count falls below
	// this target the autoscaler provisions more workers, net of pods already
	// starting. When unset, buffer-based scale-up is disabled.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TargetBuffer *int32 `json:"targetBuffer,omitempty"`

	// MaxReplicas is the upper bound the autoscaler may grow the pool to. When
	// unset the autoscaler applies no ceiling of its own.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
}

type WorkerPoolStatus struct {
	// Replicas is the total number of worker pods.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas int32 `json:"replicas"`
}

// WorkerPool is the Schema for the workerpools API
// +genclient
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=workerpool
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type WorkerPool struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of WorkerPool
	// +required
	Spec WorkerPoolSpec `json:"spec"`

	// status is the observed state of WorkerPool
	// +optional
	Status WorkerPoolStatus `json:"status,omitempty"`
}

// WorkerPoolList contains a list of WorkerPools.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
type WorkerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WorkerPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkerPool{}, &WorkerPoolList{})
}
