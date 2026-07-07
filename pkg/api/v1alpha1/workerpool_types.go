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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WorkerPoolPodTemplate defines optional scheduling and resource settings for
// worker pods. NodeAffinity is mapped to spec.affinity.nodeAffinity on the pod.
type WorkerPoolPodTemplate struct {
	// NodeSelector is a selector which must be true for the pod to fit on a node.
	//
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations for the worker pods.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// PriorityClassName for the worker pods.
	//
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// NodeAffinity scheduling rules for the worker pods. Mapped to
	// spec.affinity.nodeAffinity on the pod.
	//
	// +optional
	NodeAffinity *corev1.NodeAffinity `json:"nodeAffinity,omitempty"`

	// Resources are the compute resources allocated for each worker pod.
	//
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// WorkerPoolAutoscaling holds the declarative inputs of the WorkerPool
// autoscaler. Its presence on a WorkerPoolSpec is what enables autoscaling for
// the pool — a pool with a nil Autoscaling field is never touched by the
// autoscaler, even if this struct's fields would all be zero.
//
// +kubebuilder:validation:XValidation:rule="!has(self.minReady) || !has(self.maxReplicas) || self.minReady <= self.maxReplicas",message="minReady must not exceed maxReplicas"
type WorkerPoolAutoscaling struct {
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

	// ScaleDownStabilization is how long a scale-down must be continuously
	// warranted before it is applied. Shrinking is deliberately slower than
	// growing: removing a warm worker that a burst will want back seconds later
	// costs a cold start, so the pool only shrinks after the surplus has held
	// for this whole window, and any tick that no longer wants the shrink
	// restarts it. Scale-up is never delayed by this setting. "0s" applies
	// scale-downs immediately. When unset, the autoscaler's cluster-wide
	// default (60s) is used.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="scaleDownStabilization must not be negative"
	ScaleDownStabilization *metav1.Duration `json:"scaleDownStabilization,omitempty"`

	// MaxScaleUpStep caps how many replicas a single scale-up may add on top of
	// the current count, smoothing bursts of demand into stepwise growth. The
	// reservation floor is exempt: a pool below MinReady is always raised to it
	// in one step regardless of this cap. When unset, scale-ups are uncapped
	// and jump straight to the computed target (the cluster-wide default).
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxScaleUpStep *int32 `json:"maxScaleUpStep,omitempty"`
}

type WorkerPoolSpec struct {
	// Replicas is the number of worker pods to run. When Autoscaling is set it
	// is owned by the autoscaler (written via the scale subresource) and this
	// value is only the starting point.
	// +required
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// AteomImage is the ateom container image to deploy as workers.
	// +kubebuilder:validation:MinLength=1
	// +required
	AteomImage string `json:"ateomImage"`

	// Template holds optional pod scheduling and resource settings for worker pods.
	//
	// +optional
	Template *WorkerPoolPodTemplate `json:"template,omitempty"`

	// SandboxClass selects the sandbox runtime family for this pool, which drives
	// the worker pod shape (KVM/vhost device mounts and node placement) and which
	// SandboxConfigs are eligible. The concrete binary is still selected by
	// AteomImage. Defaults to gvisor.
	//
	// See Also: TODOs in ActorTemplate SandboxClass
	//
	// +optional
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass,omitempty"`

	// SandboxConfigName names a cluster-scoped SandboxConfig to use for fetching
	// sandbox binaries. It overrides the cluster-wide default SandboxConfig for
	// this pool's SandboxClass. The referenced config's SandboxClass must match
	// this pool's SandboxClass. If empty, the default SandboxConfig for the
	// SandboxClass is used.
	// +optional
	SandboxConfigName string `json:"sandboxConfigName,omitempty"`

	// Autoscaling enables demand-reactive management of Replicas for this pool
	// and carries the autoscaler's bounds. When nil, autoscaling is off and
	// Replicas stays whatever a human (or other tooling) set it to.
	// +optional
	Autoscaling *WorkerPoolAutoscaling `json:"autoscaling,omitempty"`
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
