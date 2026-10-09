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

// WorkerPoolLabelValue is a Kubernetes label value for generated worker
// workloads.
//
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`
type WorkerPoolLabelValue string

// WorkerPoolPodTemplate defines optional metadata, scheduling, and resource
// settings for worker workloads. NodeAffinity is mapped to
// spec.affinity.nodeAffinity on the pod.
type WorkerPoolPodTemplate struct {
	// Labels are added to the generated Deployment and worker pods. Keys in
	// the ate.dev domain and its subdomains are reserved for controllers.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(key, !key.startsWith('ate.dev/') && !key.contains('.ate.dev/'))",message="ate.dev and its subdomains are reserved"
	// +kubebuilder:validation:XValidation:rule="self.all(key, !format.qualifiedName().validate(key).hasValue())",message="label keys must be valid Kubernetes qualified names"
	Labels map[string]WorkerPoolLabelValue `json:"labels,omitempty"`

	// Annotations are added to the generated Deployment and worker pods. Keys
	// in the ate.dev domain and its subdomains are reserved for controllers.
	//
	// +optional
	// +kubebuilder:validation:MaxProperties=64
	// +kubebuilder:validation:XValidation:rule="self.all(key, !key.startsWith('ate.dev/') && !key.contains('.ate.dev/'))",message="ate.dev and its subdomains are reserved"
	// +kubebuilder:validation:XValidation:rule="self.all(key, !format.qualifiedName().validate(key).hasValue())",message="annotation keys must be valid Kubernetes qualified names"
	Annotations map[string]string `json:"annotations,omitempty"`

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

	// ServiceAccountName for the worker pods. The ServiceAccount must exist
	// in the WorkerPool's namespace. When omitted, the pods use the
	// namespace's default ServiceAccount.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	ServiceAccountName *string `json:"serviceAccountName,omitempty"`
}

type WorkerPoolSpec struct {
	// Replicas is the number of worker pods to run.
	// +required
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`

	// WorkerImage is the ateom container image to deploy as workers.
	// +kubebuilder:validation:MinLength=1
	// +required
	WorkerImage string `json:"workerImage"`

	// Template holds optional metadata, scheduling, and resource settings for worker workloads.
	//
	// +optional
	Template *WorkerPoolPodTemplate `json:"template,omitempty"`

	// sandboxClasses lists the sandbox runtime families this pool runs. Exactly
	// one entry is allowed today.
	//
	// +required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	// +listType=map
	// +listMapKey=name
	SandboxClasses []WorkerPoolSandboxClass `json:"sandboxClasses,omitempty"`
}

// WorkerPoolSandboxClass is one sandbox runtime family a WorkerPool runs.
type WorkerPoolSandboxClass struct {
	// name selects the sandbox runtime family, which drives the worker pod
	// shape (KVM/vhost device mounts and node placement). The concrete binary
	// is still selected by WorkerImage. The sandbox binaries themselves come
	// from the SandboxConfig each ActorTemplate names (required).
	//
	// See Also: TODOs in ActorTemplate SandboxClass
	//
	// +required
	// +kubebuilder:validation:Enum=gvisor;microvm
	Name SandboxClass `json:"name,omitempty"`

	// configRef names a cluster-scoped SandboxConfig for this sandbox class.
	// The referenced config's sandboxClass must match name. When omitted, the
	// pool uses the newest SandboxConfig of this class annotated
	// sandboxconfig.ate.dev/is-class-default: "true". The controller reports
	// the resolved config in status.sandboxClasses.
	//
	// +optional
	ConfigRef *SandboxConfigReference `json:"configRef,omitempty"`
}

// SandboxConfigReference names a cluster-scoped SandboxConfig.
type SandboxConfigReference struct {
	// name is the SandboxConfig's metadata.name.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name,omitempty"`
}

// DefaultSandboxClass returns the sandbox class of the pool's first
// SandboxClasses entry, or "" if there is none.
// Currently SandboxClasses is always a list of length 1.
func (s *WorkerPoolSpec) DefaultSandboxClass() SandboxClass {
	if len(s.SandboxClasses) == 0 {
		return ""
	}
	return s.SandboxClasses[0].Name
}

type WorkerPoolStatus struct {
	// Replicas is the total number of worker pods.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas int32 `json:"replicas"`

	// ReadyReplicas is the number of ready worker pods.
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Selector is the label selector for the worker pods.
	// +optional
	Selector string `json:"selector,omitempty"`

	// sandboxClasses reports the SandboxConfig each sandbox class of the pool
	// uses. An entry keeps its last resolved config while spec.sandboxClasses
	// cannot be resolved; see the SandboxConfigResolved condition.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=1
	// +listType=map
	// +listMapKey=name
	SandboxClasses []WorkerPoolSandboxClassStatus `json:"sandboxClasses,omitempty"`

	// conditions describe the current state of the WorkerPool.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkerPoolSandboxClassStatus is the SandboxConfig one sandbox class of a
// WorkerPool uses.
type WorkerPoolSandboxClassStatus struct {
	// name is the sandbox runtime family.
	//
	// +required
	// +kubebuilder:validation:Enum=gvisor;microvm
	Name SandboxClass `json:"name,omitempty"`

	// configRef names the cluster-scoped SandboxConfig in use: either
	// spec.sandboxClasses[].configRef or the class default.
	//
	// +required
	ConfigRef SandboxConfigReference `json:"configRef,omitzero"`
}

const (
	// WorkerPoolConditionSandboxConfigResolved is True when every entry of
	// spec.sandboxClasses resolves to a usable SandboxConfig. While it is
	// False, the controller does not create or update the worker Deployment.
	WorkerPoolConditionSandboxConfigResolved = "SandboxConfigResolved"

	// WorkerPoolReasonResolved: every sandbox class resolved.
	WorkerPoolReasonResolved = "Resolved"
	// WorkerPoolReasonSandboxConfigNotFound: configRef names a SandboxConfig
	// that does not exist.
	WorkerPoolReasonSandboxConfigNotFound = "SandboxConfigNotFound"
	// WorkerPoolReasonSandboxConfigDeleting: configRef names a SandboxConfig
	// that is being deleted and that the pool does not already use.
	WorkerPoolReasonSandboxConfigDeleting = "SandboxConfigDeleting"
	// WorkerPoolReasonSandboxClassMismatch: configRef names a SandboxConfig
	// of another sandbox class.
	WorkerPoolReasonSandboxClassMismatch = "SandboxClassMismatch"
	// WorkerPoolReasonDefaultConfigNotFound: configRef is omitted, no
	// SandboxConfig of the class is annotated as the class default, and the
	// pool is not already using one that still exists.
	WorkerPoolReasonDefaultConfigNotFound = "DefaultConfigNotFound"
)

// WorkerPool is the Schema for the workerpools API
// +genclient
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=workerpool
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="SandboxConfig",type=string,JSONPath=`.status.sandboxClasses[*].configRef.name`
// +kubebuilder:printcolumn:name="Resolved",type=string,JSONPath=`.status.conditions[?(@.type=="SandboxConfigResolved")].status`
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
