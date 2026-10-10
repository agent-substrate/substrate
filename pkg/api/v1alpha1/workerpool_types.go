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

	// SecurityContext restricts the worker container's runtime permissions.
	// These settings do not change the actor containers' security contexts.
	// +optional
	SecurityContext *WorkerPoolSecurityContext `json:"securityContext,omitempty"`
}

// WorkerPoolSecurityContext configures restrictions on the worker container.
type WorkerPoolSecurityContext struct {
	// DropCapabilities removes capabilities from the sandbox class's default set.
	// ALL removes the entire set. No capabilities are added by this field.
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=set
	// +kubebuilder:validation:items:Enum=ALL;NET_ADMIN;SYS_ADMIN;SYS_CHROOT;SYS_PTRACE;SETUID;SETGID;SETPCAP;DAC_OVERRIDE;FOWNER;CHOWN;MKNOD;NET_RAW;SETFCAP;FSETID;DAC_READ_SEARCH
	DropCapabilities []corev1.Capability `json:"dropCapabilities,omitempty"`

	// SeccompProfile replaces the sandbox class's default seccomp profile.
	// A Localhost profile must be installed on every node this pool selects.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self.type in ['Unconfined', 'RuntimeDefault', 'Localhost']",message="invalid seccomp profile type"
	// +kubebuilder:validation:XValidation:rule="self.type == 'Localhost' ? (has(self.localhostProfile) && size(self.localhostProfile) > 0) : !has(self.localhostProfile)",message="localhostProfile is required only for Localhost"
	// +kubebuilder:validation:XValidation:rule="!has(self.localhostProfile) || (self.localhostProfile.matches('^[^/]+(/[^/]+)*$') && !self.localhostProfile.matches('(^|/)[.]{1,2}(/|$)'))",message="localhostProfile must be a relative path without traversal"
	SeccompProfile *corev1.SeccompProfile `json:"seccompProfile,omitempty"`

	// AllowPrivilegeEscalation requests no_new_privs when false; explicit true is rejected.
	// When omitted, the controller leaves the field unset, preserving default runtime behavior.
	// Removing a configured false restores the omitted/default behavior.
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == false",message="allowPrivilegeEscalation can only be disabled"
	AllowPrivilegeEscalation *bool `json:"allowPrivilegeEscalation,omitempty"`
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

	// SandboxClasses lists the sandbox runtime families this pool runs. Exactly
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
	// Name selects the sandbox runtime family, which drives the worker pod
	// shape (KVM/vhost device mounts and node placement). The concrete binary
	// is still selected by WorkerImage. The sandbox binaries themselves come
	// from the SandboxConfig each ActorTemplate names (required).
	//
	// See Also: TODOs in ActorTemplate SandboxClass
	//
	// +required
	// +kubebuilder:validation:Enum=gvisor;microvm
	Name SandboxClass `json:"name,omitempty"`

	// ConfigRef names a cluster-scoped SandboxConfig for this sandbox class.
	// The referenced config's SandboxClass must match Name. Not consumed yet.
	//
	// +optional
	ConfigRef *SandboxConfigReference `json:"configRef,omitempty"`
}

// SandboxConfigReference names a cluster-scoped SandboxConfig.
type SandboxConfigReference struct {
	// Name is the SandboxConfig's metadata.name.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
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
}

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
