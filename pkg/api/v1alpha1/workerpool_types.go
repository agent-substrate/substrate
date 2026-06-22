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
}

// StorageVolume describes a network or persistent storage volume to be mounted
// into each worker pod. Actors reference these by name via their ActorTemplate's
// Volumes field. Exactly one source (NFS, PersistentVolumeClaim, or HostPath)
// must be specified.
type StorageVolume struct {
	// Name of the volume. Referenced by ActorTemplate volume entries.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z][a-zA-Z0-9-]*$`
	Name string `json:"name"`

	// NFS represents an NFS mount on the host.
	//
	// +optional
	NFS *corev1.NFSVolumeSource `json:"nfs,omitempty"`

	// PersistentVolumeClaim references a PVC in the worker pool's namespace.
	//
	// +optional
	PersistentVolumeClaim *corev1.PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty"`

	// HostPath represents a pre-existing host path (e.g., NFS already
	// mounted at a well-known path on all nodes).
	//
	// +optional
	HostPath *corev1.HostPathVolumeSource `json:"hostPath,omitempty"`
}

type WorkerPoolSpec struct {
	// Replicas is the number of worker pods to run.
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

	// StorageVolumes defines network or persistent storage volumes that will be
	// mounted into each worker pod. Actors can reference these by name via their
	// ActorTemplate's volumes field.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	StorageVolumes []StorageVolume `json:"storageVolumes,omitempty"`
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
