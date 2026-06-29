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

type PhaseType string

// Define your phases as constants
const (
	PhaseInitial           PhaseType = ""
	PhaseResumeGoldenActor PhaseType = "ResumeGoldenActor"
	PhaseWaitGoldenActor   PhaseType = "WaitGoldenActor"
	PhaseReady             PhaseType = "Ready"
	PhaseFailed            PhaseType = "Failed"
)

// Represents a durable directory on rootfs that persists across resumes and
// participates in snapshots.
type DurableDirVolumeSource struct {
}

// Represents the source of a volume to mount.
// Exactly one of its members must be specified.
//
// When adding a new source type, list it in the ExactlyOneOf marker below.
//
// +kubebuilder:validation:ExactlyOneOf={durableDir}
type VolumeSource struct {
	// durableDir represents a durable directory on rootfs that persists across
	// resumes and participates in snapshots.
	// +optional
	DurableDir *DurableDirVolumeSource `json:"durableDir,omitempty" protobuf:"bytes,2,opt,name=durableDir"`
}

type Volume struct {
	// name of the volume.
	//
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Label().validate(self).hasValue()",message="Name must be a valid DNS label"
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// volumeSource represents the location and type of the mounted volume.
	VolumeSource `json:",inline" protobuf:"bytes,2,opt,name=volumeSource"`
}

// VolumeMount describes a mounting of a Volume within a actor.
type VolumeMount struct {
	// This must match the Name of a Volume.
	//
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Label().validate(self).hasValue()",message="Name must be a valid DNS label"
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// Path within the actor at which the volume should be mounted. Must be a
	// clean absolute Unix path: must start with '/', not be '/', and contain
	// no ':', '..', '.', '//', trailing '/', or control characters.
	//
	// +required
	// +kubebuilder:validation:MaxLength=4096
	// +kubebuilder:validation:XValidation:rule="self.startsWith('/') && size(self) > 1 && !self.endsWith('/') && !self.contains('//') && !self.contains(':') && !self.matches('[\\x00-\\x1f\\x7f]') && !self.matches('(^|/)[.][.]?(/|$)')",message="MountPath must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters"
	MountPath string `json:"mountPath" protobuf:"bytes,3,opt,name=mountPath"`
}

// A single application container that you want to run within a WorkerPool.
type Container struct {
	// Name of the container.
	//
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Label().validate(self).hasValue()",message="Name must be a valid DNS label"
	Name string `json:"name"`

	// Image to use for the worker replicas.
	//
	// +required
	// +kubebuilder:validation:XValidation:rule="self.contains('@')",message="All images must be pinned (changing the image invalidates snapshots)"
	Image string `json:"image,omitempty"`

	// Entrypoint array. Not executed within a shell.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Command []string `json:"command,omitempty"`

	// Environment variables to set in the worker replicas.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Env []EnvVar `json:"env,omitempty"`

	// Readyz is an optional HTTP readiness probe. When set, the actor is not
	// considered ready (and Run/Restore RPCs do not return success) until the
	// container's HTTP endpoint returns 200.
	//
	// +optional
	Readyz *ContainerReadyz `json:"readyz,omitempty"`

	// volumeMounts define the volumes to mount into this container.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=32
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty"`
}

// ContainerReadyz configures the readiness signal for a container.
type ContainerReadyz struct {
	// HTTPGet specifies the HTTP request to perform against the container.
	//
	// +required
	HTTPGet *HTTPGetAction `json:"httpGet"`
}

// HTTPGetAction describes an HTTP GET request to perform against the
// container's interior IP. Modeled after a subset of corev1.HTTPGetAction.
type HTTPGetAction struct {
	// Path to access on the HTTP server. Defaults to "/readyz".
	// Must be a valid URL path starting with "/". Only characters permitted
	// by RFC 3986 path segments are accepted; percent-escapes must be a
	// literal "%" followed by exactly two hex digits. Query strings ("?")
	// and fragments ("#") must be omitted.
	//
	// +optional
	// +kubebuilder:default="/readyz"
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^/([A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$`
	Path string `json:"path,omitempty"`

	// Port to access on the container.
	//
	// +required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`
}

// EnvVar represents an environment variable supplied to a container in an
// ActorTemplate. It models only a subset of Kubernetes Pod env behavior:
// literal values are not expanded with Kubernetes-style $(VAR) references,
// envFrom is not supported, and valueFrom currently supports only secretKeyRef.
//
// +kubebuilder:validation:ExactlyOneOf={value, valueFrom}
type EnvVar struct {
	// Name is the name of the environment variable. May be any printable ASCII
	// character except '='.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[ -<>-~]+$`
	Name string `json:"name"`

	// Exactly one of the following must be specified.

	// Variable value. Mutually exclusive with ValueFrom.
	// Value is the literal value of the environment variable. Unlike in
	// Kubernetes pods, this value is not interpolated, and $(VAR)
	// references are not expanded.
	//
	// +optional
	// +kubebuilder:validation:MinLength=0
	Value *string `json:"value,omitempty"`

	// Source for the environment variable's value. Mutually exclusive with
	// Value.
	//
	// +optional
	ValueFrom *EnvVarSource `json:"valueFrom,omitempty"`
}

// EnvVarSource represents a source for the value of an EnvVar. Exactly one of
// its fields must be set.
//
// +kubebuilder:validation:MinProperties=1
// +kubebuilder:validation:MaxProperties=1
type EnvVarSource struct {
	// Selects a key of a Secret in the ActorTemplate's namespace.
	//
	// +optional
	SecretKeyRef *SecretKeySelector `json:"secretKeyRef,omitempty"`
}

// SecretKeySelector selects a key from a Secret.
type SecretKeySelector struct {
	// Name of the referent Secret.
	//
	// +required
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Subdomain().validate(self).hasValue()",message="Name must be a valid DNS subdomain"
	Name string `json:"name"`

	// Key to select within the Secret.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	Key string `json:"key"`

	// Specify whether the Secret or its key must be defined.
	//
	// +optional
	Optional *bool `json:"optional,omitempty"`
}

// SnapshotScope defines what components to include in a snapshot.
// +kubebuilder:validation:Enum=Full;Data
type SnapshotScope string

const (
	// Full captures process memory plus the entire filesystem delta on top of
	// the OCI image (including any attached DurableDir volumes).
	SnapshotScopeFull SnapshotScope = "Full"
	// Data captures only the contents of attached volumes that support
	// snapshots (currently DurableDir-typed volumes). Process memory and
	// the rest of rootfs are excluded.
	SnapshotScopeData SnapshotScope = "Data"
)

// +kubebuilder:validation:XValidation:rule="(has(self.onPause) ? self.onPause : 'Full') == 'Full' || (has(self.onCommit) ? self.onCommit : 'Full') == (has(self.onPause) ? self.onPause : 'Full')",message="onCommit must be a subset of onPause"
type SnapshotsConfig struct {
	// Location to store snapshots in.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	Location string `json:"location"`

	// OnPause specifies what to include in the snapshot when the actor is paused.
	// If not provided, the "Full" behavior is used by default.
	//
	// +optional
	// +kubebuilder:default=Full
	OnPause SnapshotScope `json:"onPause,omitempty"`

	// OnCommit specifies what to include in the snapshot when a commit is requested.
	// If not provided, the "Full" behavior is used by default.
	// onCommit must be a subset of the onPause content.
	//
	// For example:
	//   - if onPause is "Full", then onCommit can be "Full" or "Data".
	//   - if onPause is "Data", then onCommit must be "Data".
	//
	// +optional
	// +kubebuilder:default=Full
	OnCommit SnapshotScope `json:"onCommit,omitempty"`
}

// ActorTemplateSpec defined desired spec of an actor.
//
// +kubebuilder:validation:XValidation:rule="!has(self.volumes) || self.volumes.filter(v, has(v.durableDir)).size() <= 1",message="At most one DurableDir-typed volume is supported per ActorTemplate"
// +kubebuilder:validation:XValidation:rule="!has(self.containers) || self.containers.all(c, !has(c.volumeMounts) || c.volumeMounts.filter(vm, has(self.volumes) && self.volumes.exists(v, v.name == vm.name && has(v.durableDir))).size() <= 1)",message="A container may mount at most one DurableDir-typed volume"
// +kubebuilder:validation:XValidation:rule="!has(self.sandboxClass) || self.sandboxClass != 'microvm' || !has(self.volumes) || !self.volumes.exists(v, has(v.durableDir))",message="DurableDir volumes are not supported when sandboxClass is 'microvm'"
type ActorTemplateSpec struct {
	// PauseImage is the container to use as the root sandbox container.
	//
	// Typically, set it to [1] for on-gcp, and [2] for off-gcp
	//
	//   - [1] gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da
	//   - [2] registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4
	//
	// +required
	// +kubebuilder:validation:XValidation:rule="self.contains('@')",message="All images must be pinned (changing the image invalidates snapshots)"
	PauseImage string `json:"pauseImage,omitempty"`

	// Containers is the workload definition.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=10
	Containers []Container `json:"containers,omitempty"`

	// Snapshots configuration for the actor.
	//
	// +required
	SnapshotsConfig SnapshotsConfig `json:"snapshotsConfig"`

	// SandboxClass selects the sandbox runtime family this template's actors run
	// on. Only worker pools whose SandboxClass matches are eligible. Snapshots are
	// not portable across classes, so this is a hard gate, AND'd with WorkerSelector
	// and the actor's worker_selector. Defaults to gvisor.
	//
	// TODO: This is almost certainly insufficient.  We have to decide a number of things:
	//
	// 1) How does someone discover what classes are available, or what they mean?
	// 2) How does someone define a new sandbox class?
	// 3) Does a class mean the specific type of sandbox tech or does it include some aspect of config (e.g. can we have 2 different classes which both use gVisor with different config, or 2 classes which use different microvms)
	// 4) How does the default get set and who sets it?
	//
	// See Also: WorkerPool SandboxClass
	//
	//
	// +optional
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass,omitempty"`

	// WorkerSelector restricts which worker pools actors from this template may
	// use. The scheduler only considers pools whose labels match this selector.
	// If nil, all pools are eligible (subject to the actor's own worker_selector).
	// Acts as a gate: the actor's worker_selector can only narrow this set further,
	// never expand it.
	//
	// +optional
	WorkerSelector *metav1.LabelSelector `json:"workerSelector,omitempty"`

	// Volumes defines the volumes to mount into all containers in the actor.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Volumes []Volume `json:"volumes,omitempty"`
}

// TODO: add validation
type ActorTemplateStatus struct {
	// Phase of the actor template.
	// +optional
	Phase PhaseType `json:"phase,omitempty"`

	GoldenActorID        string      `json:"goldenActorID,omitempty"`
	TakeGoldenSnapshotAt metav1.Time `json:"takeGoldenSnapshotAt,omitempty"`
	GoldenSnapshot       string      `json:"goldenSnapshot,omitempty"`

	// conditions defines the status conditions array
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=actortemplate
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.sandboxClass`
type ActorTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ActorTemplate. This field is immutable.
	// +required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Spec is immutable"
	Spec ActorTemplateSpec `json:"spec"`

	// status is the observed state of ActorTemplate
	// +optional
	Status ActorTemplateStatus `json:"status,omitempty"`
}

// ActorTemplateList contains a list of ActorTemplates.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=actortemplate
type ActorTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ActorTemplate `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ActorTemplate{}, &ActorTemplateList{})
}
