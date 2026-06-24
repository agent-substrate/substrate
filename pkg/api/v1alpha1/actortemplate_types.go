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

type SnapshotsConfig struct {
	// Location to store snapshots in.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	Location string `json:"location"`
}

// ActorTemplateSpec defined desired spec of an actor.
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
