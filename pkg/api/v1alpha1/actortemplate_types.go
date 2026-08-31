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
	"k8s.io/apimachinery/pkg/api/resource"
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

// Represents the contents of an OCI image, mounted read-only.
type ImageVolumeSource struct {
	// reference is the image to mount.
	//
	// +required
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.contains('@')",message="All images must be pinned (changing the image invalidates snapshots)"
	Reference string `json:"reference"`
}

// Represents an external volume dynamically provisioned for each actor.
type ExternalVolumeTemplate struct {
	// capacity specifies the size of the volume to create.
	// +required
	Capacity resource.Quantity `json:"capacity"`
	// storageClassName refers to the StorageClass to create the volume from.
	// +required
	StorageClassName string `json:"storageClassName"`
}

// ActorMetadataField selects one identity field of the actor, following the
// resource identity model (see docs/api-style-guide.md#2-resource-naming-and-identity).
//
// +kubebuilder:validation:Enum=name;atespace;uid
type ActorMetadataField string

const (
	// ActorMetadataFieldName is the actor's metadata.name, unique within its
	// atespace.
	ActorMetadataFieldName ActorMetadataField = "name"
	// ActorMetadataFieldAtespace is the atespace the actor belongs to.
	ActorMetadataFieldAtespace ActorMetadataField = "atespace"
	// ActorMetadataFieldUID is the actor's server-generated UID, which
	// distinguishes incarnations of the same (atespace, name).
	ActorMetadataFieldUID ActorMetadataField = "uid"
)

// ActorMetadataItem projects one actor identity field to one file.
type ActorMetadataItem struct {
	// Field selects which identity field to project.
	//
	// +required
	Field ActorMetadataField `json:"field"`

	// Relative path from the root of the SystemInfo volume at which the
	// field's value is written. Must be a clean relative Unix path: it must
	// not start or end with '/' and must not contain ':', '//', '.' or '..'
	// segments, or control characters.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.endsWith('/') && !self.contains('//') && !self.contains(':') && !self.matches('[\\x00-\\x1f\\x7f]') && !self.matches('(^|/)[.][.]?(/|$)')",message="path must be a clean relative Unix path: it must not start or end with '/' and must not contain ':', '//', '.' or '..' segments, or control characters"
	Path string `json:"path"`
}

// ActorMetadataDataSource is a SystemInfo volume data source that projects the
// actor's identity fields (name, atespace, uid) to files, one per item —
// analogous to the Kubernetes downwardAPI volume. Values are written raw with
// no trailing newline, and are fixed for the actor's lifetime across
// suspend/resume/migration.
type ActorMetadataDataSource struct {
	// Items is the list of fields to project and the file path each is
	// written to.
	//
	// +required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, y.field == x.field))",message="items must not project the same field twice"
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, y.path == x.path))",message="items must not contain duplicate paths"
	Items []ActorMetadataItem `json:"items"`
}

// TrustBundleDataSource is a SystemInfo volume data source that projects the
// trust anchors of a named trust bundle to a single PEM file — inspired by
// the Kubernetes clusterTrustBundle projected volume source, but
// source-neutral: the name selects a bundle substrate knows how to fetch,
// and where it is fetched from is a substrate deployment concern, not part
// of this API (atelet enforces the supported set and resolves the backend).
//
// Supported names are allowlisted in atelet. Initially the only supported
// bundle is "egress-mitm.ate.dev" (the egress gateway CA bundle), resolved
// from the Kubernetes ClusterTrustBundle (certificates.k8s.io/v1beta1) that
// atecontroller derives from the egress-mitm-ca-pool; a configurable backend
// registry may widen this later.
//
// The bundle is resolved and sanitized on the node when the actor starts:
// atelet reads the backing object through a cluster-wide watch and keeps
// only CERTIFICATE PEM blocks, deduplicated and deliberately shuffled (order
// carries no meaning); the actor itself never talks to any bundle backend.
// Starting the actor fails if the named bundle is not on the allowlist, its
// backend is unavailable in this deployment, or the resolved bundle is
// missing, empty, or unparseable.
type TrustBundleDataSource struct {
	// Name of the trust bundle to project. Must be a bundle name supported
	// by this deployment (currently only "egress-mitm.ate.dev").
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Relative path from the root of the SystemInfo volume at which the PEM
	// bundle is written. Must be a clean relative Unix path: it must not
	// start or end with '/' and must not contain ':', '//', '.' or '..'
	// segments, or control characters.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	// +kubebuilder:validation:XValidation:rule="!self.startsWith('/') && !self.endsWith('/') && !self.contains('//') && !self.contains(':') && !self.matches('[\\x00-\\x1f\\x7f]') && !self.matches('(^|/)[.][.]?(/|$)')",message="path must be a clean relative Unix path: it must not start or end with '/' and must not contain ':', '//', '.' or '..' segments, or control characters"
	Path string `json:"path"`
}

// SystemInfoDataSource is a container allowing you to pick a particular
// SystemInfo data source.
//
// Exactly one member must be set.
//
// +kubebuilder:validation:ExactlyOneOf={actorMetadata,trustBundle}
type SystemInfoDataSource struct {
	ActorMetadata *ActorMetadataDataSource `json:"actorMetadata,omitempty"`

	TrustBundle *TrustBundleDataSource `json:"trustBundle,omitempty"`
}

// Represents a system information volume, which provides files containing
// substrate-generated per-actor data such as the actor's identity fields,
// projected trust bundles (and, in the future, identity JWTs and
// certificates).
type SystemInfoVolumeSource struct {
	// DataSources is the list of data sources to place within the SystemInfo
	// volume.
	//
	// At most one actorMetadata entry may appear, and file paths must be
	// unique across all entries (uniqueness within actorMetadata is enforced
	// on its items).
	//
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.filter(x, has(x.actorMetadata)).size() <= 1",message="dataSources must contain at most one actorMetadata entry"
	// +kubebuilder:validation:XValidation:rule="self.all(x, !has(x.trustBundle) || self.exists_one(y, has(y.trustBundle) && y.trustBundle.path == x.trustBundle.path))",message="dataSources must not contain duplicate paths"
	// +kubebuilder:validation:XValidation:rule="!self.exists(x, has(x.trustBundle) && self.exists(y, has(y.actorMetadata) && y.actorMetadata.items.exists(i, i.path == x.trustBundle.path)))",message="dataSources must not contain duplicate paths"
	DataSources []SystemInfoDataSource `json:"dataSources,omitempty"`
}

// Represents the source of a volume to mount.
// Exactly one of its members must be specified.
//
// When adding a new source type, list it in the ExactlyOneOf marker below.
//
// +kubebuilder:validation:ExactlyOneOf={durableDir,externalVolumeTemplate,image,systemInfo}
type VolumeSource struct {
	// durableDir represents a durable directory on rootfs that persists across
	// resumes and participates in snapshots.
	// +optional
	DurableDir *DurableDirVolumeSource `json:"durableDir,omitempty"`

	// image represents the contents of an OCI image, mounted read-only.
	// +optional
	Image *ImageVolumeSource `json:"image,omitempty"`

	// externalVolumeTemplate represents an external volume dynamically provisioned
	// for each actor. The volume only lives as long as the actor and is deleted
	// when the actor is deleted.
	// +optional
	ExternalVolumeTemplate *ExternalVolumeTemplate `json:"externalVolumeTemplate,omitempty"`

	// systemInfo configures a system information volume.
	//
	// +optional
	SystemInfo *SystemInfoVolumeSource `json:"systemInfo,omitempty"`
}

type Volume struct {
	// name of the volume.
	//
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Label().validate(self).hasValue()",message="Name must be a valid DNS label"
	Name string `json:"name"`

	// volumeSource represents the location and type of the mounted volume.
	VolumeSource `json:",inline"`
}

// VolumeMount describes a mounting of a Volume within a actor.
type VolumeMount struct {
	// This must match the Name of a Volume.
	//
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="!format.dns1123Label().validate(self).hasValue()",message="Name must be a valid DNS label"
	Name string `json:"name"`
	// Path within the actor at which the volume should be mounted. Must be a
	// clean absolute Unix path: must start with '/', not be '/', and contain
	// no ':', '..', '.', '//', trailing '/', or control characters.
	//
	// +required
	// +kubebuilder:validation:MaxLength=4096
	// +kubebuilder:validation:XValidation:rule="self.startsWith('/') && size(self) > 1 && !self.endsWith('/') && !self.contains('//') && !self.contains(':') && !self.matches('[\\x00-\\x1f\\x7f]') && !self.matches('(^|/)[.][.]?(/|$)')",message="MountPath must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters"
	MountPath string `json:"mountPath"`
}

// Capability is a Linux capability named without the "CAP_" prefix (e.g.
// "NET_BIND_SERVICE"), as in Kubernetes. The prefix is added when the OCI spec
// is written, and the prefixed spelling is rejected so a manifest copied from
// OCI docs fails at admission rather than granting nothing.
//
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^[A-Z][A-Z0-9_]*$`
// +kubebuilder:validation:XValidation:rule="!self.startsWith('CAP_')",message="Capability must be named without the 'CAP_' prefix (e.g. 'NET_BIND_SERVICE', not 'CAP_NET_BIND_SERVICE')"
type Capability string

// CapabilityAll drops every default capability when used in Capabilities.Drop.
const CapabilityAll Capability = "ALL"

// Capabilities adjusts a container's Linux capabilities relative to the default
// set. Drop applies first, then Add, so a capability in both is granted.
type Capabilities struct {
	// Add lists capabilities to grant on top of the default set.
	//
	// "ALL" is rejected: Kubernetes accepts it in the API and relies on
	// PodSecurity admission to deny it, and there is no equivalent policy layer
	// here yet.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	// +kubebuilder:validation:XValidation:rule="!self.exists(c, c == 'ALL')",message="add does not accept 'ALL'; name the individual capabilities the container needs"
	Add []Capability `json:"add,omitempty"`

	// Drop lists capabilities to remove from the default set. "ALL" drops the
	// whole set, so drop+add expresses an exact set rather than a relative one.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Drop []Capability `json:"drop,omitempty"`
}

// SecurityContext holds security settings for a container's process. It models
// a subset of the Kubernetes container securityContext.
type SecurityContext struct {
	// Capabilities adjusts this container's Linux capabilities relative to the
	// default set.
	//
	// +optional
	Capabilities *Capabilities `json:"capabilities,omitempty"`
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

	// Entrypoint array. Not executed within a shell. The container image's
	// ENTRYPOINT is used if this is not provided; if it is provided, the
	// image's ENTRYPOINT and CMD are both ignored and the process argv is
	// command + args.
	//
	// Unlike Kubernetes, variable references $(VAR_NAME) are NOT expanded.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Command []string `json:"command,omitempty"`

	// Arguments to the entrypoint. Not executed within a shell. The container
	// image's CMD is used if this is not provided (unless command is set,
	// which discards the image's CMD).
	//
	// Unlike Kubernetes, variable references $(VAR_NAME) are NOT expanded.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +listType=atomic
	Args []string `json:"args,omitempty"`

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

	// securityContext holds security settings for this container. Unset leaves
	// it with the default capability set.
	//
	// +optional
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`

	// Resources are the compute limits for this container, enforced inside the
	// actor's sandbox. Only cpu and memory are supported, and only on micro-VM
	// actors: gVisor applies cgroup limits at the sandbox level, so a
	// per-container cgroup there is created but stays empty.
	//
	// +optional
	Resources *ContainerResources `json:"resources,omitempty"`
}

// ContainerResources are the resource limits for one actor container.
//
// Only limits are expressible. A request is a scheduling hint, and scheduling
// happens at the pod level: the WorkerPool reserves capacity on a node, and
// per-container limits subdivide a budget that is already held.
//
// +kubebuilder:validation:XValidation:rule="!has(self.limits) || self.limits.all(k, k == 'cpu' || k == 'memory')",message="only cpu and memory limits are supported"
// +kubebuilder:validation:XValidation:rule="!has(self.limits) || !('memory' in self.limits) || quantity(string(self.limits['memory'])).isGreaterThan(quantity('0'))",message="memory limit must be greater than zero"
// +kubebuilder:validation:XValidation:rule="!has(self.limits) || !('cpu' in self.limits) || quantity(string(self.limits['cpu'])).isGreaterThan(quantity('0'))",message="cpu limit must be greater than zero"
// +kubebuilder:validation:XValidation:rule="!has(self.limits) || !('cpu' in self.limits) || quantity(string(self.limits['cpu'])).isLessThan(quantity('1k'))",message="cpu limit must be less than 1000 cores"
type ContainerResources struct {
	// Limits is the maximum amount of compute resources allowed. Only cpu and
	// memory are supported, and each must be greater than zero.
	//
	// A cpu limit below 10m is raised to 10m: the kernel rejects a CFS quota
	// under 1ms, and the quota is expressed against a 100ms period.
	//
	// +optional
	Limits ContainerResourceList `json:"limits,omitempty"`
}

// ContainerResourceList is the limits map for one actor container. It is a
// named type so the bound below applies to the map itself, which also keeps the
// CEL rules on ContainerResources inside the API server's cost budget: an
// unbounded map makes the estimator assume the worst case and reject the whole
// schema.
//
// +kubebuilder:validation:MaxProperties=2
type ContainerResourceList map[corev1.ResourceName]resource.Quantity

// ContainerReadyz configures the readiness signal for a container.
type ContainerReadyz struct {
	// HTTPGet specifies the HTTP request to perform against the container.
	//
	// +required
	HTTPGet *HTTPGetAction `json:"httpGet"`

	// TimeoutSeconds is how long to keep polling HTTPGet before giving up.
	// Exceeding it fails the actor start rather than proceeding with a
	// container that never reported ready.
	//
	// How long a workload takes to become ready is a property of that workload,
	// which is why this is set per template rather than cluster-wide: a heavy
	// runtime that needs minutes should not force every other template to wait
	// as long before its failures surface.
	//
	// Unset defaults to 30, applied by the API server so the effective value is
	// visible on the stored object rather than only in the ateom. A manifest
	// asking for 0 is rejected: unlike a warmup delay, a zero deadline could
	// never be met, so it is never what a template author means.
	//
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
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
// and envFrom and valueFrom are not supported.
type EnvVar struct {
	// Name is the name of the environment variable. May be any printable ASCII
	// character except '='.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^[ -<>-~]+$`
	Name string `json:"name"`

	// Value is the literal value of the environment variable. Unlike in
	// Kubernetes pods, this value is not interpolated, and $(VAR)
	// references are not expanded.
	//
	// +required
	// +kubebuilder:validation:MinLength=0
	Value string `json:"value"`
}

// SnapshotScope defines what components to include in a snapshot.
// +kubebuilder:validation:Enum=Full;Data
type SnapshotScope string

const (
	// Full captures process memory plus the entire filesystem delta on top of
	// the OCI image (including any attached DurableDir volumes).
	SnapshotScopeFull SnapshotScope = "Full"
	// Data captures only the contents of attached volumes that support
	// snapshots (currently DurableDir-typed volumes; external/CSI volumes
	// are not snapshotted as they persist independently). Process memory and
	// the rest of rootfs are excluded.
	SnapshotScopeData SnapshotScope = "Data"
)

// ResumeSource selects what supplies the guest state when an actor is brought
// back from one of the snapshot situations named by OnResumeConfig's fields.
// +kubebuilder:validation:Enum=ColdBoot;Golden
type ResumeSource string

const (
	// ResumeSourceColdBoot starts the actor's containers afresh from the OCI
	// image, with the durable-dir volumes pre-populated from the snapshot.
	ResumeSourceColdBoot ResumeSource = "ColdBoot"
	// ResumeSourceGolden restores the ActorTemplate's golden snapshot (guest
	// memory + filesystem delta) and serves the snapshot's durable data to
	// it, so the actor resumes with the golden's warm state over its own
	// data. Requires sandboxClass "microvm".
	ResumeSourceGolden ResumeSource = "Golden"
)

// OnResumeConfig selects, per snapshot situation, what supplies the guest
// state at resume. Each field names what is being resumed FROM; the value
// names the boot source. Full snapshots that are still valid always restore
// from their own content and are not configurable here.
type OnResumeConfig struct {
	// FromData applies when the resume uses a Data-scope snapshot (from
	// onPause or onCommit): "ColdBoot" starts fresh from the OCI image with
	// the durable data restored; "Golden" combines the durable data with the
	// template's golden snapshot. Defaults to "ColdBoot".
	//
	// +optional
	// +kubebuilder:default=ColdBoot
	FromData ResumeSource `json:"fromData,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="(has(self.onPause) ? self.onPause : 'Full') == 'Full' || (has(self.onCommit) ? self.onCommit : 'Full') == (has(self.onPause) ? self.onPause : 'Full')",message="onCommit must be a subset of onPause"
type SnapshotsConfig struct {
	// Location is the base object-storage URI snapshots of this template's
	// actors are stored under.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	Location string `json:"location"`

	// OnPause specifies what to include in the snapshot when the actor is paused.
	// If not provided, the "Full" behavior is used by default.
	// Note: Data scope only captures DurableDir-typed volumes; external/CSI
	// volumes are not snapshotted as they persist independently.
	//
	// +optional
	// +kubebuilder:default=Full
	OnPause SnapshotScope `json:"onPause,omitempty"`

	// OnCommit specifies what to include in the snapshot when a commit is requested.
	// If not provided, the "Full" behavior is used by default.
	// onCommit must be a subset of the onPause content.
	// Note: Data scope only captures DurableDir-typed volumes; external/CSI
	// volumes are not snapshotted as they persist independently.
	//
	// For example:
	//   - if onPause is "Full", then onCommit can be "Full" or "Data".
	//   - if onPause is "Data", then onCommit must be "Data".
	//
	// +optional
	// +kubebuilder:default=Full
	OnCommit SnapshotScope `json:"onCommit,omitempty"`

	// OnResume specifies, per snapshot situation, what supplies the guest
	// state at resume (see OnResumeConfig). "fromData: Golden" requires
	// sandboxClass "microvm".
	//
	// +optional
	// +kubebuilder:default={}
	OnResume OnResumeConfig `json:"onResume,omitempty"`
}

// ActorTemplateSpec defined desired spec of an actor.
//
// +kubebuilder:validation:XValidation:rule="!has(self.volumes) || self.volumes.all(v, has(self.containers) && self.containers.exists(c, has(c.volumeMounts) && c.volumeMounts.exists(vm, vm.name == v.name)))",message="All volumes defined in spec.volumes must be mounted by at least one container"
// +kubebuilder:validation:XValidation:rule="(has(self.sandboxClass) && self.sandboxClass == 'microvm') || !has(self.snapshotsConfig.onResume) || (has(self.snapshotsConfig.onResume.fromData) ? self.snapshotsConfig.onResume.fromData : 'ColdBoot') != 'Golden'",message="onResume.fromData: Golden is not supported when sandboxClass is 'gvisor'"
// +kubebuilder:validation:XValidation:rule="!has(self.resources) || !has(self.resources.requests)",message="spec.resources.requests is not supported; actors are sized by spec.resources.limits only"
// +kubebuilder:validation:XValidation:rule="!has(self.resources) || !has(self.resources.claims)",message="spec.resources.claims is not supported"
// A micro-VM's guest RAM is the declared memory limit minus a fixed VMM reserve
// (128Mi, held back for cloud-hypervisor + virtiofsd); below a 128Mi guest minimum
// there is no useful headroom. Reject at admission any micro-VM memory limit under
// 256Mi (128Mi reserve + 128Mi guest minimum) so it fails at create time rather than at
// cold boot — a coarse pre-filter; the reserve-aware check in ateom (see
// cmd/ateom-microvm/run.go: resolveGuestMemMiB) stays authoritative. The 256Mi floor
// assumes the default reserve; deployments that raise --vmm-mem-reserve-mib rely on
// the runtime check. gVisor has no reserve, so this only applies to micro-VM.
// +kubebuilder:validation:XValidation:rule="!has(self.sandboxClass) || self.sandboxClass != 'microvm' || !has(self.resources) || !has(self.resources.limits) || !('memory' in self.resources.limits) || !quantity(self.resources.limits['memory']).isLessThan(quantity('256Mi'))",message="For sandboxClass 'microvm', spec.resources.limits.memory must be at least 256Mi (128Mi VMM reserve + 128Mi guest minimum); below this the VM cannot boot"
// +kubebuilder:validation:XValidation:rule="!has(self.containers) || self.containers.all(c, !has(c.volumeMounts) || c.volumeMounts.all(vm, has(self.volumes) && self.volumes.exists(v, v.name == vm.name)))",message="All volume mounts must refer to a volume defined in spec.volumes"
// +kubebuilder:validation:XValidation:rule="!has(self.containers) || !self.containers.exists(c, has(c.resources)) || (has(self.sandboxClass) && self.sandboxClass == 'microvm')",message="container resources are only supported when sandboxClass is 'microvm'"
type ActorTemplateSpec struct {
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
	// +listType=map
	// +listMapKey=name
	Volumes []Volume `json:"volumes,omitempty"`

	// Resources declares the compute resources for each actor of this template.
	// Unlike a pod, an actor is sized by its Limits: the sandbox is built to the
	// CPU/memory limits (cgroup caps, and for the micro-VM the VM's vCPU count and
	// memory), the scheduler only places the actor on a worker whose capacity is
	// >= these limits, and the limits are supplied to the sandbox over the actor
	// RPCs. Because the size is baked into snapshots, it is part of the immutable
	// spec. Requests and claims are not supported (actors are sized by limits only).
	// A zero or absent limit leaves the sandbox at the runtime default (unlimited
	// for gVisor, the kata config for the micro-VM).
	//
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
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
