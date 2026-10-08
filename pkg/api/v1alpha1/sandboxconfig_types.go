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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxClass selects the sandbox runtime family. It is shared by WorkerPool
// (which family a pool runs) and SandboxConfig (which family a config is for).
type SandboxClass string

const (
	// SandboxClassGvisor is the gVisor/runsc runtime (cmd/ateom-gvisor). Default.
	SandboxClassGvisor SandboxClass = "gvisor"
	// SandboxClassMicroVM is the micro-VM runtime (cmd/ateom-microvm); needs
	// /dev/kvm and vhost devices.
	SandboxClassMicroVM SandboxClass = "microvm"
)

// AssetFile is one content-addressed file that atelet fetches for a sandbox
// runtime (e.g. the gVisor runsc binary, or a micro-VM kernel/firmware/config).
type AssetFile struct {
	// URL is where to download the asset from (e.g. a gs:// URL). It may be
	// fetched anonymously or with credentials depending on atelet's
	// configuration.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`

	// SHA256 is the lower-case hex SHA256 of the asset. It both names the cached
	// file (preventing collisions) and verifies the download's integrity.
	//
	// +required
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	SHA256 string `json:"sha256"`
}

// SandboxConfigClassDefaultAnnotation marks a SandboxConfig as a default for its
// sandboxClass when set to "true". More than one SandboxConfig of the same
// class may carry it; the newest one wins.
const SandboxConfigClassDefaultAnnotation = "sandboxconfig.ate.dev/is-class-default"

// SandboxVersionState is whether a SandboxConfig version may be used.
//
// +kubebuilder:validation:Enum=Enabled;Disabled
type SandboxVersionState string

const (
	// SandboxVersionStateEnabled marks a version as usable. Default.
	SandboxVersionStateEnabled SandboxVersionState = "Enabled"
	// SandboxVersionStateDisabled marks a version as unusable. The version
	// named by defaultVersion cannot be disabled.
	SandboxVersionStateDisabled SandboxVersionState = "Disabled"
)

// SandboxVersionConfig is one version of a SandboxConfig: the exact pause image
// and asset set a sandbox boots with.
type SandboxVersionConfig struct {
	// name identifies this version within its SandboxConfig. It is a DNS label
	// (lower-case alphanumerics and '-').
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name,omitempty"`

	// state is whether this version may be used: Enabled (default) or
	// Disabled.
	//
	// +optional
	// +kubebuilder:default=Enabled
	State *SandboxVersionState `json:"state,omitempty"`

	// TODO: drop PauseImage once gVisor can run without a pause container:
	// https://github.com/google/gvisor/pull/13981

	// PauseImage is the container image used as the root sandbox container.
	// It holds the sandbox's namespaces and runs no workload code, so it is an
	// implementation detail of the sandbox rather than something actor authors
	// choose. Required for gvisor; not allowed for microvm, which runs no pause
	// container. It is captured in the snapshot manifest alongside the sandbox
	// binaries, so a restore always re-creates the sandbox from the same image
	// the snapshot was taken with.
	//
	// Typically, set it to [1] for on-gcp, and [2] for off-gcp
	//
	//   - [1] gcr.io/gke-release/pause@sha256:bcbd57ba5653580ec647b16d8163cdd1112df3609129b01f912a8032e48265da
	//   - [2] registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.contains('@')",message="All images must include a digest"
	PauseImage string `json:"pauseImage,omitempty"`

	// Assets is the set of files atelet fetches for this runtime, keyed first by
	// architecture (GOARCH, e.g. "amd64", "arm64") and then by asset name. The
	// asset names are interpreted by the sandbox backend: gVisor expects a
	// "gvisor" asset (the release's gvisor.tar.zstd, which atelet extracts so
	// the gvisor-bin/ helpers sit next to runsc; a legacy bare-binary "runsc"
	// asset is still accepted); a micro-VM backend expects several (e.g.
	// "cloud-hypervisor", "kata-kernel", "kata-image"). The schema is
	// intentionally generic; per-class requirements are enforced by a
	// ValidatingAdmissionPolicy.
	//
	// +optional
	Assets map[string]map[string]AssetFile `json:"assets,omitempty"`
}

// SandboxConfigSpec is the desired state of a SandboxConfig.
//
// +kubebuilder:validation:XValidation:rule="self.versions.exists(v, v.name == self.defaultVersion)",message="defaultVersion must name an entry in versions"
// +kubebuilder:validation:XValidation:rule="!self.versions.exists(v, v.name == self.defaultVersion && has(v.state) && v.state == 'Disabled')",message="the version named by defaultVersion must not be Disabled"
// +kubebuilder:validation:XValidation:rule="self.sandboxClass == 'gvisor' ? self.versions.all(v, has(v.pauseImage) && size(v.pauseImage) > 0) : self.versions.all(v, !has(v.pauseImage) || size(v.pauseImage) == 0)",message="pauseImage is required on every version for gvisor and not allowed for other sandbox classes"
type SandboxConfigSpec struct {
	// SandboxClass is the sandbox runtime family this config applies to. An
	// ActorTemplate only uses SandboxConfigs whose SandboxClass matches its
	// sandbox_config.sandbox_class.
	//
	// +required
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass"`

	// defaultVersion names the entry in versions that new sandboxes boot with.
	// It must name an existing, non-Disabled version.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	DefaultVersion string `json:"defaultVersion,omitempty"`

	// versions are the sandbox versions this config offers, keyed by name.
	//
	// +required
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	Versions []SandboxVersionConfig `json:"versions,omitempty"`
}

// DefaultVersionConfig returns the entry of Versions named by DefaultVersion,
// and whether one exists. A Disabled entry is never returned: the CRD rejects
// a Disabled default, so a spec that reads back that way is treated as having
// no default version.
func (s *SandboxConfigSpec) DefaultVersionConfig() (*SandboxVersionConfig, bool) {
	for i := range s.Versions {
		if s.Versions[i].Name != s.DefaultVersion {
			continue
		}
		if st := s.Versions[i].State; st != nil && *st == SandboxVersionStateDisabled {
			return nil, false
		}
		return &s.Versions[i], true
	}
	return nil, false
}

// SandboxConfig is cluster-scoped configuration describing the versions of the
// sandbox binaries for a sandbox runtime family. It is referenced by an
// ActorTemplate's sandbox_config.config_name (required) and decouples sandbox
// binary selection from the workload definition.
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sandboxconfig
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.sandboxClass`
// +kubebuilder:printcolumn:name="Default",type=string,JSONPath=`.spec.defaultVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type SandboxConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of SandboxConfig
	// +required
	Spec SandboxConfigSpec `json:"spec"`
}

// SandboxConfigList contains a list of SandboxConfigs.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
type SandboxConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxConfig{}, &SandboxConfigList{})
}
