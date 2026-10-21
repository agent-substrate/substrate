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

	"go.ate.dev/substrate/pkg/sandboxclass"
)

// SandboxClass selects the sandbox runtime family. It is shared by WorkerPool
// (which family a pool runs) and SandboxConfig (which family a config is for).
type SandboxClass string

const (
	// SandboxClassGvisor is the gVisor/runsc runtime (cmd/ateworker-gvisor). Default.
	SandboxClassGvisor SandboxClass = sandboxclass.GVisor
	// SandboxClassMicroVM is the micro-VM runtime (cmd/ateworker-microvm); needs
	// /dev/kvm and vhost devices.
	SandboxClassMicroVM SandboxClass = sandboxclass.MicroVM
)

// AssetFile is one content-addressed file that atelet fetches for a sandbox
// runtime (e.g. the gVisor runsc binary, or a micro-VM kernel/firmware/config).
type AssetFile struct {
	// url is where to download the asset from (e.g. a gs:// URL). atelet
	// fetches every URL through the node's object-store plugin, with the
	// plugin's credentials. The plugin refuses URLs under a snapshot location.
	// The in-tree plugin serves only its storage backend's scheme: gs:// on
	// GCS, s3:// on S3. A gs:// URL the plugin has no object for or cannot
	// serve is then read anonymously, which serves public buckets such as
	// gs://gvisor.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	URL string `json:"url"`

	// sha256 is the lower-case hex SHA256 of the asset. It both names the cached
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

// SandboxConfigWorkerPoolProtectionFinalizer is put on a SandboxConfig by
// atecontroller before any WorkerPool's status records it. It stays until the
// config is deleted. Deleting a config that carries it goes in three steps:
//
//  1. The delete only sets deletionTimestamp, because the finalizer list is
//     not empty. The config stays, Terminating.
//  2. atecontroller checks whether any WorkerPool's status.sandboxClasses
//     names the config, in configRef or pendingConfigRef. If one does, it
//     leaves the finalizer and checks again whenever a WorkerPool changes.
//     Once none does, it removes the finalizer.
//  3. With the finalizer list empty, the API server deletes the config.
//
// Without atecontroller, step 2 never happens and the config stays
// Terminating until someone clears the finalizer by hand. A config no pool
// has ever used carries no finalizer and deletes at once.
//
// A pool that switches to a config names it in pendingConfigRef first. Then
// atecontroller reads the config from the API server, and moves configRef to
// it only if that read finds it is not being deleted. So a config is never
// released while a pool's configRef names it.
const SandboxConfigWorkerPoolProtectionFinalizer = "sandboxconfig.ate.dev/workerpool-protection"

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

// SandboxVersionConfig is one version of a SandboxConfig: the asset set a
// sandbox boots with.
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

	// assets is the set of files atelet fetches for this runtime, keyed first by
	// architecture (GOARCH, e.g. "amd64", "arm64") and then by asset name. The
	// asset names are interpreted by the sandbox backend: gVisor expects a
	// "gvisor" asset (the release's gvisor.tar.zstd, which atelet extracts so
	// the gvisor-bin/ helpers sit next to runsc); a micro-VM backend expects
	// several (e.g. "cloud-hypervisor", "kata-kernel", "kata-image"). The
	// schema is intentionally generic; per-class requirements are enforced by
	// a ValidatingAdmissionPolicy.
	//
	// +optional
	Assets map[string]map[string]AssetFile `json:"assets,omitempty"`
}

// SandboxConfigSpec is the desired state of a SandboxConfig.
//
// +kubebuilder:validation:XValidation:rule="self.versions.exists(v, v.name == self.defaultVersion)",message="defaultVersion must name an entry in versions"
// +kubebuilder:validation:XValidation:rule="!self.versions.exists(v, v.name == self.defaultVersion && has(v.state) && v.state == 'Disabled')",message="the version named by defaultVersion must not be Disabled"
type SandboxConfigSpec struct {
	// sandboxClass is the sandbox runtime family this config applies to. A
	// WorkerPool only uses SandboxConfigs whose sandboxClass matches the entry
	// in its spec.sandboxClasses.
	//
	// +optional
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass,omitempty"`

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
// sandbox binaries for a sandbox runtime family. It is referenced by a
// WorkerPool's spec.sandboxClasses[].configRef (or resolved via the
// sandboxconfig.ate.dev/is-class-default annotation) and decouples sandbox
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
