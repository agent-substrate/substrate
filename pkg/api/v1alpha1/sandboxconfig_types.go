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

// SandboxConfigSpec is the desired state of a SandboxConfig.
type SandboxConfigSpec struct {
	// SandboxClass is the sandbox runtime family this config applies to. A
	// WorkerPool only uses SandboxConfigs whose SandboxClass matches its own.
	//
	// +required
	// +kubebuilder:validation:Enum=gvisor;microvm
	// +kubebuilder:default=gvisor
	SandboxClass SandboxClass `json:"sandboxClass"`

	// Default marks this SandboxConfig as the cluster-wide default for its
	// SandboxClass. A WorkerPool with no explicit SandboxConfigName resolves to
	// the default config for its SandboxClass. At most one default is expected
	// per SandboxClass.
	//
	// +optional
	Default bool `json:"default,omitempty"`

	// Assets is the set of files atelet fetches for this runtime, keyed first by
	// architecture (GOARCH, e.g. "amd64", "arm64") and then by asset name. The
	// asset names are interpreted by the sandbox backend: gVisor expects a
	// "runsc" asset; a micro-VM backend expects several (e.g. "cloud-hypervisor",
	// "kata-kernel", "kata-image"). The schema is intentionally generic;
	// per-class requirements are enforced by a ValidatingAdmissionPolicy.
	//
	// +optional
	Assets map[string]map[string]AssetFile `json:"assets,omitempty"`
}

// SandboxConfig is cluster-scoped configuration describing the sandbox binaries
// for a sandbox runtime family. It is referenced (or defaulted) by WorkerPools
// and decouples sandbox binary selection from ActorTemplate.
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=sandboxconfig
// +kubebuilder:printcolumn:name="Class",type=string,JSONPath=`.spec.sandboxClass`
// +kubebuilder:printcolumn:name="Default",type=boolean,JSONPath=`.spec.default`
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
