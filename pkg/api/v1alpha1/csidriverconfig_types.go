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

// CSIDriverConfigSpec defines the desired state of CSIDriverConfig
type CSIDriverConfigSpec struct {
	// DriverName is the standard CSI driver name (e.g. "hostpath.csi.k8s.io").
	// Matches the StorageClass referenced in ActorTemplate volume definitions.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^(substrate\.io/)?([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)$`
	DriverName string `json:"driverName"`

	// ControllerEndpoint is the gRPC endpoint for the CSI Controller service.
	// Must be a valid network URI (e.g. dns:///csi-service:9000 or tcp://127.0.0.1:9000).
	// TODO: Harden endpoint validation to prevent invalid or unsafe URI inputs.
	//
	// +required
	// +kubebuilder:validation:Pattern=`^(tcp|dns)://.+$`
	ControllerEndpoint string `json:"controllerEndpoint"`

	// NodeSocketOverride is an optional override for the CSI Node service socket
	// on the worker nodes. If empty, ATE defaults to unix:///var/lib/kubelet/plugins/[DriverName]/csi.sock.
	//
	// +optional
	// +kubebuilder:validation:Pattern=`^unix://.+$`
	NodeSocketOverride string `json:"nodeSocketOverride,omitempty"`

	// TLS configures TLS/mTLS for the connection to the ControllerEndpoint.
	// +optional
	TLS *CSIDriverTLSConfig `json:"tls,omitempty"`
}

// SecretReference references a Secret in a specific namespace.
type SecretReference struct {
	// Name of the referent Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the referent Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
}

// CSIDriverTLSConfig holds TLS and mTLS configuration for CSI driver connections.
type CSIDriverTLSConfig struct {
	// Enabled controls whether TLS is used.
	// +required
	Enabled bool `json:"enabled"`

	// CACertSecretRef references a Secret containing the CA certificate to verify the server.
	// The Secret must contain a key named "ca.crt".
	// +optional
	CACertSecretRef *SecretReference `json:"caCertSecretRef,omitempty"`

	// ClientCertSecretRef references a Secret containing the client certificate and key for mTLS.
	// The Secret must contain keys named "tls.crt" and "tls.key".
	// +optional
	ClientCertSecretRef *SecretReference `json:"clientCertSecretRef,omitempty"`

	// ServerName override for TLS verification.
	// +optional
	ServerName string `json:"serverName,omitempty"`
}


// CSIDriverConfig is the Schema for the csidriverconfigs API
//
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=csidriverconfig
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=`.spec.driverName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CSIDriverConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec CSIDriverConfigSpec `json:"spec"`
}

// CSIDriverConfigList contains a list of CSIDriverConfigs
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
type CSIDriverConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CSIDriverConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CSIDriverConfig{}, &CSIDriverConfigList{})
}
