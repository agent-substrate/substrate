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

// AuthBindingSource names the agent a binding applies to.
type AuthBindingSource struct {
	// identity is the agent's SPIFFE ID, in one of two forms. Kubernetes
	// workloads use spiffe://<trust-domain>/ns/<namespace>/sa/<name>; the
	// namespace segment must equal the binding's namespace. The API server
	// enforces that through the ValidatingAdmissionPolicy in
	// manifests/ate-install/authbinding-validation.yaml, and Auth Manager
	// checks it again. Substrate actors use
	// spiffe://<trust-domain>/worker-for-actor/<atespace>/<name>, which
	// carries no Kubernetes namespace and so is not checked against the
	// binding's namespace yet.
	//
	// TODO: agree with the Substrate and Prism owners how an actor
	// identity is tied to a namespace, and enforce it here and in the
	// policy. Both forms are placeholders until bindings name agents by
	// their registry URN.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^spiffe://[^/]+/(ns/[^/]+/sa|worker-for-actor/[^/]+)/[^/]+$`
	Identity string `json:"identity,omitempty"`
}

// AuthBindingTarget names the destination a binding applies to.
type AuthBindingTarget struct {
	// hostname is the destination's DNS name: lowercase, no trailing dot,
	// not an IP address. A leading "*." matches exactly one label, so
	// "*.example.com" matches "api.example.com" but neither "example.com"
	// nor "a.b.example.com". "*" alone is not allowed.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^(\*\.)?([a-z0-9]([-a-z0-9]*[a-z0-9])?\.)*[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:XValidation:rule="!self.matches('(^|[.])[0-9]+$')",message="hostname must not be an IP address: its last label must not be all digits"
	Hostname string `json:"hostname,omitempty"`

	// ports are the destination ports the binding covers; a request to any
	// other port does not match. Defaults to [443]. There is no "all ports"
	// option: a credential is only sent to ports named here.
	//
	// TODO: add an "all ports" option only if a use case needs it; it
	// widens where a credential may be sent.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=1
	// +kubebuilder:validation:items:Maximum=65535
	// +kubebuilder:default={443}
	Ports []int32 `json:"ports,omitempty"`
}

// SecretKeyRef names one key of a Secret in the binding's namespace. The
// Secret must have type agentplatform.io/credential.
type SecretKeyRef struct {
	// name is the Secret's name.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	Name string `json:"name,omitempty"`

	// key is the entry in the Secret's data map whose value becomes the
	// header value.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	Key string `json:"key,omitempty"`
}

// AuthBindingCredential is one header Auth Manager sets on a matching
// request, from a stored secret.
type AuthBindingCredential struct {
	// header is the HTTP header to set. Any value the agent sent in it is
	// replaced. Pseudo-headers, names starting with x-envoy, and the
	// connection-level headers host, transfer-encoding, content-length,
	// connection, te and upgrade are not allowed.
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9!#$%&'*+.^_|~-]+$`
	// +kubebuilder:validation:XValidation:rule="!self.lowerAscii().startsWith('x-envoy')",message="header must not start with x-envoy"
	// +kubebuilder:validation:XValidation:rule="!(self.lowerAscii() in ['host', 'transfer-encoding', 'content-length', 'connection', 'te', 'upgrade'])",message="header must not be host, transfer-encoding, content-length, connection, te or upgrade"
	Header string `json:"header,omitempty"`

	// prefix is prepended verbatim to the secret value, separator included,
	// for example "Bearer ". Printable ASCII only.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[ -~]*$`
	Prefix *string `json:"prefix,omitempty"`

	// secretRef names the Secret key holding the value. The Secret is in
	// the binding's namespace.
	//
	// +required
	SecretRef SecretKeyRef `json:"secretRef,omitzero"`
}

// AuthBindingSpec defines the desired state of AuthBinding.
type AuthBindingSpec struct {
	// source is the agent the binding applies to.
	//
	// +required
	Source AuthBindingSource `json:"source,omitzero"`

	// target is the destination the binding applies to.
	//
	// +required
	Target AuthBindingTarget `json:"target,omitzero"`

	// credentials are the headers to set, 1 to 16, with header names unique
	// case-insensitively.
	//
	// +required
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:XValidation:rule="self.all(a, self.filter(b, b.header.lowerAscii() == a.header.lowerAscii()).size() == 1)",message="credentials header names must be unique, ignoring case"
	Credentials []AuthBindingCredential `json:"credentials,omitempty"`
}

// AuthBinding allows one agent to call one destination and names the
// credentials Auth Manager adds to those requests.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=authbinding
// +kubebuilder:printcolumn:name="Identity",type=string,JSONPath=`.spec.source.identity`
// +kubebuilder:printcolumn:name="Hostname",type=string,JSONPath=`.spec.target.hostname`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AuthBinding struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of AuthBinding
	// +required
	Spec AuthBindingSpec `json:"spec,omitzero"`
}

// AuthBindingList contains a list of AuthBindings.
// +kubebuilder:object:generate=true
// +kubebuilder:object:root=true
type AuthBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AuthBinding `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AuthBinding{}, &AuthBindingList{})
}
