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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EgressGatewaySpec identifies an administrator-managed egress enforcement point.
type EgressGatewaySpec struct {
	// address is the host:port that actor tunnels dial. The gateway must serve
	// mTLS CONNECT with a certificate valid for this host under the worker's
	// egress trust bundle.
	// +required
	// +kubebuilder:validation:MinLength=3
	// +kubebuilder:validation:MaxLength=261
	// +kubebuilder:validation:Pattern=`^([^: /]+|\[[0-9a-fA-F:]+\]):[0-9]{1,5}$`
	Address string `json:"address,omitempty"`
	// identity is the mTLS SPIFFE identity allowed to read actor egress policy
	// and mint credentials for actors assigned to this gateway. It does not
	// replace the tunnel's DNS server-certificate verification.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^spiffe://[^/?#]+/[^?#]+$`
	Identity string `json:"identity,omitempty"`
}

// EgressGateway is a cluster-scoped egress enforcement point. Only trusted
// administrators should have Kubernetes write access to these resources.
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Address",type=string,JSONPath=`.spec.address`
type EgressGateway struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// spec configures the gateway address and client identity.
	// +required
	Spec EgressGatewaySpec `json:"spec,omitzero"`
}

// EgressGatewayList contains egress gateways.
// +kubebuilder:object:root=true
type EgressGatewayList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EgressGateway `json:"items"`
}

func init() { SchemeBuilder.Register(&EgressGateway{}, &EgressGatewayList{}) }
