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

// Package substratex509 contains routines for creating and parsing x509
// certificates that embed Substrate-specific X.509 extensions communicating
// the identity of a given workload. It is modeled on the upstream Kubernetes
// component-helpers/kubernetesx509 package, but encodes extension values as
// JSON instead of ASN.1.
package substratex509

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/json"
	"fmt"
	"strings"
)

var (
	// GoogleSubstratePEN is the ASN.1 Private Enterprise Number arc used to
	// name X.509 extensions that communicate Substrate-specific concepts.
	GoogleSubstratePEN = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 11129, 2, 12}

	// oidPodIdentity identifies the Kubernetes PodIdentity X.509 extension specifically in substrate.
	oidPodIdentity = makeSubstrateOID(1)
)

func makeSubstrateOID(subIDs ...int) asn1.ObjectIdentifier {
	base := asn1.ObjectIdentifier{}
	base = append(base, GoogleSubstratePEN...)
	base = append(base, subIDs...)
	return base
}

// PodIdentity is the Kubernetes Pod Identity of a pod, as embedded in the
// oidPodIdentity extension of its certificate.
type PodIdentity struct {
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
	PodName            string
	PodUID             string
	NodeName           string
	NodeUID            string
}

func AddPodIdentityToCertificate(pod *PodIdentity, template *x509.Certificate) error {
	if err := validatePodIdentity(pod); err != nil {
		return fmt.Errorf("while validating PodIdentity input: %w", err)
	}
	podIdentityBytes, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("while json-marshaling PodIdentity extension: %w", err)
	}

	template.ExtraExtensions = append(template.ExtraExtensions, pkix.Extension{
		Id:    oidPodIdentity,
		Value: podIdentityBytes,
	})

	return nil
}

func PodIdentityFromCertificate(cert *x509.Certificate) (*PodIdentity, error) {
	podIdentityCount := 0

	var podIdentityValue []byte
	for _, ext := range cert.Extensions {
		if ext.Id.Equal(oidPodIdentity) {
			podIdentityCount++
			podIdentityValue = ext.Value
		}
	}

	if podIdentityCount == 0 {
		return nil, nil
	}
	if podIdentityCount > 1 {
		return nil, fmt.Errorf("certificate contains multiple PodIdentity extensions")
	}

	pod := &PodIdentity{}
	if err := json.Unmarshal(podIdentityValue, pod); err != nil {
		return nil, fmt.Errorf("while json-unmarshaling PodIdentity extension: %w", err)
	}

	if err := validatePodIdentity(pod); err != nil {
		return nil, fmt.Errorf("while validating PodIdentity extension: %w", err)
	}

	return pod, nil
}

func validatePodIdentity(pod *PodIdentity) error {
	var empty []string
	if pod.Namespace == "" {
		empty = append(empty, "Namespace")
	}
	if pod.ServiceAccountName == "" {
		empty = append(empty, "ServiceAccountName")
	}
	if pod.ServiceAccountUID == "" {
		empty = append(empty, "ServiceAccountUID")
	}
	if pod.PodName == "" {
		empty = append(empty, "PodName")
	}
	if pod.PodUID == "" {
		empty = append(empty, "PodUID")
	}
	if pod.NodeName == "" {
		empty = append(empty, "NodeName")
	}
	if pod.NodeUID == "" {
		empty = append(empty, "NodeUID")
	}
	if len(empty) > 0 {
		return fmt.Errorf("empty fields: %s", strings.Join(empty, ", "))
	}
	return nil
}
