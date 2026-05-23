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

package podcertificate

import (
	"crypto"
	"crypto/x509"
	"fmt"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
)

// PublicKey extracts the subject public key from a PodCertificateRequest.
func PublicKey(pcr *certsv1beta1.PodCertificateRequest) (crypto.PublicKey, error) {
	if len(pcr.Spec.StubPKCS10Request) > 0 {
		csr, err := x509.ParseCertificateRequest(pcr.Spec.StubPKCS10Request)
		if err != nil {
			return nil, fmt.Errorf("while parsing stub PKCS#10 request: %w", err)
		}
		return csr.PublicKey, nil
	}

	if len(pcr.Spec.PKIXPublicKey) > 0 { //nolint:staticcheck // SA1019: PKIXPublicKey kept for transition alongside StubPKCS10Request.
		subjectPublicKey, err := x509.ParsePKIXPublicKey(pcr.Spec.PKIXPublicKey) //nolint:staticcheck // SA1019: same as above.
		if err != nil {
			return nil, fmt.Errorf("while parsing PKIX public key: %w", err)
		}
		return subjectPublicKey, nil
	}

	return nil, fmt.Errorf("pod certificate request does not contain a public key")
}
