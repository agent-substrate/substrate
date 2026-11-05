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
	"fmt"
	"time"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
)

const (
	maxLifetime = 24 * time.Hour
	minLifetime = time.Hour
)

// Lifetime returns the certificate lifetime for pcr: 24 hours, or less if the
// request asks for less. A request for under an hour is an error.
func Lifetime(pcr *certsv1beta1.PodCertificateRequest) (time.Duration, error) {
	if pcr.Spec.MaxExpirationSeconds == nil {
		return maxLifetime, nil
	}
	requested := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
	if requested < minLifetime {
		return 0, fmt.Errorf("requested certificate lifetime must be at least %v", minLifetime)
	}
	return min(requested, maxLifetime), nil
}
