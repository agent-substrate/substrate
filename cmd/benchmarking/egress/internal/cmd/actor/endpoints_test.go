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

package actor

import "testing"

func TestEndpointNames(t *testing.T) {
	t.Parallel()
	if got, want := ServiceName(7), "egress-target-7"; got != want {
		t.Errorf("ServiceName(7) = %q, want %q", got, want)
	}
	if got, want := EndpointHost(7), "egress-target-7.egress-tests-targets.svc.cluster.local"; got != want {
		t.Errorf("EndpointHost(7) = %q, want %q", got, want)
	}
	if got, want := EndpointURL(7, SchemeHTTP), "http://egress-target-7.egress-tests-targets.svc.cluster.local/"; got != want {
		t.Errorf("EndpointURL(7, http) = %q, want %q", got, want)
	}
	if got, want := EndpointURL(7, SchemeHTTPS), "https://egress-target-7.egress-tests-targets.svc.cluster.local/"; got != want {
		t.Errorf("EndpointURL(7, https) = %q, want %q", got, want)
	}
	if got, want := EndpointHostPattern, "*.egress-tests-targets.svc.cluster.local"; got != want {
		t.Errorf("EndpointHostPattern = %q, want %q", got, want)
	}
}
