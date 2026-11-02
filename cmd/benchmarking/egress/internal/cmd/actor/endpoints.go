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

import "fmt"

// TargetNamespace holds the endpoint Services the benchmark deploys.
const TargetNamespace = "egress-tests-targets"

// MaxEndpoints is the most endpoints a run can use.
const MaxEndpoints = 1024

// ServiceName is the name of endpoint i's Service.
func ServiceName(i int) string {
	return fmt.Sprintf("egress-target-%d", i)
}

// EndpointDomain is the DNS domain every endpoint name sits directly under.
const EndpointDomain = TargetNamespace + ".svc.cluster.local"

// EndpointHostPattern matches every endpoint name and nothing below them: a
// wildcard stands for exactly one leftmost label. The target's certificate is
// issued for it.
const EndpointHostPattern = "*." + EndpointDomain

// EndpointHost is the DNS name of endpoint i. It is the one place the
// endpoint names are defined: the actor's loop calls these names and the
// driver's egress policy allows them.
func EndpointHost(i int) string {
	return ServiceName(i) + "." + EndpointDomain
}

// EndpointURL is the URL the actor's loop requests for endpoint i over scheme,
// http or https, each on its default port.
func EndpointURL(i int, scheme string) string {
	return scheme + "://" + EndpointHost(i) + "/"
}
