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

package resources

// NetworkPolicyName returns the NetworkPolicy name for a WorkerPool. It keeps
// the whole pool name, so each pool gets its own policy. The policy also uses
// the pool name as its ate.dev/worker-pool label value, which Kubernetes caps
// at 63 characters, so the result never needs truncating.
func NetworkPolicyName(wpName string) string {
	return "substrate-" + wpName
}
