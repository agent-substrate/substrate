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

// NetworkPolicyName returns the NetworkPolicy name for a WorkerPool. The
// WorkerPool name is a label value (at most 63 characters), so the result is
// unique per pool and a valid name without truncation.
func NetworkPolicyName(wpName string) string {
	return "substrate-" + wpName
}
