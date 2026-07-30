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

const (
	// ResourceNameRegexPattern is the regular expression pattern for a valid
	// Substrate resource name.
	ResourceNameRegexPattern = `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
	// ActorDNSSuffix is suffix to the DNS name for direct access to Actor
	// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev"
	ActorDNSSuffix = "actors.resources.substrate.ate.dev"
	// GoldenActorAtespace is the reserved system atespace that per-template golden
	// actors live in.
	GoldenActorAtespace = "ate-golden"
)
