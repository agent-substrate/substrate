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

import "strings"

// IsDeviceResource reports whether a Kubernetes resource name denotes an
// extended-resource device (for example nvidia.com/gpu) rather than a native
// resource. The native resources that can appear on a container's limits (cpu,
// memory, ephemeral-storage, hugepages-*) are unprefixed, while every extended
// resource is domain-qualified, so a "/" in the name is what marks a device.
//
// Shared so the worker-capacity and actor-limit sides classify identically.
func IsDeviceResource(name string) bool {
	return strings.Contains(name, "/")
}
