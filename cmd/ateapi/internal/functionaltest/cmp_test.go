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

package functionaltest

import (
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// cmp options, used with protocmp.Transform, that skip the resource metadata
// ate-api assigns.
var (
	// ignoreVersion and ignoreTimestamps skip what ate-api changes on every
	// write, for comparing a resource before and after an operation.
	ignoreVersion    = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "version")
	ignoreTimestamps = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "create_time", "update_time")
	// ignoreDeleteTime skips the time ate-api stamps on a deleted resource.
	ignoreDeleteTime = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "delete_time")
	// ignoreServerMetadata also skips the uid, for comparing a resource
	// against a literal written before ate-api created it.
	ignoreServerMetadata = protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time")
)

// foreignUID is a valid UID no resource has, for preconditions that must miss.
const foreignUID = "0f0e0d0c-0b0a-4908-8706-050403020100"
