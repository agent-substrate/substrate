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

package objectstore

import "github.com/agent-substrate/substrate/internal/env"

// BackendEnv selects the snapshot backend for ateapi and atelet.
var BackendEnv = env.Var[string]{
	Name:    "ATE_STORAGE_BACKEND",
	Default: "gcs",
	Description: `Selects the snapshot storage backend. The exact, case-sensitive value s3 selects S3.
The default is gcs. Empty and unrecognized values also select GCS.`,
}

// S3PathStyleEnv controls S3 addressing for ateapi and atelet.
var S3PathStyleEnv = env.Var[bool]{
	Name:    "AWS_S3_USE_PATH_STYLE",
	Default: false,
	Description: `Enables S3 path-style addressing when ATE_STORAGE_BACKEND=s3.
Accepted true values: 1, t, T, TRUE, true, True. Accepted false values: 0, f, F, FALSE, false, False.
Unset, empty, or invalid values use false. Whitespace is not trimmed.`,
}
