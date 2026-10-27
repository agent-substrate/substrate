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

package e2e

import (
	"os"
	"testing"
)

// GCSRequiredEnv makes a suite that needs GCS fail, rather than skip, on an
// install that stores snapshots elsewhere. A GCS e2e lane sets it, so a lane
// misconfigured as S3 cannot pass by running nothing.
const GCSRequiredEnv = "E2E_REQUIRE_GCS"

// GCSRequired reports whether the lane declared itself a GCS lane.
func GCSRequired() bool { return os.Getenv(GCSRequiredEnv) == "true" }

// RequireGCSBackend skips t unless backend, the install's snapshot backend
// as the objectstore-plugin reports it, is GCS. Under GCSRequired it fails
// instead.
func RequireGCSBackend(t testing.TB, backend string) {
	t.Helper()
	skip, fail := requireGCSDecision(backend, GCSRequired())
	if fail != "" {
		t.Fatal(fail)
	}
	if skip != "" {
		t.Skip(skip)
	}
}

// requireGCSDecision is RequireGCSBackend's rule, apart from testing.TB so
// its strict branch is proven without a cluster.
func requireGCSDecision(backend string, required bool) (skip, fail string) {
	if backend == "gcs" {
		return "", ""
	}
	if required {
		return "", "snapshot backend is " + backend + " and GCS is required (" + GCSRequiredEnv + " is set)"
	}
	return "snapshot backend is " + backend + ", this suite needs GCS", ""
}
