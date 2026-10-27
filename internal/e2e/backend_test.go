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

import "testing"

// recordingTB captures what RequireGCSBackend does to the test.
type recordingTB struct {
	testing.TB
	skipped, failed string
}

func (r *recordingTB) Helper()           {}
func (r *recordingTB) Skip(args ...any)  { r.skipped = args[0].(string) }
func (r *recordingTB) Fatal(args ...any) { r.failed = args[0].(string) }
func (r *recordingTB) Skipped() bool     { return r.skipped != "" }
func (r *recordingTB) Failed() bool      { return r.failed != "" }

func TestRequireGCSBackend(t *testing.T) {
	for _, tc := range []struct {
		name     string
		backend  string
		required string
		skip     bool
		fail     bool
	}{
		{"gcs runs", "gcs", "", false, false},
		{"gcs runs when required", "gcs", "true", false, false},
		{"s3 skips", "s3", "", true, false},
		{"s3 fails when required", "s3", "true", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(GCSRequiredEnv, tc.required)
			rec := &recordingTB{TB: t}
			RequireGCSBackend(rec, tc.backend)
			if rec.Skipped() != tc.skip || rec.Failed() != tc.fail {
				t.Fatalf("skipped=%v (%q) failed=%v (%q), want skip=%v fail=%v", rec.Skipped(), rec.skipped, rec.Failed(), rec.failed, tc.skip, tc.fail)
			}
		})
	}
}
