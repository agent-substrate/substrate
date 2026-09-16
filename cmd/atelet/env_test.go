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

package main

import "testing"

func TestS3PathStyleEnvironment(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"true", true}, {"TRUE", false}, {"1", false}, {"false", false}, {"", false}, {"invalid", false},
	} {
		t.Setenv("AWS_S3_USE_PATH_STYLE", tc.raw)
		if got := s3PathStyleEnv.Get(); got != tc.want {
			t.Errorf("AWS_S3_USE_PATH_STYLE=%q: got %v, want %v", tc.raw, got, tc.want)
		}
	}
}
