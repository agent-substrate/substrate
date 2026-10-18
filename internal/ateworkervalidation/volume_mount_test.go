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

package ateworkervalidation

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"k8s.io/apimachinery/pkg/api/operation"
)

func TestValidateVolumeMount(t *testing.T) {
	for _, tc := range []struct {
		sub     string
		invalid bool
	}{
		{"", false}, {"sessions/018f2c", false}, {"${SESSION_ID}", false},
		{"/absolute", true}, {"..", true}, {"a/../b", true}, {".", true}, {"a/./b", true},
		{"a//b", true}, {"a/", true}, {"a\x00b", true}, {strings.Repeat("a/", 16) + "a", true},
	} {
		t.Run(tc.sub, func(t *testing.T) {
			m := &ateworkerpb.VolumeMount{Name: "data", MountPath: "/data", SubPath: tc.sub, ReadOnly: true}
			errs := Validate_VolumeMount(context.Background(), operation.Operation{Type: operation.Create}, nil, m, nil)
			if (len(errs) > 0) != tc.invalid {
				t.Fatalf("validation = %v, want invalid %v", errs, tc.invalid)
			}
		})
	}
}
