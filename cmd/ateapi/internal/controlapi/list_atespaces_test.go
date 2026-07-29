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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func TestValidateListAtespacesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapipb.ListAtespacesRequest
		want field.ErrorList
	}{{
		"valid, no page_size",
		&ateapipb.ListAtespacesRequest{},
		nil,
	}, {
		"valid, positive page_size",
		&ateapipb.ListAtespacesRequest{PageSize: 10},
		nil,
	}, {
		"negative page_size",
		&ateapipb.ListAtespacesRequest{PageSize: -1},
		field.ErrorList{field.Invalid(field.NewPath("page_size"), int32(-1), "")},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateListAtespacesRequest(tt.req), tt.want)
		})
	}
}
