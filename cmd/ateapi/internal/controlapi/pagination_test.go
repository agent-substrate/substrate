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

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

func TestToStoreListOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *ateapipb.ListOptions
		want store.ListOptions
	}{{
		name: "nil options default the page size",
		opts: nil,
		want: store.ListOptions{PageSize: 1000},
	}, {
		name: "unset page size defaults",
		opts: &ateapipb.ListOptions{PageToken: "tok"},
		want: store.ListOptions{PageSize: 1000, PageToken: "tok"},
	}, {
		name: "page size within the cap is kept",
		opts: &ateapipb.ListOptions{PageSize: 10, PageToken: "tok"},
		want: store.ListOptions{PageSize: 10, PageToken: "tok"},
	}, {
		name: "page size above the cap is coerced",
		opts: &ateapipb.ListOptions{PageSize: 5000},
		want: store.ListOptions{PageSize: 1000},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := toStoreListOptions(tt.opts); got != tt.want {
				t.Errorf("toStoreListOptions(%v) = %+v, want %+v", tt.opts, got, tt.want)
			}
		})
	}
}
