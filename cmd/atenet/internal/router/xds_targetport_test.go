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

package router

import (
	"slices"
	"testing"

	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
)

func TestConnectDoesNotCallExtProc(t *testing.T) {
	x := &XdsServer{}
	hcm := &hcmv3.HttpConnectionManager{}
	if err := x.buildConnectTerminateHCM("connect").UnmarshalTo(hcm); err != nil {
		t.Fatal(err)
	}
	filters := hcm.GetHttpFilters()
	var names []string
	for _, filter := range filters {
		names = append(names, filter.GetName())
	}
	want := []string{"envoy.filters.http.set_filter_state", "envoy.filters.http.router"}
	if !slices.Equal(names, want) {
		t.Fatalf("filters = %v, want %v", names, want)
	}
}
