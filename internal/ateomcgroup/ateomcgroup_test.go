//go:build linux

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

package ateomcgroup

import "testing"

func TestAtNamespaceRoot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    bool
		wantErr bool
	}{
		{name: "private namespace", in: "0::/\n", want: true},
		{name: "host namespace", in: "0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n", want: false},
		{name: "hybrid hierarchy", in: "12:pids:/\n0::/\n", want: true},
		{name: "cgroup v1 only", in: "12:pids:/\n11:memory:/\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := atNamespaceRoot(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("atNamespaceRoot() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("atNamespaceRoot() = %v, want %v", got, tc.want)
			}
		})
	}
}
