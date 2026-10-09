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

import (
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

func TestParseCapacity(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"8Gi", 8 << 30},
		{"512Mi", 512 << 20},
		{"1000", 1000},
	} {
		got, err := parseCapacity(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseCapacity(%q) = %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"", "0", "-1Gi", "0.5", "1m", "lots"} {
		if got, err := parseCapacity(in); err == nil {
			t.Errorf("parseCapacity(%q) = %d, want an error", in, got)
		}
	}
}

// TestInstallStagingCapacityFitsVolume checks that the install's node plugin
// stages no more than its asset-staging volume holds, so concurrent asset
// downloads cannot get atelet's pod evicted.
func TestInstallStagingCapacityFitsVolume(t *testing.T) {
	raw, err := os.ReadFile("../../manifests/ate-install/atelet.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var ds *appsv1.DaemonSet
	for _, doc := range strings.Split(string(raw), "\n---") {
		var obj appsv1.DaemonSet
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatal(err)
		}
		if obj.Kind == "DaemonSet" && strings.HasPrefix(obj.Name, "atelet-") {
			ds = &obj
		}
	}
	if ds == nil {
		t.Fatal("atelet.yaml has no atelet DaemonSet")
	}
	var capacityArg string
	for _, c := range ds.Spec.Template.Spec.InitContainers {
		if c.Name != "snapshot-plugin" {
			continue
		}
		for _, a := range c.Args {
			if v, ok := strings.CutPrefix(a, "--asset-staging-capacity="); ok {
				capacityArg = v
			}
		}
	}
	if capacityArg == "" {
		t.Fatal("the snapshot-plugin sidecar does not set --asset-staging-capacity")
	}
	capacity, err := parseCapacity(capacityArg)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range ds.Spec.Template.Spec.Volumes {
		if v.Name != "asset-staging" {
			continue
		}
		if v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
			t.Fatal("asset-staging is not an emptyDir with a sizeLimit")
		}
		if limit := v.EmptyDir.SizeLimit.Value(); capacity >= limit {
			t.Errorf("--asset-staging-capacity=%s (%d bytes) leaves no room below the asset-staging sizeLimit of %d bytes", capacityArg, capacity, limit)
		}
		return
	}
	t.Fatal("atelet DaemonSet has no asset-staging volume")
}
