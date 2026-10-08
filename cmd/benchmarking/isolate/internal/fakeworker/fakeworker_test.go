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

package fakeworker

import (
	"net/netip"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Names are pinned: a change renames every fake Worker, which strands the
// ones a previous fake-workersync registered.
func TestNamePinned(t *testing.T) {
	if got, want := Name("r1", "benchmark-workloads", "benchmark-worker", 0), "fake-r1-4ce21861-0"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := PodUID("fake-r1-4ce21861-0"), "eaa30ab6-fe68-512a-90fc-b17484c18607"; got != want {
		t.Errorf("PodUID = %q, want %q", got, want)
	}
}

// k8sShortName is the k8s-short-name format Worker names are validated
// against.
var k8sShortName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

func TestNameIsAShortNameAtTheBounds(t *testing.T) {
	name := Name(strings.Repeat("z", MaxRunLength), strings.Repeat("n", 63), strings.Repeat("p", 253), 99999)
	if len(name) > 63 || !k8sShortName.MatchString(name) {
		t.Errorf("Name at the bounds = %q (%d chars), want a short name of at most 63", name, len(name))
	}
}

func TestNamesDistinctAcrossPools(t *testing.T) {
	a := Name("r1", "ns", "pool-a", 0)
	b := Name("r1", "ns", "pool-b", 0)
	c := Name("r1", "other", "pool-a", 0)
	if a == b || a == c || b == c {
		t.Errorf("pools share a name: %q %q %q", a, b, c)
	}
}

func TestIndexRoundTrips(t *testing.T) {
	for _, i := range []int{0, 7, 1234} {
		got, ok := Index("r1", "ns", "pool", Name("r1", "ns", "pool", i))
		if !ok || got != i {
			t.Errorf("Index(Name(%d)) = %d, %v", i, got, ok)
		}
	}
	for _, name := range []string{
		Name("r1", "ns", "other", 3),                                // another pool
		Name("r2", "ns", "pool", 3),                                 // another run
		Name("r1", "ns", "pool", 3) + "x",                           // not a number
		strings.TrimSuffix(Name("r1", "ns", "pool", 0), "0") + "01", // not canonical
		"benchmark-worker-5d8f-xyz",
	} {
		if _, ok := Index("r1", "ns", "pool", name); ok {
			t.Errorf("Index(%q) matched the pool", name)
		}
	}
}

func TestNodeRoundRobin(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	var got []string
	for i := range 5 {
		got = append(got, Node(i, nodes))
	}
	if strings.Join(got, ",") != "a,b,c,a,b" {
		t.Errorf("Node over 5 indexes = %v", got)
	}
}

func TestPodUIDIsAStableUUID(t *testing.T) {
	a, b := PodUID("fake-r1-00000000-0"), PodUID("fake-r1-00000000-0")
	if a != b {
		t.Errorf("PodUID is not deterministic: %q then %q", a, b)
	}
	if _, err := uuid.Parse(a); err != nil {
		t.Errorf("PodUID = %q, not a UUID: %v", a, err)
	}
	if PodUID("fake-r1-00000000-1") == a {
		t.Error("two names share a pod UID")
	}
}

func TestIPInDocumentationRange(t *testing.T) {
	doc := netip.MustParsePrefix("192.0.2.0/24")
	for _, i := range []int{0, 253, 254, 1000} {
		addr, err := netip.ParseAddr(IP(i))
		if err != nil || !doc.Contains(addr) || addr.As4()[3] == 0 || addr.As4()[3] == 255 {
			t.Errorf("IP(%d) = %q, want a host address in %s", i, IP(i), doc)
		}
	}
	if IP(0) != IP(254) {
		t.Errorf("IP(0) = %q, IP(254) = %q; addresses should repeat every 254", IP(0), IP(254))
	}
}

func TestValidateRun(t *testing.T) {
	for _, run := range []string{"r", "abc12345"} {
		if err := ValidateRun(run); err != nil {
			t.Errorf("ValidateRun(%q) = %v, want nil", run, err)
		}
	}
	for _, run := range []string{"", "abc123456", "Run", "a-b", "a_b"} {
		if err := ValidateRun(run); err == nil {
			t.Errorf("ValidateRun(%q) = nil, want an error", run)
		}
	}
}
