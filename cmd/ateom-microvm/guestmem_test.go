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

package main

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

// testScrape is a trimmed kata-agent scrape for a guest of 2048 MiB, in the
// units and spelling /proc/meminfo uses. The exhaustive parsing cases live in
// internal/guestmem; what these tests need from it is a guest whose slices are
// far enough apart to be told from each other.
// Cached is 256 MiB of which 128 MiB is Shmem, so the page-cache and tmpfs
// slices come out unequal — a scrape where they matched would pass even if the
// callback observed one of them twice.
const testScrape = `# TYPE kata_guest_meminfo gauge
kata_guest_meminfo{item="mem_total"} 2013204
kata_guest_meminfo{item="mem_free"} 1048576
kata_guest_meminfo{item="cached"} 262144
kata_guest_meminfo{item="shmem"} 131072
kata_guest_meminfo{item="buffers"} 0
kata_guest_meminfo{item="s_reclaimable"} 0
kata_agent_total_rss 15728640
`

// containerStatsWithAnon is containerStats plus the anonymous figure, which is
// the one the breakdown charges to the workload (containerStats reports only
// the reclaimable key, which is what GetWorkloadStats needs).
func containerStatsWithAnon(usage, inactiveFile, anon uint64) *agentpb.CgroupStats {
	return &agentpb.CgroupStats{
		MemoryStats: &agentpb.MemoryStats{
			Usage: &agentpb.MemoryData{Usage: usage},
			Stats: map[string]uint64{"inactive_file": inactiveFile, "anon": anon},
		},
	}
}

// components collects one sample of the guest memory gauge into a map from
// component to value, going through a real MeterProvider so the registration,
// the callback and the attributes are all exercised the way the export does it.
func components(t *testing.T, s *AteomService) (map[string]int64, []attribute.KeyValue) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	if err := s.registerGuestMemoryMetrics(mp); err != nil {
		t.Fatalf("registerGuestMemoryMetrics: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	got := make(map[string]int64)
	var other []attribute.KeyValue
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != guestMemoryMetric {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 gauge (the residual must be able to go negative)", m.Name, m.Data)
			}
			for _, dp := range g.DataPoints {
				component, rest := splitComponent(t, dp.Attributes)
				if _, dup := got[component]; dup {
					t.Errorf("component %q observed twice in one collection", component)
				}
				got[component] = dp.Value
				other = rest
			}
		}
	}
	return got, other
}

// splitComponent pulls the component attribute out of a data point, returning
// it and everything else the point was labelled with.
func splitComponent(t *testing.T, set attribute.Set) (string, []attribute.KeyValue) {
	t.Helper()
	var component string
	var rest []attribute.KeyValue
	for _, kv := range set.ToSlice() {
		if kv.Key == guestMemComponentKey {
			component = kv.Value.AsString()
			continue
		}
		rest = append(rest, kv)
	}
	if component == "" {
		t.Fatalf("data point has no %s attribute: %v", guestMemComponentKey, set.ToSlice())
	}
	return component, rest
}

// The whole point of the metric: six slices that tile the guest's MemTotal.
// If this ever stops holding, a stacked chart of it is lying.
func TestGuestMemoryComponentsTileMemTotal(t *testing.T) {
	agent := &fakeAgent{
		scrape: testScrape,
		stats: map[string]*agentpb.CgroupStats{
			"app_ovl": containerStatsWithAnon(400<<20, 100<<20, 256<<20),
		},
	}
	got, _ := components(t, newStatsService(agent, "app_ovl"))

	want := map[string]int64{
		guestMemFree: 1048576 * 1024,
		// Cached minus Shmem: the tmpfs half is the next line, not this one.
		guestMemPageCache:  131072 * 1024,
		guestMemContainers: 256 << 20,
		guestMemTmpfs:      131072 * 1024,
		guestMemAgent:      15728640,
	}
	for component, v := range want {
		if got[component] != v {
			t.Errorf("%s = %d, want %d", component, got[component], v)
		}
	}
	if _, ok := got[guestMemKernelAndOther]; !ok {
		t.Fatalf("no %s point; got %v", guestMemKernelAndOther, got)
	}
	if len(got) != len(want)+1 {
		t.Errorf("observed %d components, want %d: the set must be closed for a stack to be meaningful", len(got), len(want)+1)
	}

	var sum int64
	for _, v := range got {
		sum += v
	}
	if wantTotal := int64(2013204 * 1024); sum != wantTotal {
		t.Errorf("components sum to %d, want MemTotal %d", sum, wantTotal)
	}
}

// The container slice is the ANONYMOUS figure, not the cgroup usage. Using
// usage would double-count the container's file pages against the page-cache
// slice and the total would come out over MemTotal.
func TestGuestMemoryChargesContainersTheirAnonOnly(t *testing.T) {
	agent := &fakeAgent{
		scrape: testScrape,
		stats: map[string]*agentpb.CgroupStats{
			// Usage far above anon: most of what this container is charged is
			// file-backed, and those pages are the page-cache slice's.
			"app_ovl": containerStatsWithAnon(900<<20, 700<<20, 64<<20),
		},
	}
	got, _ := components(t, newStatsService(agent, "app_ovl"))

	if got[guestMemContainers] != 64<<20 {
		t.Errorf("containers = %d, want the anon figure %d and not the cgroup usage", got[guestMemContainers], 64<<20)
	}
}

// Each actor container is two guest containers and only the workload half is
// summed; the multi-container case is where a double-count would show up.
func TestGuestMemorySumsEveryWorkloadContainer(t *testing.T) {
	agent := &fakeAgent{
		scrape: testScrape,
		stats: map[string]*agentpb.CgroupStats{
			"app_ovl":     containerStatsWithAnon(200<<20, 0, 100<<20),
			"sidecar_ovl": containerStatsWithAnon(80<<20, 0, 40<<20),
		},
	}
	got, _ := components(t, newStatsService(agent, "app_ovl", "sidecar_ovl"))

	if want := int64(140 << 20); got[guestMemContainers] != want {
		t.Errorf("containers = %d, want %d", got[guestMemContainers], want)
	}
}

// A container the agent cannot report contributes zero rather than failing the
// breakdown, as in sumContainerStats. Here it costs nothing in silence: what it
// was holding is still inside MemTotal, so it lands in the residual.
func TestGuestMemorySurvivesAnUnreadableContainer(t *testing.T) {
	agent := &fakeAgent{
		scrape: testScrape,
		stats: map[string]*agentpb.CgroupStats{
			"app_ovl": containerStatsWithAnon(200<<20, 0, 100<<20),
		},
		errs: map[string]error{"gone_ovl": errors.New("container not found")},
	}
	got, _ := components(t, newStatsService(agent, "app_ovl", "gone_ovl"))

	if want := int64(100 << 20); got[guestMemContainers] != want {
		t.Errorf("containers = %d, want %d from the one readable container", got[guestMemContainers], want)
	}
}

// Attribution is by template. Actor name and uid are unbounded on a worker that
// is recycled through actor after actor, and each would leave a dead series
// behind; the repo draws the same line in ateattr.ActorMetricAttributes.
func TestGuestMemoryAttributesByTemplateNotActor(t *testing.T) {
	agent := &fakeAgent{scrape: testScrape}
	_, attrs := components(t, newStatsService(agent))

	got := make(map[attribute.Key]string, len(attrs))
	for _, kv := range attrs {
		got[kv.Key] = kv.Value.Emit()
	}
	if got[ateattr.TemplateNamespaceKey] != testActor.TemplateNamespace {
		t.Errorf("%s = %q, want %q", ateattr.TemplateNamespaceKey, got[ateattr.TemplateNamespaceKey], testActor.TemplateNamespace)
	}
	if got[ateattr.TemplateNameKey] != testActor.TemplateName {
		t.Errorf("%s = %q, want %q", ateattr.TemplateNameKey, got[ateattr.TemplateNameKey], testActor.TemplateName)
	}
	for _, banned := range []attribute.Key{ateattr.ActorNameKey, ateattr.ActorUIDKey, ateattr.AtespaceKey} {
		if v, ok := got[banned]; ok {
			t.Errorf("high-cardinality attribute %s = %q must not label this metric", banned, v)
		}
	}
}

// Observing nothing is the right report when there is no guest. A zero would
// read as "the guest is empty", which is a different and false claim.
func TestGuestMemoryObservesNothingWithoutAGuest(t *testing.T) {
	available := &AteomService{}

	booting := &AteomService{}
	booting.activeActor.Store(&testActor)

	// A target left behind by a transition, disagreeing with the attribution:
	// belt and braces against reporting one actor's guest under another's
	// template.
	mismatched := &AteomService{}
	mismatched.activeActor.Store(&testActor)
	mismatched.guestStats.Store(&guestStatsTarget{actorUID: "some-other-actor", agent: &fakeAgent{scrape: testScrape}})

	tests := map[string]*AteomService{
		"available":          available,
		"booting":            booting,
		"mismatched target":  mismatched,
		"agent unreachable":  newStatsService(&fakeAgent{scrapeErr: errors.New("vsock closed")}),
		"unparseable scrape": newStatsService(&fakeAgent{scrape: "# nothing this ateom can read\n"}),
	}
	for name, s := range tests {
		t.Run(name, func(t *testing.T) {
			if got, _ := components(t, s); len(got) != 0 {
				t.Errorf("observed %v, want no points at all", got)
			}
		})
	}
}

// A checkpoint plus a fresh run can complete across the unlocked reads above,
// and the numbers would then be filed under a template that did not produce
// them.
func TestGuestMemoryDropsASampleOverwrittenMidFlight(t *testing.T) {
	s := newStatsService(nil)
	other := resources.ActorAttribution{
		Ref:               resources.ActorRef{Atespace: "space-b", Name: "actor-b"},
		UID:               "uid-b",
		TemplateNamespace: "ns-b",
		TemplateName:      "template-b",
	}
	// Swap the active actor from under the sample, at the last moment the
	// handler can still notice: while the containers are being read.
	s.guestStats.Store(&guestStatsTarget{
		actorUID: testActor.UID,
		agent: &fakeAgent{
			scrape: testScrape,
			stats:  map[string]*agentpb.CgroupStats{"app_ovl": containerStatsWithAnon(1<<20, 0, 1<<20)},
			onCall: func() {
				s.activeActor.Store(&other)
			},
		},
		workloadIDs: []string{"app_ovl"},
	})

	if got, _ := components(t, s); len(got) != 0 {
		t.Errorf("observed %v, want no points: the actor changed while the sample was being taken", got)
	}
}
