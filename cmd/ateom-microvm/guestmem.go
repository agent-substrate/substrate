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
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/agentstats"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/guestmem"
	"github.com/agent-substrate/substrate/internal/ateattr"
)

// guestMemoryMetric is the gauge the breakdown is published on: one series per
// slice, distinguished by guestMemComponentKey, all of them adding up to the
// guest's MemTotal.
//
// Deliberately NOT the same shape as GetWorkloadStats. That RPC answers "how
// much is this actor using", is pulled per actor by whoever is attributing
// cost, and is the same question on both sandbox classes. This answers "what is
// inside a micro-VM's RAM", is pushed on the ateom's own metric interval, and
// has no gVisor counterpart to be consistent with — the sentry gives no
// equivalent decomposition.
const guestMemoryMetric = "ate.microvm.guest.memory.bytes"

// guestMemComponentKey labels which slice of the guest a point is. Its values
// are the guestMemComponent* constants and nothing else: the set is closed, so
// a dashboard can stack them and trust the total.
const guestMemComponentKey = attribute.Key("ate.guest.memory.component")

const (
	guestMemFree           = "free"
	guestMemPageCache      = "page_cache"
	guestMemContainers     = "containers"
	guestMemTmpfs          = "tmpfs"
	guestMemAgent          = "kata_agent"
	guestMemKernelAndOther = "kernel_and_other"
)

// registerGuestMemoryMetrics publishes the guest memory breakdown as an
// observable gauge.
//
// The sample is taken inside the callback rather than by a goroutine of its
// own. That looks like an RPC in a collection path, and would be a bad idea if
// the callback had to synchronize with the ateom's lifecycle — but it does not:
// the two pieces of state it reads are the same atomics GetWorkloadStats reads,
// which exist precisely so a reader never waits on a boot, a snapshot or a
// restore. With no lock to take, a background sampler would add a goroutine to
// start, stop and leak, and a staleness window between its tick and the export,
// to buy nothing. The SDK gives the callback a context carrying the reader's
// timeout, and the guest calls below respect it.
func (s *AteomService) registerGuestMemoryMetrics(mp metric.MeterProvider) error {
	meter := mp.Meter("ateom-microvm")

	gauge, err := meter.Int64ObservableGauge(
		guestMemoryMetric,
		metric.WithUnit("By"),
		metric.WithDescription("Guest RAM of the micro-VM this ateom is running, split by what is holding it. The components sum to the guest kernel's MemTotal, which is itself less than the RAM assigned to the VM. Everything but 'free' costs snapshot bytes on a Full checkpoint, and so costs suspend and resume latency."),
	)
	if err != nil {
		return fmt.Errorf("creating the %s gauge: %w", guestMemoryMetric, err)
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		b, attrs, ok := s.sampleGuestMemory(ctx)
		if !ok {
			// No guest to measure: the ateom is available, or a boot or restore
			// is still running. Observing nothing is the correct report — a zero
			// would read as "the guest is empty" — and this is the common case
			// on an idle worker, so it is not worth a log line per interval.
			return nil
		}
		observe := func(component string, v int64) {
			o.ObserveInt64(gauge, v, metric.WithAttributes(append(attrs, guestMemComponentKey.String(component))...))
		}
		observe(guestMemFree, int64(b.Free))
		observe(guestMemPageCache, int64(b.PageCache))
		observe(guestMemContainers, int64(b.Containers))
		observe(guestMemTmpfs, int64(b.Tmpfs))
		observe(guestMemAgent, int64(b.Agent))
		observe(guestMemKernelAndOther, b.KernelAndOther)
		return nil
	}, gauge)
	if err != nil {
		return fmt.Errorf("registering the %s callback: %w", guestMemoryMetric, err)
	}
	return nil
}

// sampleGuestMemory takes one breakdown of the live guest, with the attributes
// to file it under. ok is false when there is nothing to measure or the guest
// declined to answer.
//
// Attribution is by TEMPLATE, not by actor. Actor name and uid are unbounded
// here — a worker is recycled through actor after actor, and each one would
// leave a series behind — and the repo already draws that line the same way
// (see ateattr.ActorMetricAttributes, which omits atespace, actor name and
// actor uid for exactly this reason). The template is what a reader of this
// metric is comparing anyway: "does this image sit in more page cache than that
// one" is a template question.
func (s *AteomService) sampleGuestMemory(ctx context.Context) (guestmem.Breakdown, []attribute.KeyValue, bool) {
	// Same two atomics, read in the same order and re-checked the same way, as
	// GetWorkloadStats. See its doc comment for why neither may be reached
	// through s.lock.
	active := s.activeActor.Load()
	if active == nil {
		return guestmem.Breakdown{}, nil, false
	}
	target := s.guestStats.Load()
	if target == nil || target.actorUID != active.UID {
		return guestmem.Breakdown{}, nil, false
	}

	scrapeCtx, cancel := context.WithTimeout(ctx, statsCallTimeout)
	scrape, err := target.agent.GetMetrics(scrapeCtx)
	cancel()
	if err != nil {
		// Debug, not warn: a guest that has stopped answering is routine here
		// (a teardown racing the interval looks exactly like this), and this
		// runs on every collection, so a louder level would turn one sick
		// worker into a flood.
		slog.DebugContext(ctx, "No guest memory breakdown: the agent scrape failed",
			slog.String("actor.uid", active.UID), slog.Any("error", err))
		return guestmem.Breakdown{}, nil, false
	}

	g, err := guestmem.Parse(scrape)
	if err != nil {
		// Warn, and unlike the case above this one deserves it: the agent
		// answered and its scrape was not what this ateom knows how to read,
		// which is a kata-agent version having moved under us rather than a
		// transient. It will repeat every interval for as long as that is true,
		// which is the point — it is not self-healing.
		slog.WarnContext(ctx, "No guest memory breakdown: the agent scrape could not be parsed. This usually means the kata-agent's metric names have changed; see cmd/ateom-microvm/internal/guestmem",
			slog.String("actor.uid", active.UID), slog.Any("error", err))
		return guestmem.Breakdown{}, nil, false
	}

	// A container the agent cannot report contributes zero, exactly as in
	// sumContainerStats, and for the same reason: a container that has exited
	// took its guest cgroup with it and holds nothing. Here it does not even
	// cost accuracy in silence — whatever a missing container was holding is
	// still in the guest's MemTotal, so it shifts into the residual and stays
	// visible there.
	var containers agentstats.Sample
	for _, id := range target.workloadIDs {
		if ctx.Err() != nil {
			return guestmem.Breakdown{}, nil, false
		}
		callCtx, cancel := context.WithTimeout(ctx, statsCallTimeout)
		cs, err := target.agent.StatsContainer(callCtx, id)
		cancel()
		if err != nil {
			continue
		}
		containers = containers.Plus(agentstats.FromCgroupStats(cs))
	}

	// Re-check that no transition happened underneath the sample, as
	// GetWorkloadStats does and for the same reason: the reads above hold no
	// lock, so a checkpoint plus a fresh run can complete across them and the
	// numbers would be filed under the wrong template.
	if s.activeActor.Load() != active {
		return guestmem.Breakdown{}, nil, false
	}

	attrs := []attribute.KeyValue{
		ateattr.TemplateNamespaceKey.String(active.TemplateNamespace),
		ateattr.TemplateNameKey.String(active.TemplateName),
	}
	return guestmem.Compute(g, containers.MemoryAnonBytes), attrs, true
}
