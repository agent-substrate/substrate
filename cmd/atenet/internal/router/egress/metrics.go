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

package egress

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// atenet.router.egress.refused counts the requests the egress handler refuses,
// by the ActorTemplate of the actor that sent them and the reason. The actor
// and the destination stay on the log line each refusal writes.
const refusedMetricName = "atenet.router.egress.refused"

// Metrics holds the egress handler's instruments. A nil *Metrics is a valid
// no-op, so call sites need no guard.
type Metrics struct {
	refused metric.Int64Counter
}

// NewMetrics creates the egress handler's instruments from meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	refused, err := meter.Int64Counter(
		refusedMetricName,
		metric.WithUnit("{request}"),
		metric.WithDescription("number of egress requests the egress gateway refused, by actor template and reason"),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", refusedMetricName, err)
	}
	return &Metrics{refused: refused}, nil
}

// recordRefused counts one refused request of an actor of the template
// tmplNs/tmplName; empty names one the gateway does not know.
func (m *Metrics) recordRefused(ctx context.Context, tmplNs, tmplName, reason string) {
	if m == nil || m.refused == nil {
		return
	}
	m.refused.Add(ctx, 1, metric.WithAttributes(
		ateattr.TemplateAtespaceKey.String(ateattr.NormalizeTemplateDimension(tmplNs)),
		ateattr.TemplateNameKey.String(ateattr.NormalizeTemplateDimension(tmplName)),
		ateattr.EgressRefusalReasonKey.String(reason),
	))
}

// refused counts a refusal of ref's request for reason and returns err, the
// denial itself. A zero ref is a request whose actor is not known.
func (h *Handler) refused(ctx context.Context, ref resources.ActorRef, reason string, err error) error {
	tmplNs, tmplName := h.templates.lookup(ref)
	h.metrics.recordRefused(ctx, tmplNs, tmplName, reason)
	return err
}

// templateMemory is how long the gateway remembers an actor's template after
// the actor was last seen.
const templateMemory = time.Hour

// minTemplateCleanupSize is the map size below which removing forgotten
// entries is not worth a walk of the map.
const minTemplateCleanupSize = 64

// actorTemplates remembers the ActorTemplate of each actor whose CONNECT the
// gateway validated, so a refusal inside the tunnel names the template without
// another GetActor. The requests of a tunnel reach the gateway that took its
// CONNECT, so the entry is there for them.
type actorTemplates struct {
	now func() time.Time

	mu      sync.Mutex
	entries map[resources.ActorRef]templateEntry
	// cleanupAt is the entry count at which the next store also removes the
	// entries not seen for templateMemory. It doubles after each cleanup.
	cleanupAt int
}

type templateEntry struct {
	atespace, name string
	seen           time.Time
}

func newActorTemplates() *actorTemplates {
	return &actorTemplates{
		now:       time.Now,
		entries:   make(map[resources.ActorRef]templateEntry),
		cleanupAt: minTemplateCleanupSize,
	}
}

// remember records the template of ref's actor.
func (t *actorTemplates) remember(ref resources.ActorRef, template *ateapipb.ObjectRef) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.entries[ref] = templateEntry{atespace: template.GetAtespace(), name: template.GetName(), seen: now}
	if len(t.entries) < t.cleanupAt {
		return
	}
	for k, e := range t.entries {
		if now.Sub(e.seen) > templateMemory {
			delete(t.entries, k)
		}
	}
	t.cleanupAt = max(minTemplateCleanupSize, 2*len(t.entries))
}

// lookup returns the template of ref's actor, or empty strings when the
// gateway has not seen its CONNECT.
func (t *actorTemplates) lookup(ref resources.ActorRef) (atespace, name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[ref]
	if !ok {
		return "", ""
	}
	entry.seen = t.now()
	t.entries[ref] = entry
	return entry.atespace, entry.name
}
