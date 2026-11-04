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

package dns

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/metric"

	"go.ate.dev/substrate/internal/ateattr"
)

const queriesMetric = "ate.dns.queries"

// instruments holds the DNS relay's instruments. A nil *instruments is a valid
// no-op, so call sites need no guard.
type instruments struct {
	queries metric.Int64Counter
}

// newInstruments creates the DNS relay's instruments against meter.
func newInstruments(meter metric.Meter) (*instruments, error) {
	queries, err := meter.Int64Counter(
		queriesMetric,
		metric.WithUnit("{query}"),
		metric.WithDescription("Number of actor DNS queries the sandbox DNS relay checked against the actor's egress policy, by decision."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", queriesMetric, err)
	}
	return &instruments{queries: queries}, nil
}

// recordQuery counts one query the egress policy allowed or denied.
func (i *instruments) recordQuery(allowed bool) {
	if i == nil {
		return
	}
	decision := ateattr.DNSDecisionDenied
	if allowed {
		decision = ateattr.DNSDecisionAllowed
	}
	i.queries.Add(context.Background(), 1, metric.WithAttributes(ateattr.DNSDecisionKey.String(decision)))
}
