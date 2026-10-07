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

package actorjwt

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

const (
	lookupsMetric      = "ate.egress.actor_jwt.lookups"
	mintDurationMetric = "ate.egress.actor_jwt.mint.duration"
)

// Instruments holds a Minter's instruments. A nil *Instruments is a valid
// no-op.
type Instruments struct {
	lookups      metric.Int64Counter
	mintDuration metric.Float64Histogram
}

// NewInstruments creates a Minter's instruments from meter.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	lookups, err := meter.Int64Counter(
		lookupsMetric,
		metric.WithUnit("{lookup}"),
		metric.WithDescription("Number of actor JWT lookups in the egress gateway's token cache, by outcome."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", lookupsMetric, err)
	}
	mintDuration, err := meter.Float64Histogram(
		mintDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Time the egress gateway spent on each MintActorJWT call to ateapi."),
		// A mint is one ateapi round trip and one signature, a few
		// milliseconds; the top buckets catch an ateapi slow enough to hold up
		// egress requests.
		metric.WithExplicitBucketBoundaries(
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", mintDurationMetric, err)
	}
	return &Instruments{lookups: lookups, mintDuration: mintDuration}, nil
}

// recordLookup counts one Token call that ended with outcome, a hit or a miss,
// or with err.
func (i *Instruments) recordLookup(ctx context.Context, outcome string, err error) {
	if i == nil {
		return
	}
	switch {
	case err == nil:
	case errors.Is(err, context.Canceled):
		outcome = ateattr.EgressActorJWTOutcomeCancelled
	case errors.Is(err, context.DeadlineExceeded):
		outcome = ateattr.EgressActorJWTOutcomeTimeout
	default:
		outcome = ateattr.EgressActorJWTOutcomeError
	}
	attrs := []attribute.KeyValue{ateattr.EgressActorJWTOutcomeKey.String(outcome)}
	if outcome == ateattr.EgressActorJWTOutcomeError {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	i.lookups.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// recordMint records one MintActorJWT call.
func (i *Instruments) recordMint(ctx context.Context, d time.Duration, err error) {
	if i == nil {
		return
	}
	var attrs []attribute.KeyValue
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	i.mintDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
}
