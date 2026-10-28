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

package authz

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	checkDurationMetric = "ate.authz.check.duration"
	writeDurationMetric = "ate.authz.write.duration"
)

// durationBuckets match ate.scheduler.assignment.duration: a check or a
// reconcile is a few Postgres reads, and 5s keeps latency spikes measurable.
var durationBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// instruments holds the authz histograms. A nil *instruments is a valid no-op.
type instruments struct {
	checkDuration metric.Float64Histogram
	writeDuration metric.Float64Histogram
}

func newInstruments(meter metric.Meter) (*instruments, error) {
	checkDuration, err := meter.Float64Histogram(
		checkDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one OpenFGA Check by the ateapi authorizer, and its decision."),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", checkDurationMetric, err)
	}
	writeDuration, err := meter.Float64Histogram(
		writeDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one reconcile of the OpenFGA role binding tuples on an object."),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", writeDurationMetric, err)
	}
	return &instruments{checkDuration: checkDuration, writeDuration: writeDuration}, nil
}

// recordCheck records one Check. A non-nil err means OpenFGA gave no decision,
// so err sets error.type in place of the decision.
func (i *instruments) recordCheck(ctx context.Context, elapsed time.Duration, relation, object, principalKind string, allowed bool, err error) {
	if i == nil || i.checkDuration == nil {
		return
	}
	// TODO: label checks with whether the OpenFGA check cache is on, once
	// ateapi can enable it.
	attrs := []attribute.KeyValue{
		ateattr.AuthzRelationKey.String(relation),
		ateattr.AuthzObjectTypeKey.String(objectType(object)),
		ateattr.PrincipalKindKey.String(principalKind),
	}
	switch {
	case err != nil:
		attrs = append(attrs, ateattr.ErrorTypeKey.String(apierror.Code(err).String()))
	case allowed:
		attrs = append(attrs, ateattr.AuthzDecisionKey.String(ateattr.AuthzDecisionAllowed))
	default:
		attrs = append(attrs, ateattr.AuthzDecisionKey.String(ateattr.AuthzDecisionDenied))
	}
	i.checkDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attrs...))
}

// recordWrite records one reconcile of the bindings on object.
func (i *instruments) recordWrite(ctx context.Context, elapsed time.Duration, object string, err error) {
	if i == nil || i.writeDuration == nil {
		return
	}
	attrs := []attribute.KeyValue{ateattr.AuthzObjectTypeKey.String(objectType(object))}
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(apierror.Code(err).String()))
	}
	i.writeDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attrs...))
}

// objectType returns the OpenFGA type of object, such as "actor" for
// "actor:team-a/runner".
func objectType(object string) string {
	t, _, _ := strings.Cut(object, ":")
	return t
}
