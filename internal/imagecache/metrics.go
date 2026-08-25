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

package imagecache

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

const requestsMetric = "ate.imagecache.requests"

// errTypeOther is the OTel registry's fallback, used for a failure that
// carries no status of its own. It is spelled _OTHER, not the "unknown" of the
// ate.* labels, because error.type is that registry's attribute reused
// verbatim.
const errTypeOther = "_OTHER"

// reportedStatuses bounds the label: transport.Error carries whatever the
// remote returned, so a registry or a proxy could otherwise mint a new series
// per status. Each listed one has its own action: credentials (401, 403), the
// ref (404), throttling (429), a registry-side fault (5xx).
var reportedStatuses = map[int]bool{
	401: true, 403: true, 404: true, 429: true,
	500: true, 502: true, 503: true, 504: true,
}

// newRequestsCounter creates the ate.imagecache.requests instrument.
func newRequestsCounter(meter metric.Meter) (metric.Int64Counter, error) {
	counter, err := meter.Int64Counter(
		requestsMetric,
		metric.WithUnit("{request}"),
		metric.WithDescription("Number of image lookups in the node-local image cache, by outcome."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", requestsMetric, err)
	}
	return counter, nil
}

// recordRequest counts one EnsureImage lookup. A store with no meter records
// nothing. A failure replaces the hit-or-miss outcome the caller passed, and
// only the Error outcome carries an error.type.
func (s *Store) recordRequest(ctx context.Context, outcome string, err error) {
	if s.requests == nil {
		return
	}
	if err != nil {
		outcome = failureOutcome(err)
	}
	attrs := []attribute.KeyValue{ateattr.ImageCacheOutcomeKey.String(outcome)}
	if outcome == ateattr.ImageCacheOutcomeError {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(errorType(err)))
	}
	// A cancelled lookup still reports: its pull was started and paid for.
	s.requests.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attrs...))
}

// failureOutcome separates a failed lookup from a caller that gave up.
// Cancellation is read first: an abandoned request also carries a transport
// error, which would otherwise read as a registry outage.
func failureOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return ateattr.ImageCacheOutcomeCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return ateattr.ImageCacheOutcomeTimeout
	}
	return ateattr.ImageCacheOutcomeError
}

// errorType reports the registry's own status for its rejection, the only
// domain status this path has. Each other failure, and each status outside the
// reported set, carries none.
func errorType(err error) string {
	var terr *transport.Error
	if errors.As(err, &terr) && reportedStatuses[terr.StatusCode] {
		return strconv.Itoa(terr.StatusCode)
	}
	return errTypeOther
}
